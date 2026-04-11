#!/usr/bin/env python3
"""VictoriaMetrics cluster similarity tool.

Evaluates data parity between two VictoriaMetrics clusters that serve the same
data (e.g. an HA pair). Used after maintenance operations — backfilling, restores,
upgrades — to quantify how similar the clusters' data are.

Comparison is statistical: four dimensions are scored 0–100% and combined into
an overall weighted score.

  Dimension        Weight  What it measures
  ─────────────────────────────────────────────────────────────────────────────
  Metric coverage  25%     Jaccard overlap of metric name sets
  Cardinality      25%     Median series-count ratio for shared metrics
  Sample density   30%     Series presence at N timestamps across the window
  Value alignment  20%     avg() closeness within a fractional tolerance

Additionally, VictoriaMetrics's own internal /metrics are scraped from both
clusters to provide a diagnostic layer explaining operational divergence.

Usage:
    python vm_comparison.py <cluster-a> <cluster-b> [options]

Arguments:
    cluster-a, cluster-b   Names of VictoriaMetrics clusters in clusters.yaml

Options:
    --window DURATION      Comparison window (default: 24h)
    --metric-samples N     Metrics sampled for density/value checks (default: 20)
    --timestamps N         Timestamps per check (0 = skip value check, default: 5)
    --tolerance FLOAT      Fractional value tolerance (default: 0.02)
    --skip-internals       Skip VM /metrics diagnostic
"""

import argparse
import json
import logging
import re
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
from rich import box
from rich.console import Console
from rich.table import Table

from slopbox.logging import configure_logging
from slopbox.vm_client import VmClient, build_vm_client
from slopbox_domain.metrics.cluster import VictoriaMetricsClusterConfig
from slopbox_domain.metrics.models import (
    CardinalityProfile,
    CoverageProfile,
    DensityProfile,
    DensityWindowResult,
    SimilarityReport,
    ValueProfile,
    VmInternalMetric,
    VmInternalProfile,
)
from slopbox_domain.metrics.types import RawTsdbStatus
from slopbox_domain.registry import ClusterRegistry

logger = logging.getLogger("vm_comparison")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_CLUSTERS_YAML = Path(__file__).parent.parent.parent / "clusters.yaml"

# Duration windows always evaluated for density (in addition to --window)
DENSITY_WINDOWS = ["1h", "24h", "7d"]

# VM-internal metrics to include in the diagnostic table (label-free only)
VM_INTERNAL_KEYS = [
    "vm_new_timeseries_created_total",
    "vm_rows_per_insert_total",
    "vm_active_time_series",
]

# Grade thresholds
_GRADE_COLORS = {
    "excellent": "green",
    "good": "cyan",
    "degraded": "yellow",
    "critical": "red",
}


# ---------------------------------------------------------------------------
# Fetch functions
# ---------------------------------------------------------------------------

def fetch_metric_names(client: VmClient) -> set[str]:
    """Return all metric names present in the cluster."""
    return set(client.label_values("__name__"))


def fetch_tsdb_status(client: VmClient, top_n: int) -> RawTsdbStatus:
    """Return TSDB cardinality stats for the top-N metrics."""
    return client.tsdb_status(top_n=top_n)


def fetch_series_counts_at(
    client: VmClient,
    metrics: list[str],
    timestamp: float,
) -> dict[str, float]:
    """Return active series counts for *metrics* at *timestamp*.

    Uses a single batched query: count({__name__=~"..."}) by (__name__).
    Returns {metric_name: series_count}. Metrics with no active series at
    the timestamp are absent from the result (treat as 0).
    """
    if not metrics:
        return {}
    metric_regex = "|".join(re.escape(m) for m in metrics)
    expr = f'count({{__name__=~"{metric_regex}"}}) by (__name__)'
    response = client.query(expr, time=timestamp)
    return {
        entry.metric.get("__name__", ""): float(entry.value[1])
        for entry in response.data.result
        if entry.metric.get("__name__") and len(entry.value) == 2
    }


def fetch_avg_values_at(
    client: VmClient,
    metrics: list[str],
    timestamp: float,
) -> dict[str, float]:
    """Return avg() across all series for *metrics* at *timestamp*.

    Uses a single batched query: avg({__name__=~"..."}) by (__name__).
    Returns {metric_name: avg_value}. Metrics with no data are absent.
    """
    if not metrics:
        return {}
    metric_regex = "|".join(re.escape(m) for m in metrics)
    expr = f'avg({{__name__=~"{metric_regex}"}}) by (__name__)'
    response = client.query(expr, time=timestamp)
    return {
        entry.metric.get("__name__", ""): float(entry.value[1])
        for entry in response.data.result
        if entry.metric.get("__name__") and len(entry.value) == 2
    }


def fetch_vm_internals(client: VmClient) -> dict[str, float]:
    """Scrape VM's own /metrics endpoint and return the diagnostic signals."""
    try:
        all_metrics = client.scrape_metrics()
    except requests.RequestException as exc:
        logger.warning("failed to scrape /metrics from %s: %s", client.name, exc)
        return {}
    return {k: all_metrics[k] for k in VM_INTERNAL_KEYS if k in all_metrics}


# ---------------------------------------------------------------------------
# Profile functions
# ---------------------------------------------------------------------------

def profile_coverage(names_a: set[str], names_b: set[str]) -> CoverageProfile:
    """Compute Jaccard similarity between the two metric name sets."""
    shared = names_a & names_b
    union = names_a | names_b
    jaccard = len(shared) / len(union) if union else 1.0
    return CoverageProfile(
        total_a=len(names_a),
        total_b=len(names_b),
        shared=len(shared),
        only_in_a=len(names_a - names_b),
        only_in_b=len(names_b - names_a),
        jaccard=jaccard,
    )


def profile_cardinality(
    tsdb_a: RawTsdbStatus,
    tsdb_b: RawTsdbStatus,
    shared_names: set[str],
) -> CardinalityProfile:
    """Compute median per-metric series-count ratio for shared metrics."""
    counts_a = {e.name: e.value for e in tsdb_a.data.seriesCountByMetricName}
    counts_b = {e.name: e.value for e in tsdb_b.data.seriesCountByMetricName}

    ratios: list[float] = []
    for name in shared_names:
        ca = counts_a.get(name, 0)
        cb = counts_b.get(name, 0)
        if ca == 0 and cb == 0:
            ratios.append(1.0)
        elif ca == 0 or cb == 0:
            ratios.append(0.0)
        else:
            ratios.append(min(ca, cb) / max(ca, cb))

    return CardinalityProfile(
        total_series_a=tsdb_a.data.totalSeries,
        total_series_b=tsdb_b.data.totalSeries,
        metrics_checked=len(ratios),
        median_ratio=statistics.median(ratios) if ratios else 1.0,
    )


def profile_density(
    client_a: VmClient,
    client_b: VmClient,
    sample_metrics: list[str],
    n_timestamps: int,
) -> DensityProfile:
    """Score data presence at N timestamps across each density window.

    For each window, N evenly-spaced timestamps are queried. At each timestamp
    a batched count() query checks whether each sampled metric has active series.
    The ratio min/max is computed per (metric, timestamp) pair; the median over
    all pairs is the window score.
    """
    end_time = time.time()
    window_results: list[DensityWindowResult] = []

    for window in _density_windows_to_check():
        window_seconds = _parse_window_seconds(window)
        timestamps = _spread_timestamps(end_time, window_seconds, n_timestamps)

        per_metric_ratios: dict[str, list[float]] = {m: [] for m in sample_metrics}

        with ThreadPoolExecutor(max_workers=2) as pool:
            for ts in timestamps:
                fut_a = pool.submit(fetch_series_counts_at, client_a, sample_metrics, ts)
                fut_b = pool.submit(fetch_series_counts_at, client_b, sample_metrics, ts)
                counts_a = fut_a.result()
                counts_b = fut_b.result()

                for m in sample_metrics:
                    ca = counts_a.get(m, 0.0)
                    cb = counts_b.get(m, 0.0)
                    if ca == 0.0 and cb == 0.0:
                        # Both absent — neutral, skip this (metric, timestamp) pair
                        continue
                    elif ca == 0.0 or cb == 0.0:
                        per_metric_ratios[m].append(0.0)
                    else:
                        per_metric_ratios[m].append(min(ca, cb) / max(ca, cb))

        all_ratios: list[float] = []
        gap_metrics: list[str] = []
        for m, ratios in per_metric_ratios.items():
            if not ratios:
                continue
            median = statistics.median(ratios)
            all_ratios.append(median)
            if median < 0.8:
                gap_metrics.append(m)

        window_results.append(DensityWindowResult(
            window=window,
            metrics_checked=len([m for m in per_metric_ratios if per_metric_ratios[m]]),
            median_ratio=statistics.median(all_ratios) if all_ratios else 1.0,
            gap_metrics=sorted(gap_metrics),
        ))

    return DensityProfile(windows=window_results)


def profile_values(
    client_a: VmClient,
    client_b: VmClient,
    value_metrics: list[str],
    n_timestamps: int,
    window: str,
    tolerance: float,
) -> ValueProfile | None:
    """Compare avg() values for value_metrics at N timestamps.

    Returns None if value_metrics is empty or n_timestamps == 0.
    """
    if not value_metrics or n_timestamps == 0:
        return None

    end_time = time.time()
    window_seconds = _parse_window_seconds(window)
    timestamps = _spread_timestamps(end_time, window_seconds, n_timestamps)

    samples_compared = 0
    within_tolerance = 0

    with ThreadPoolExecutor(max_workers=2) as pool:
        for ts in timestamps:
            fut_a = pool.submit(fetch_avg_values_at, client_a, value_metrics, ts)
            fut_b = pool.submit(fetch_avg_values_at, client_b, value_metrics, ts)
            vals_a = fut_a.result()
            vals_b = fut_b.result()

            for m in value_metrics:
                va = vals_a.get(m)
                vb = vals_b.get(m)
                if va is None or vb is None:
                    continue
                samples_compared += 1
                denom = max(abs(va), abs(vb))
                if denom == 0.0:
                    within_tolerance += 1  # both zero → exact match
                elif abs(va - vb) / denom <= tolerance:
                    within_tolerance += 1

    return ValueProfile(
        metrics_checked=len(value_metrics),
        timestamps_checked=n_timestamps,
        samples_compared=samples_compared,
        within_tolerance=within_tolerance,
        tolerance=tolerance,
    )


def profile_vm_internals(
    internals_a: dict[str, float],
    internals_b: dict[str, float],
) -> VmInternalProfile | None:
    """Build a side-by-side comparison of VM internal metrics.

    Returns None if neither cluster returned any internal metrics.
    """
    all_keys = sorted(set(internals_a) | set(internals_b))
    if not all_keys:
        return None

    metrics: list[VmInternalMetric] = []
    for key in all_keys:
        va = internals_a.get(key, 0.0)
        vb = internals_b.get(key, 0.0)
        ratio = (va / vb) if vb != 0.0 else float("nan")
        metrics.append(VmInternalMetric(name=key, value_a=va, value_b=vb, ratio=ratio))

    return VmInternalProfile(metrics=metrics)


def compute_overall_score(
    coverage: CoverageProfile,
    cardinality: CardinalityProfile,
    density: DensityProfile,
    values: ValueProfile | None,
) -> float:
    """Weighted average of all dimension scores."""
    if values is not None:
        return (
            coverage.jaccard * 0.25
            + cardinality.median_ratio * 0.25
            + density.overall_ratio * 0.30
            + values.match_ratio * 0.20
        )
    # Redistribute value weight to density when value check is skipped
    return (
        coverage.jaccard * 0.27
        + cardinality.median_ratio * 0.27
        + density.overall_ratio * 0.46
    )


# ---------------------------------------------------------------------------
# Render: human mode
# ---------------------------------------------------------------------------

def render_report(console: Console, report: SimilarityReport) -> None:
    """Render the similarity report as Rich tables."""
    grade_color = _GRADE_COLORS.get(report.grade, "white")

    console.print()
    console.rule("[bold]VictoriaMetrics Cluster Comparison[/bold]")
    console.print(
        f"  [bold]{report.cluster_a}[/bold] vs [bold]{report.cluster_b}[/bold]"
        f"  [dim]|  window: {report.compare_window}  |  {report.evaluated_at}[/dim]"
    )
    console.print()
    console.print(
        f"  Overall score: [bold {grade_color}]{report.overall_score_pct}[/bold {grade_color}]"
        f"  [{grade_color}][{report.grade.upper()}][/{grade_color}]"
    )
    console.print()

    # ── Summary table ──────────────────────────────────────────────────────
    console.rule("[bold]Dimension Scores[/bold]")
    console.print()

    summary = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold", pad_edge=False)
    summary.add_column("Dimension", style="white", no_wrap=True)
    summary.add_column("Score", justify="right")
    summary.add_column("Details")

    cov = report.coverage
    summary.add_row(
        "Metric coverage",
        _score_cell(cov.jaccard),
        f"{cov.shared:,} shared / {cov.shared + cov.only_in_a + cov.only_in_b:,} union"
        f"  ({cov.only_in_a:,} only-A, {cov.only_in_b:,} only-B)",
    )

    card = report.cardinality
    summary.add_row(
        "Cardinality",
        _score_cell(card.median_ratio),
        f"{card.total_series_a:,} series A / {card.total_series_b:,} series B"
        f"  (Δ {card.total_delta_pct})",
    )

    dens = report.density
    summary.add_row(
        "Sample density",
        _score_cell(dens.overall_ratio),
        "  ".join(
            f"{w.window}: {w.score_pct}" for w in dens.windows
        ),
    )

    if report.values is not None:
        val = report.values
        summary.add_row(
            "Value alignment",
            _score_cell(val.match_ratio),
            f"{val.metrics_checked} metrics × {val.timestamps_checked} timestamps"
            f", {val.tolerance * 100:.0f}% tolerance"
            f" → {val.within_tolerance}/{val.samples_compared} pairs matched",
        )
    else:
        summary.add_row("Value alignment", "[dim]—[/dim]", "[dim]skipped[/dim]")

    console.print(summary)
    console.print()

    # ── Density gap details ────────────────────────────────────────────────
    gap_windows = [w for w in report.density.windows if w.gap_metrics]
    if gap_windows:
        console.rule("[bold]Density Gaps[/bold]")
        console.print(
            "  [dim]Metrics with median series-presence ratio < 80% in at least one window:[/dim]"
        )
        console.print()
        gap_table = Table(
            box=box.SIMPLE_HEAD, show_header=True, header_style="bold", pad_edge=False
        )
        gap_table.add_column("Window", no_wrap=True)
        gap_table.add_column("Metric", style="white")
        for w in gap_windows:
            for i, m in enumerate(w.gap_metrics):
                gap_table.add_row(w.window if i == 0 else "", m)
        console.print(gap_table)
        console.print()

    # ── VM internal metrics ────────────────────────────────────────────────
    if report.vm_internals is not None:
        console.rule("[bold]VM Internal Metrics (diagnostic)[/bold]")
        console.print(f"  [dim]{report.cluster_a_host}/metrics  vs  {report.cluster_b_host}/metrics[/dim]")
        console.print()
        int_table = Table(
            box=box.SIMPLE_HEAD, show_header=True, header_style="bold", pad_edge=False
        )
        int_table.add_column("Metric", style="white", no_wrap=True)
        int_table.add_column(report.cluster_a, justify="right")
        int_table.add_column(report.cluster_b, justify="right")
        int_table.add_column("A/B ratio", justify="right")
        for m in report.vm_internals.metrics:
            int_table.add_row(
                m.name,
                f"{m.value_a:,.0f}",
                f"{m.value_b:,.0f}",
                m.ratio_pct,
            )
        console.print(int_table)
        console.print()


# ---------------------------------------------------------------------------
# Render: JSON mode
# ---------------------------------------------------------------------------

def render_report_json(report: SimilarityReport) -> None:
    """Emit the report as a single JSON document to stdout."""
    print(report.model_dump_json(indent=2))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _score_cell(ratio: float) -> str:
    pct = ratio * 100
    if pct >= 95:
        color = "green"
    elif pct >= 85:
        color = "cyan"
    elif pct >= 70:
        color = "yellow"
    else:
        color = "red"
    return f"[bold {color}]{pct:.1f}%[/bold {color}]"


def _parse_window_seconds(window: str) -> int:
    """Parse a duration string (e.g. '24h', '7d') into seconds."""
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
    if window and window[-1] in units:
        try:
            return int(window[:-1]) * units[window[-1]]
        except ValueError:
            pass
    raise ValueError(
        f"invalid duration {window!r}: expected a number followed by s/m/h/d/w"
    )


def _spread_timestamps(end_time: float, window_seconds: int, n: int) -> list[float]:
    """Return N evenly-spaced timestamps from (end_time - window_seconds) to end_time."""
    if n <= 1:
        return [end_time]
    return [
        end_time - window_seconds * i / (n - 1)
        for i in range(n)
    ]


def _density_windows_to_check() -> list[str]:
    """Return the fixed set of density windows."""
    return DENSITY_WINDOWS


def _select_sample_metrics(
    tsdb_a: RawTsdbStatus,
    tsdb_b: RawTsdbStatus,
    shared_names: set[str],
    n: int,
) -> list[str]:
    """Pick top-N shared metrics by combined series count, excluding vm_* internals."""
    counts_a = {e.name: e.value for e in tsdb_a.data.seriesCountByMetricName}
    counts_b = {e.name: e.value for e in tsdb_b.data.seriesCountByMetricName}

    scored = [
        (m, counts_a.get(m, 0) + counts_b.get(m, 0))
        for m in shared_names
        if not m.startswith("vm_")  # exclude VM-internal series from stored-metric sample
    ]
    scored.sort(key=lambda x: x[1], reverse=True)
    return [m for m, _ in scored[:n]]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    log_format = configure_logging()
    console = Console() if log_format == "human" else None

    parser = argparse.ArgumentParser(
        description="Compare data similarity between two VictoriaMetrics clusters.",
    )
    parser.add_argument("cluster_a", metavar="cluster-a", help="Name of the first cluster in clusters.yaml")
    parser.add_argument("cluster_b", metavar="cluster-b", help="Name of the second cluster in clusters.yaml")
    parser.add_argument("--window", default="24h", help="Comparison window (default: 24h)")
    parser.add_argument(
        "--metric-samples",
        type=int,
        default=20,
        metavar="N",
        help="Number of metrics to sample for density and value checks (default: 20)",
    )
    parser.add_argument(
        "--timestamps",
        type=int,
        default=5,
        metavar="N",
        help="Timestamps per check; 0 skips value alignment (default: 5)",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=0.02,
        help="Fractional value tolerance for alignment check (default: 0.02 = 2%%)",
    )
    parser.add_argument(
        "--skip-internals",
        action="store_true",
        help="Skip the VM /metrics diagnostic table",
    )
    args = parser.parse_args()

    # Validate window string early so we fail fast with a clear message
    try:
        _parse_window_seconds(args.window)
    except ValueError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    # Load registry
    clusters_yaml = DEFAULT_CLUSTERS_YAML
    if not clusters_yaml.exists():
        logger.error("clusters.yaml not found at %s", clusters_yaml.resolve())
        sys.exit(1)

    registry = ClusterRegistry.from_yaml(clusters_yaml)

    def _resolve_cluster(name: str) -> VictoriaMetricsClusterConfig:
        matches = registry.vm(name=name)
        if not matches:
            logger.error(
                "cluster %r not found in clusters.yaml victoriametrics section; "
                "available: %s",
                name,
                ", ".join(c.name for c in registry.victoriametrics) or "(none)",
            )
            sys.exit(1)
        return matches[0]

    cfg_a = _resolve_cluster(args.cluster_a)
    cfg_b = _resolve_cluster(args.cluster_b)

    client_a = build_vm_client(cfg_a)
    client_b = build_vm_client(cfg_b)

    logger.info(
        "comparing %s (%s) vs %s (%s)  window=%s  samples=%d  timestamps=%d",
        cfg_a.name, cfg_a.host,
        cfg_b.name, cfg_b.host,
        args.window,
        args.metric_samples,
        args.timestamps,
    )

    # ── Phase 1: Fetch metadata ────────────────────────────────────────────
    if console:
        console.status("[dim]Fetching metric names and cardinality stats…[/dim]").__enter__()

    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            fut_names_a = pool.submit(fetch_metric_names, client_a)
            fut_names_b = pool.submit(fetch_metric_names, client_b)
            fut_tsdb_a = pool.submit(fetch_tsdb_status, client_a, args.metric_samples * 2)
            fut_tsdb_b = pool.submit(fetch_tsdb_status, client_b, args.metric_samples * 2)
            names_a = fut_names_a.result()
            names_b = fut_names_b.result()
            tsdb_a = fut_tsdb_a.result()
            tsdb_b = fut_tsdb_b.result()
    except requests.RequestException as exc:
        logger.error("failed to fetch metadata: %s", exc)
        sys.exit(1)

    logger.info(
        "metric names: %d in %s, %d in %s",
        len(names_a), cfg_a.name,
        len(names_b), cfg_b.name,
    )

    # ── Phase 2: Profile coverage and cardinality ──────────────────────────
    shared_names = names_a & names_b
    coverage = profile_coverage(names_a, names_b)
    cardinality = profile_cardinality(tsdb_a, tsdb_b, shared_names)

    logger.info(
        "coverage: %.1f%%  cardinality: %.1f%%  shared metrics: %d",
        coverage.jaccard * 100,
        cardinality.median_ratio * 100,
        coverage.shared,
    )

    # ── Phase 3: Select sample metrics ────────────────────────────────────
    sample_metrics = _select_sample_metrics(
        tsdb_a, tsdb_b, shared_names, args.metric_samples
    )
    value_metrics = sample_metrics[: min(len(sample_metrics), max(0, args.timestamps))]
    # Value check uses the same timestamp count as --timestamps; pick fewer metrics
    # for value alignment (top half of the density sample) so value queries are cheap.
    value_metrics = sample_metrics[: max(1, len(sample_metrics) // 2)] if args.timestamps > 0 else []

    logger.info(
        "sampling %d metrics for density, %d for value alignment",
        len(sample_metrics),
        len(value_metrics),
    )

    # ── Phase 4: Density check ─────────────────────────────────────────────
    if console:
        console.status("[dim]Checking sample density across windows…[/dim]").__enter__()

    try:
        density = profile_density(client_a, client_b, sample_metrics, args.timestamps)
    except requests.RequestException as exc:
        logger.error("density check failed: %s", exc)
        sys.exit(1)

    logger.info("density score: %.1f%%", density.overall_ratio * 100)

    # ── Phase 5: Value alignment ───────────────────────────────────────────
    if console and value_metrics:
        console.status("[dim]Comparing metric values…[/dim]").__enter__()

    try:
        values = profile_values(
            client_a, client_b, value_metrics,
            args.timestamps, args.window, args.tolerance,
        )
    except requests.RequestException as exc:
        logger.error("value alignment check failed: %s", exc)
        sys.exit(1)

    if values is not None:
        logger.info("value alignment: %.1f%%", values.match_ratio * 100)

    # ── Phase 6: VM internal metrics ───────────────────────────────────────
    vm_internals: VmInternalProfile | None = None
    if not args.skip_internals:
        if console:
            console.status("[dim]Scraping VM internal metrics…[/dim]").__enter__()
        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_int_a = pool.submit(fetch_vm_internals, client_a)
            fut_int_b = pool.submit(fetch_vm_internals, client_b)
            int_a = fut_int_a.result()
            int_b = fut_int_b.result()
        vm_internals = profile_vm_internals(int_a, int_b)

    # ── Build report ───────────────────────────────────────────────────────
    overall = compute_overall_score(coverage, cardinality, density, values)
    report = SimilarityReport(
        cluster_a=cfg_a.name,
        cluster_b=cfg_b.name,
        cluster_a_host=cfg_a.host,
        cluster_b_host=cfg_b.host,
        evaluated_at=datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        compare_window=args.window,
        coverage=coverage,
        cardinality=cardinality,
        density=density,
        values=values,
        vm_internals=vm_internals,
        overall_score=overall,
    )

    logger.info("overall score: %.1f%%  grade: %s", overall * 100, report.grade)

    # ── Render ─────────────────────────────────────────────────────────────
    if log_format == "human" and console:
        render_report(console, report)
    else:
        render_report_json(report)


if __name__ == "__main__":
    main()
