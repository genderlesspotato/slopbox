"""Unit tests for vm_comparison.py.

All VictoriaMetrics API calls are mocked — no running cluster required.
"""

import json
import math
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import vm_comparison
from vm_comparison import (
    _parse_window_seconds,
    _select_sample_metrics,
    _spread_timestamps,
    compute_overall_score,
    fetch_avg_values_at,
    fetch_series_counts_at,
    fetch_vm_internals,
    profile_cardinality,
    profile_coverage,
    profile_density,
    profile_values,
    profile_vm_internals,
    render_report_json,
)
from slopbox.vm_client import VmClient, _parse_prometheus_text
from slopbox_domain.metrics.cluster import VictoriaMetricsClusterConfig
from slopbox_domain.metrics.models import (
    CardinalityProfile,
    CoverageProfile,
    DensityProfile,
    DensityWindowResult,
    SimilarityReport,
    ValueProfile,
    VmInternalProfile,
)
from slopbox_domain.metrics.types import (
    RawLabelValuesResponse,
    RawQueryData,
    RawQueryResponse,
    RawQueryResultVector,
    RawTsdbData,
    RawTsdbEntry,
    RawTsdbStatus,
)
from slopbox_domain.registry import ClusterRegistry


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _cfg(name="vm-a", host="http://vm-a:8428", environment="prod"):
    return VictoriaMetricsClusterConfig(name=name, host=host, environment=environment)


def _client(name="vm-a", host="http://vm-a:8428"):
    return VmClient(host=host, name=name)


def _tsdb_status(total_series: int, entries: list[tuple[str, int]]) -> RawTsdbStatus:
    return RawTsdbStatus(
        status="success",
        data=RawTsdbData(
            totalSeries=total_series,
            totalSeriesAll=total_series,
            seriesCountByMetricName=[
                RawTsdbEntry(name=name, value=count) for name, count in entries
            ],
        ),
    )


def _query_response(results: dict[str, float]) -> RawQueryResponse:
    return RawQueryResponse(
        status="success",
        data=RawQueryData(
            resultType="vector",
            result=[
                RawQueryResultVector(
                    metric={"__name__": name},
                    value=[1700000000.0, str(value)],
                )
                for name, value in results.items()
            ],
        ),
    )


def _full_report(
    *,
    coverage: CoverageProfile | None = None,
    cardinality: CardinalityProfile | None = None,
    density: DensityProfile | None = None,
    values: ValueProfile | None = None,
) -> SimilarityReport:
    cov = coverage or CoverageProfile(
        total_a=100, total_b=100, shared=90, only_in_a=10, only_in_b=10, jaccard=0.818
    )
    card = cardinality or CardinalityProfile(
        total_series_a=1000, total_series_b=980, metrics_checked=90, median_ratio=0.95
    )
    dens = density or DensityProfile(windows=[
        DensityWindowResult(window="24h", metrics_checked=20, median_ratio=0.92, gap_metrics=[])
    ])
    return SimilarityReport(
        cluster_a="vm-a",
        cluster_b="vm-b",
        cluster_a_host="http://vm-a:8428",
        cluster_b_host="http://vm-b:8428",
        evaluated_at="2026-01-01T00:00:00Z",
        compare_window="24h",
        coverage=cov,
        cardinality=card,
        density=dens,
        values=values,
        vm_internals=None,
        overall_score=compute_overall_score(cov, card, dens, values),
    )


# ---------------------------------------------------------------------------
# _parse_window_seconds
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("window,expected", [
    ("1h", 3600),
    ("24h", 86400),
    ("7d", 604800),
    ("30m", 1800),
    ("1w", 604800),
])
def test_parse_window_seconds_valid(window, expected):
    assert _parse_window_seconds(window) == expected


def test_parse_window_seconds_invalid():
    with pytest.raises(ValueError, match="invalid duration"):
        _parse_window_seconds("24x")


# ---------------------------------------------------------------------------
# _spread_timestamps
# ---------------------------------------------------------------------------

def test_spread_timestamps_n1():
    ts = _spread_timestamps(1000.0, 3600, 1)
    assert ts == [1000.0]


def test_spread_timestamps_n5():
    end = 1000.0
    ts = _spread_timestamps(end, 3600, 5)
    assert len(ts) == 5
    assert ts[0] == pytest.approx(end)
    assert ts[-1] == pytest.approx(end - 3600)


# ---------------------------------------------------------------------------
# profile_coverage
# ---------------------------------------------------------------------------

def test_coverage_full_overlap():
    names = {"a", "b", "c"}
    cov = profile_coverage(names, names)
    assert cov.jaccard == 1.0
    assert cov.shared == 3
    assert cov.only_in_a == 0
    assert cov.only_in_b == 0


def test_coverage_no_overlap():
    cov = profile_coverage({"a", "b"}, {"c", "d"})
    assert cov.jaccard == 0.0
    assert cov.shared == 0
    assert cov.only_in_a == 2
    assert cov.only_in_b == 2


def test_coverage_partial_overlap():
    cov = profile_coverage({"a", "b", "c"}, {"b", "c", "d"})
    # shared=2, union=4  → 0.5
    assert cov.jaccard == pytest.approx(0.5)
    assert cov.shared == 2
    assert cov.only_in_a == 1
    assert cov.only_in_b == 1


def test_coverage_empty_sets():
    cov = profile_coverage(set(), set())
    assert cov.jaccard == 1.0


def test_coverage_score_pct_format():
    cov = profile_coverage({"a", "b"}, {"a", "b"})
    assert cov.score_pct == "100.0%"


# ---------------------------------------------------------------------------
# profile_cardinality
# ---------------------------------------------------------------------------

def test_cardinality_exact_match():
    tsdb = _tsdb_status(1000, [("metric_a", 100), ("metric_b", 200)])
    card = profile_cardinality(tsdb, tsdb, {"metric_a", "metric_b"})
    assert card.median_ratio == 1.0
    assert card.total_series_a == 1000
    assert card.metrics_checked == 2


def test_cardinality_asymmetric():
    tsdb_a = _tsdb_status(1000, [("metric_a", 100)])
    tsdb_b = _tsdb_status(800, [("metric_a", 80)])
    card = profile_cardinality(tsdb_a, tsdb_b, {"metric_a"})
    # 80/100 = 0.8
    assert card.median_ratio == pytest.approx(0.8)


def test_cardinality_one_side_missing():
    tsdb_a = _tsdb_status(1000, [("metric_a", 100)])
    tsdb_b = _tsdb_status(0, [])  # metric_a missing from tsdb_b
    card = profile_cardinality(tsdb_a, tsdb_b, {"metric_a"})
    assert card.median_ratio == 0.0


def test_cardinality_both_zero():
    tsdb_a = _tsdb_status(0, [])
    tsdb_b = _tsdb_status(0, [])
    card = profile_cardinality(tsdb_a, tsdb_b, {"metric_a"})
    # Both zero → treated as 1.0 (no data on either side)
    assert card.median_ratio == 1.0


def test_cardinality_total_delta_pct_equal():
    tsdb = _tsdb_status(1000, [])
    card = profile_cardinality(tsdb, tsdb, set())
    assert card.total_delta_pct == "0.0%"


def test_cardinality_total_delta_pct_nonzero():
    tsdb_a = _tsdb_status(1000, [])
    tsdb_b = _tsdb_status(900, [])
    card = profile_cardinality(tsdb_a, tsdb_b, set())
    # |1000-900| / max(1000,900) = 100/1000 = 10%
    assert card.total_delta_pct == "10.0%"


# ---------------------------------------------------------------------------
# profile_density (unit — patches fetch_series_counts_at)
# ---------------------------------------------------------------------------

def test_profile_density_perfect_match():
    client_a = _client("a")
    client_b = _client("b")
    counts = {"metric_a": 5.0, "metric_b": 3.0}

    with patch("vm_comparison.fetch_series_counts_at", return_value=counts):
        density = profile_density(client_a, client_b, ["metric_a", "metric_b"], n_timestamps=1)

    for w in density.windows:
        assert w.median_ratio == pytest.approx(1.0)
        assert w.gap_metrics == []


def test_profile_density_one_side_missing():
    client_a = _client("a")
    client_b = _client("b")

    def counts_side(client, metrics, ts):
        if client.name == "a":
            return {"metric_a": 5.0}
        return {}  # cluster B has nothing

    with patch("vm_comparison.fetch_series_counts_at", side_effect=counts_side):
        density = profile_density(client_a, client_b, ["metric_a"], n_timestamps=1)

    for w in density.windows:
        assert w.median_ratio == pytest.approx(0.0)
        assert "metric_a" in w.gap_metrics


def test_profile_density_both_missing_is_neutral():
    client_a = _client("a")
    client_b = _client("b")

    with patch("vm_comparison.fetch_series_counts_at", return_value={}):
        density = profile_density(client_a, client_b, ["metric_a"], n_timestamps=1)

    # Both absent → pair is skipped → metrics_checked=0, ratio defaults to 1.0
    for w in density.windows:
        assert w.metrics_checked == 0
        assert w.median_ratio == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# profile_values (unit — patches fetch_avg_values_at)
# ---------------------------------------------------------------------------

def test_profile_values_returns_none_when_no_metrics():
    result = profile_values(_client("a"), _client("b"), [], n_timestamps=5, window="24h", tolerance=0.02)
    assert result is None


def test_profile_values_returns_none_when_timestamps_zero():
    result = profile_values(_client("a"), _client("b"), ["m"], n_timestamps=0, window="24h", tolerance=0.02)
    assert result is None


def test_profile_values_perfect_match():
    vals = {"metric_a": 42.0}

    with patch("vm_comparison.fetch_avg_values_at", return_value=vals):
        result = profile_values(_client("a"), _client("b"), ["metric_a"], n_timestamps=2, window="1h", tolerance=0.02)

    assert result is not None
    assert result.samples_compared == 2  # 1 metric × 2 timestamps
    assert result.within_tolerance == 2
    assert result.match_ratio == pytest.approx(1.0)


def test_profile_values_outside_tolerance():
    def vals_side(client, metrics, ts):
        if client.name == "a":
            return {"metric_a": 100.0}
        return {"metric_a": 50.0}  # 50% difference, well outside 2% tolerance

    with patch("vm_comparison.fetch_avg_values_at", side_effect=vals_side):
        result = profile_values(_client("a"), _client("b"), ["metric_a"], n_timestamps=1, window="1h", tolerance=0.02)

    assert result is not None
    assert result.within_tolerance == 0
    assert result.match_ratio == pytest.approx(0.0)


def test_profile_values_within_tolerance():
    def vals_side(client, metrics, ts):
        if client.name == "a":
            return {"metric_a": 100.0}
        return {"metric_a": 101.0}  # 1% difference, within 2% tolerance

    with patch("vm_comparison.fetch_avg_values_at", side_effect=vals_side):
        result = profile_values(_client("a"), _client("b"), ["metric_a"], n_timestamps=1, window="1h", tolerance=0.02)

    assert result is not None
    assert result.within_tolerance == 1


def test_profile_values_both_zero_counts_as_match():
    with patch("vm_comparison.fetch_avg_values_at", return_value={"metric_a": 0.0}):
        result = profile_values(_client("a"), _client("b"), ["metric_a"], n_timestamps=1, window="1h", tolerance=0.02)

    assert result is not None
    assert result.within_tolerance == 1


def test_profile_values_one_side_missing_not_counted():
    def vals_side(client, metrics, ts):
        if client.name == "a":
            return {"metric_a": 100.0}
        return {}  # cluster B has no data

    with patch("vm_comparison.fetch_avg_values_at", side_effect=vals_side):
        result = profile_values(_client("a"), _client("b"), ["metric_a"], n_timestamps=1, window="1h", tolerance=0.02)

    assert result is not None
    assert result.samples_compared == 0
    assert result.match_ratio == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# profile_vm_internals
# ---------------------------------------------------------------------------

def test_vm_internals_both_present():
    a = {"vm_active_time_series": 1000.0, "vm_rows_per_insert_total": 5000.0}
    b = {"vm_active_time_series": 980.0, "vm_rows_per_insert_total": 5100.0}
    profile = profile_vm_internals(a, b)
    assert profile is not None
    names = {m.name for m in profile.metrics}
    assert "vm_active_time_series" in names
    assert "vm_rows_per_insert_total" in names


def test_vm_internals_ratio_computation():
    a = {"vm_active_time_series": 1000.0}
    b = {"vm_active_time_series": 500.0}
    profile = profile_vm_internals(a, b)
    assert profile is not None
    m = profile.metrics[0]
    assert m.ratio == pytest.approx(2.0)


def test_vm_internals_zero_denominator_is_nan():
    a = {"vm_active_time_series": 100.0}
    b = {"vm_active_time_series": 0.0}
    profile = profile_vm_internals(a, b)
    assert profile is not None
    m = profile.metrics[0]
    assert math.isnan(m.ratio)
    assert m.ratio_pct == "N/A"


def test_vm_internals_both_empty_returns_none():
    assert profile_vm_internals({}, {}) is None


def test_vm_internals_only_a_present():
    a = {"vm_active_time_series": 100.0}
    profile = profile_vm_internals(a, {})
    assert profile is not None
    m = profile.metrics[0]
    assert m.value_b == 0.0
    assert math.isnan(m.ratio)


# ---------------------------------------------------------------------------
# compute_overall_score
# ---------------------------------------------------------------------------

def test_overall_score_with_values():
    cov = CoverageProfile(total_a=10, total_b=10, shared=10, only_in_a=0, only_in_b=0, jaccard=1.0)
    card = CardinalityProfile(total_series_a=100, total_series_b=100, metrics_checked=10, median_ratio=1.0)
    dens = DensityProfile(windows=[DensityWindowResult(window="24h", metrics_checked=10, median_ratio=1.0, gap_metrics=[])])
    vals = ValueProfile(metrics_checked=5, timestamps_checked=5, samples_compared=25, within_tolerance=25, tolerance=0.02)
    score = compute_overall_score(cov, card, dens, vals)
    assert score == pytest.approx(1.0)


def test_overall_score_without_values_weights_sum_to_1():
    cov = CoverageProfile(total_a=10, total_b=10, shared=10, only_in_a=0, only_in_b=0, jaccard=1.0)
    card = CardinalityProfile(total_series_a=100, total_series_b=100, metrics_checked=10, median_ratio=1.0)
    dens = DensityProfile(windows=[DensityWindowResult(window="24h", metrics_checked=10, median_ratio=1.0, gap_metrics=[])])
    score = compute_overall_score(cov, card, dens, None)
    assert score == pytest.approx(1.0)


def test_overall_score_partial():
    cov = CoverageProfile(total_a=10, total_b=10, shared=9, only_in_a=1, only_in_b=0, jaccard=0.9)
    card = CardinalityProfile(total_series_a=100, total_series_b=90, metrics_checked=9, median_ratio=0.8)
    dens = DensityProfile(windows=[DensityWindowResult(window="24h", metrics_checked=9, median_ratio=0.7, gap_metrics=[])])
    score = compute_overall_score(cov, card, dens, None)
    expected = 0.9 * 0.27 + 0.8 * 0.27 + 0.7 * 0.46
    assert score == pytest.approx(expected)


# ---------------------------------------------------------------------------
# SimilarityReport.grade
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("score,expected_grade", [
    (0.98, "excellent"),
    (0.95, "excellent"),
    (0.90, "good"),
    (0.85, "good"),
    (0.75, "degraded"),
    (0.70, "degraded"),
    (0.69, "critical"),
    (0.50, "critical"),
])
def test_grade_thresholds(score, expected_grade):
    report = _full_report()
    # Override via model construction
    report2 = report.model_copy(update={"overall_score": score})
    assert report2.grade == expected_grade


# ---------------------------------------------------------------------------
# _parse_prometheus_text (VmClient helper)
# ---------------------------------------------------------------------------

def test_parse_prometheus_text_basic():
    text = """
# HELP vm_active_time_series Active time series
# TYPE vm_active_time_series gauge
vm_active_time_series 12345
vm_rows_per_insert_total 98765
"""
    result = _parse_prometheus_text(text)
    assert result["vm_active_time_series"] == 12345.0
    assert result["vm_rows_per_insert_total"] == 98765.0


def test_parse_prometheus_text_skips_labelled_metrics():
    text = 'vm_data_size_bytes{type="storage/big"} 999\nvm_active_time_series 42\n'
    result = _parse_prometheus_text(text)
    assert "vm_data_size_bytes" not in result
    assert result["vm_active_time_series"] == 42.0


def test_parse_prometheus_text_empty():
    assert _parse_prometheus_text("") == {}


def test_parse_prometheus_text_comments_only():
    assert _parse_prometheus_text("# HELP foo A metric\n# TYPE foo gauge\n") == {}


# ---------------------------------------------------------------------------
# render_report_json
# ---------------------------------------------------------------------------

def test_render_report_json_valid_json(capsys):
    report = _full_report()
    render_report_json(report)
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    assert parsed["cluster_a"] == "vm-a"
    assert parsed["cluster_b"] == "vm-b"
    assert "coverage" in parsed
    assert "cardinality" in parsed
    assert "density" in parsed
    assert "overall_score" in parsed
    assert "grade" in parsed


def test_render_report_json_with_values(capsys):
    vals = ValueProfile(
        metrics_checked=5,
        timestamps_checked=3,
        samples_compared=15,
        within_tolerance=14,
        tolerance=0.02,
    )
    report = _full_report(values=vals)
    render_report_json(report)
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    assert parsed["values"]["metrics_checked"] == 5
    assert parsed["values"]["within_tolerance"] == 14


def test_render_report_json_null_values(capsys):
    report = _full_report(values=None)
    render_report_json(report)
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    assert parsed["values"] is None


# ---------------------------------------------------------------------------
# _select_sample_metrics
# ---------------------------------------------------------------------------

def test_select_sample_metrics_excludes_vm_internals():
    tsdb = _tsdb_status(100, [
        ("vm_active_time_series", 999),  # should be excluded
        ("http_requests_total", 500),
        ("up", 10),
    ])
    shared = {"vm_active_time_series", "http_requests_total", "up"}
    result = _select_sample_metrics(tsdb, tsdb, shared, n=10)
    assert "vm_active_time_series" not in result
    assert "http_requests_total" in result


def test_select_sample_metrics_respects_limit():
    entries = [(f"metric_{i}", i) for i in range(100)]
    tsdb = _tsdb_status(1000, entries)
    shared = {name for name, _ in entries}
    result = _select_sample_metrics(tsdb, tsdb, shared, n=10)
    assert len(result) == 10


def test_select_sample_metrics_orders_by_series_count():
    tsdb_a = _tsdb_status(200, [("big_metric", 100), ("small_metric", 10)])
    tsdb_b = _tsdb_status(200, [("big_metric", 90), ("small_metric", 5)])
    shared = {"big_metric", "small_metric"}
    result = _select_sample_metrics(tsdb_a, tsdb_b, shared, n=2)
    assert result[0] == "big_metric"


# ---------------------------------------------------------------------------
# Registry integration (ClusterRegistry.vm())
# ---------------------------------------------------------------------------

def test_registry_vm_filter_by_name(tmp_path):
    yaml_content = """
victoriametrics:
  - name: vm-a
    host: http://vm-a:8428
    environment: prod
  - name: vm-b
    host: http://vm-b:8428
    environment: prod
"""
    p = tmp_path / "clusters.yaml"
    p.write_text(yaml_content)
    registry = ClusterRegistry.from_yaml(p)
    assert len(registry.vm(name="vm-a")) == 1
    assert registry.vm(name="vm-a")[0].host == "http://vm-a:8428"
    assert len(registry.vm(name="unknown")) == 0


def test_registry_vm_filter_by_environment(tmp_path):
    yaml_content = """
victoriametrics:
  - name: vm-prod
    host: http://vm-prod:8428
    environment: prod
  - name: vm-staging
    host: http://vm-staging:8428
    environment: staging
"""
    p = tmp_path / "clusters.yaml"
    p.write_text(yaml_content)
    registry = ClusterRegistry.from_yaml(p)
    prod = registry.vm(environment="prod")
    assert len(prod) == 1
    assert prod[0].name == "vm-prod"


def test_registry_vm_empty_when_no_section(tmp_path):
    yaml_content = "elasticsearch: []\nkubernetes: []\n"
    p = tmp_path / "clusters.yaml"
    p.write_text(yaml_content)
    registry = ClusterRegistry.from_yaml(p)
    assert registry.vm() == []
