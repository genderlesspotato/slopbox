#!/usr/bin/env python3
"""Elasticsearch ILM policy review and profiling tool.

Profiles ILM-managed indices to identify rollover frequency imbalances and emit
concrete, actionable recommendations for ILM policy and index template changes.
Makes exactly 5 API calls per run.

Environment variables:
    ES_HOST       - Elasticsearch URL (e.g. https://localhost:9200)
    ES_CLOUD_ID   - Elastic Cloud ID (overrides ES_HOST)
    ES_API_KEY    - API key (base64 id:key), takes precedence over basic auth
    ES_USERNAME   - Basic auth username
    ES_PASSWORD   - Basic auth password
"""

import math
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone

from elasticsearch import Elasticsearch, AuthenticationException, ConnectionError, TransportError
from rich.console import Console
from rich.table import Table
from rich import box

from slopbox.client import build_client
from slopbox.formatting import format_bytes, format_duration, phase_style, health_style

from domain.es.models import IndexProfile
from domain.es.types import RawCatIndexEntry, RawIlmExplainEntry, RawDataStream


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Rotation cadence targets
TARGET_MIN_HOURS = 6.0   # rotating faster than this → too many indices
TARGET_MAX_HOURS = 24.0  # rotating slower than this → too few indices

# Hard limits
MAX_SHARD_GB = 50
MAX_SHARD_BYTES = MAX_SHARD_GB * 1024**3

# Fraction of data nodes a single index's shards may spread across before
# the "split into multiple indices" recommendation kicks in.
MAX_NODE_FRACTION = 0.6


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------

def fetch_ilm_policies(client: Elasticsearch) -> dict:
    """GET /_ilm/policy — returns all ILM policies."""
    return dict(client.ilm.get_lifecycle())


def fetch_ilm_explain(client: Elasticsearch) -> dict:
    """GET /*/_ilm/explain?only_managed=true — ILM state for managed indices."""
    resp = client.ilm.explain_lifecycle(index="*", only_managed=True)
    return dict(resp)


def fetch_cat_indices(client: Elasticsearch) -> list:
    """GET /_cat/indices with raw byte sizes, shard count, and creation epoch."""
    return list(
        client.cat.indices(
            index="*",
            format="json",
            h="index,docs.count,store.size,creation.date.epoch,pri,health,status",
            bytes="b",
            expand_wildcards="all",
        )
    )


def fetch_cat_nodes(client: Elasticsearch) -> int:
    """GET /_cat/nodes — returns count of data-role nodes.

    Counts nodes whose role string contains 'd' (data role).  The role column
    in compact form looks like 'dimr', 'dir', 'd', etc.
    """
    rows = client.cat.nodes(format="json", h="node.role")
    return sum(1 for row in rows if "d" in row.get("node.role", ""))


def fetch_data_streams(client: Elasticsearch) -> list[RawDataStream]:
    """GET /_data_stream/* — backing index lists and template names per stream."""
    resp = client.indices.get_data_stream(name="*")
    return [
        RawDataStream.model_validate(ds)
        for ds in resp.get("data_streams", [])
    ]


# ---------------------------------------------------------------------------
# Policy parsing
# ---------------------------------------------------------------------------

def parse_rollover_criteria(policy_body: dict) -> dict:
    """Extract rollover criteria from a single policy's body."""
    phases = policy_body.get("policy", {}).get("phases", {})
    for phase_data in phases.values():
        rollover = phase_data.get("actions", {}).get("rollover", {})
        if rollover:
            keys = ["max_age", "max_size", "max_docs", "max_primary_shard_size", "max_primary_shard_docs"]
            return {k: rollover[k] for k in keys if k in rollover}
    return {}


def parse_all_policies(policies_raw: dict) -> dict:
    """Return {policy_name: {"rollover": {...}, "phases": [...]}}."""
    result = {}
    for name, body in policies_raw.items():
        phases = list(body.get("policy", {}).get("phases", {}).keys())
        result[name] = {
            "rollover": parse_rollover_criteria(body),
            "phases": phases,
        }
    return result


# ---------------------------------------------------------------------------
# Data correlation
# ---------------------------------------------------------------------------

def correlate_data(
    ilm_indices: dict,
    cat_by_index: dict,
    policies: dict,
    data_streams: list[RawDataStream],
) -> tuple[dict, int]:
    """Join ILM explain + cat stats + data stream membership → grouped by policy."""
    now_ms = time.time() * 1000
    grouped: dict = defaultdict(list)
    skipped = 0

    # Build lookups from data stream definitions
    index_to_ds: dict[str, str] = {}
    write_indices: set[str] = set()
    for ds in data_streams:
        for idx in ds.indices:
            index_to_ds[idx.index_name] = ds.name
        if ds.indices:
            write_indices.add(ds.indices[-1].index_name)

    for index_name, ilm_info in ilm_indices.items():
        cat_info = cat_by_index.get(index_name)
        if cat_info is None:
            skipped += 1
            continue

        raw_ilm = RawIlmExplainEntry.model_validate(ilm_info)
        raw_cat = RawCatIndexEntry.model_validate(cat_info)

        # Index age from creation epoch
        age_days = (
            (now_ms - raw_cat.creation_epoch_ms) / 86_400_000
            if raw_cat.creation_epoch_ms
            else -1.0
        )

        # Phase age from ILM phase execution timestamp
        phase_entry_ms = (
            raw_ilm.phase_execution.modified_date_in_millis
            if raw_ilm.phase_execution
            else None
        )
        phase_age_days = (
            (now_ms - phase_entry_ms) / 86_400_000 if phase_entry_ms else -1.0
        )

        # is_write_index: prefer ILM explain field, fall back to _data_stream position
        is_write = raw_ilm.is_write_index or (index_name in write_indices)

        profile = IndexProfile(
            name=index_name,
            policy=raw_ilm.policy,
            phase=raw_ilm.phase,
            phase_age_days=phase_age_days,
            index_age_days=age_days,
            docs=raw_cat.docs_count,
            size_bytes=raw_cat.store_size_bytes,
            primary_shards=max(raw_cat.primary_shards, 1),
            is_write_index=is_write,
            data_stream=index_to_ds.get(index_name),
            creation_epoch_ms=raw_cat.creation_epoch_ms,
            health=raw_cat.health,
            status=raw_cat.status,
        )
        grouped[raw_ilm.policy].append(profile)

    return dict(grouped), skipped


# ---------------------------------------------------------------------------
# Profiling logic
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DataStreamProfile:
    name: str          # data stream name (or index-name prefix for standalone)
    template: str      # index template name ("unknown" if not a data stream)
    policy: str
    avg_rotation_hours: float | None   # None if < 2 indices (insufficient history)
    avg_shard_size_bytes: int | None   # None if no closed indices to measure
    primary_shards: int
    max_shard_size_str: str | None     # configured max_primary_shard_size or None
    recommendation: str
    detail: str | None


def _parse_size_bytes(size_str: str) -> int:
    """Parse an Elasticsearch size string (e.g. '50gb', '512mb') to bytes."""
    s = size_str.strip().lower()
    units = {"b": 1, "kb": 1024, "mb": 1024**2, "gb": 1024**3, "tb": 1024**4}
    for suffix, multiplier in sorted(units.items(), key=lambda x: -len(x[0])):
        if s.endswith(suffix):
            return int(float(s[: -len(suffix)]) * multiplier)
    return int(s)  # bare number → bytes


def _recommend(
    avg_rotation_hours: float,
    avg_shard_size_bytes: int,
    primary_shards: int,
    max_shard_size_str: str | None,
    data_node_count: int,
) -> tuple[str, str | None]:
    """Return (recommendation, detail) for a single data stream."""
    configured_max_bytes = (
        _parse_size_bytes(max_shard_size_str) if max_shard_size_str else None
    )

    if avg_rotation_hours < TARGET_MIN_HOURS:
        # Rotating too fast — need more capacity per index
        multiplier = TARGET_MIN_HOURS / avg_rotation_hours

        if configured_max_bytes is None or configured_max_bytes < MAX_SHARD_BYTES:
            # Lever 1: increase max_primary_shard_size
            base = configured_max_bytes or avg_shard_size_bytes or MAX_SHARD_BYTES
            new_bytes = min(base * multiplier, MAX_SHARD_BYTES)
            new_gb = math.ceil(new_bytes / 1024**3)
            return "increase max_primary_shard_size", f"{new_gb}gb"

        node_cap = max(1, int(data_node_count * MAX_NODE_FRACTION))
        if primary_shards < node_cap:
            # Lever 2: increase number_of_shards
            new_shards = min(math.ceil(primary_shards * multiplier), node_cap)
            return "increase number_of_shards", str(new_shards)

        # Both levers exhausted
        return (
            "SPLIT into multiple independent indices",
            f"shards at {MAX_SHARD_GB}gb limit, {primary_shards} pri ≥ "
            f"{MAX_NODE_FRACTION:.0%} of {data_node_count} data nodes",
        )

    if avg_rotation_hours > TARGET_MAX_HOURS:
        # Rotating too slowly — reduce capacity per index
        multiplier = TARGET_MAX_HOURS / avg_rotation_hours  # < 1

        if primary_shards > 1:
            new_shards = max(1, round(primary_shards * multiplier))
            return "decrease number_of_shards", str(new_shards)

        if configured_max_bytes:
            new_bytes = max(1024**3, configured_max_bytes * multiplier)  # floor 1gb
            new_gb = math.floor(new_bytes / 1024**3)
            return "decrease max_primary_shard_size", f"{new_gb}gb"

        return "decrease max_primary_shard_size", None

    return "OK", None


def profile_data_streams(
    grouped: dict,
    policies: dict,
    data_streams: list[RawDataStream],
    data_node_count: int,
) -> list[DataStreamProfile]:
    """Profile each data stream's rotation cadence and emit recommendations."""
    # Build data-stream-name → template lookup
    ds_template: dict[str, str] = {ds.name: ds.template for ds in data_streams}

    # Collect all profiles, then group by data_stream (or policy+name-prefix fallback)
    all_profiles: list[IndexProfile] = [
        p for profiles in grouped.values() for p in profiles
    ]

    # Group by data stream name; standalone indices use their own name as the key
    by_stream: dict[str, list[IndexProfile]] = defaultdict(list)
    for p in all_profiles:
        key = p.data_stream if p.data_stream else p.name
        by_stream[key].append(p)

    result: list[DataStreamProfile] = []

    for stream_name, profiles in sorted(by_stream.items()):
        # All profiles in a data stream share one policy (the most common one wins
        # on the off chance they diverge due to a mid-migration policy rename).
        policy_name = max(
            set(p.policy for p in profiles),
            key=lambda pol: sum(1 for p in profiles if p.policy == pol),
        )
        policy_info = policies.get(policy_name, {})
        max_shard_size_str: str | None = policy_info.get("rollover", {}).get("max_primary_shard_size")

        # Sort closed (rolled-over) indices by creation time for cadence calculation
        closed = sorted(
            [p for p in profiles if not p.is_write_index],
            key=lambda p: p.creation_epoch_ms,
        )

        # Rotation cadence: time between consecutive index creations
        avg_rotation_hours: float | None = None
        if len(closed) >= 2:
            gaps = [
                (closed[i + 1].creation_epoch_ms - closed[i].creation_epoch_ms) / 3_600_000
                for i in range(len(closed) - 1)
            ]
            avg_rotation_hours = sum(gaps) / len(gaps)
        elif len(profiles) < 2:
            # Only one index exists — can't measure cadence
            avg_rotation_hours = None
        # else: only one closed + one write index — still insufficient pairs

        # Average shard size from closed indices with non-zero size
        measurable = [p for p in closed if p.size_bytes > 0]
        avg_shard_size_bytes: int | None = (
            int(sum(p.shard_size_bytes for p in measurable) / len(measurable))
            if measurable else None
        )

        primary_shards = profiles[0].primary_shards  # same template → same shard count

        template = ds_template.get(stream_name, "unknown")

        if avg_rotation_hours is None:
            recommendation = "insufficient history"
            detail = "need ≥ 2 rolled-over indices to compute cadence"
        elif max_shard_size_str is None:
            recommendation = "cannot profile"
            detail = "policy does not use max_primary_shard_size"
        else:
            recommendation, detail = _recommend(
                avg_rotation_hours=avg_rotation_hours,
                avg_shard_size_bytes=avg_shard_size_bytes or 0,
                primary_shards=primary_shards,
                max_shard_size_str=max_shard_size_str,
                data_node_count=data_node_count,
            )

        result.append(DataStreamProfile(
            name=stream_name,
            template=template,
            policy=policy_name,
            avg_rotation_hours=avg_rotation_hours,
            avg_shard_size_bytes=avg_shard_size_bytes,
            primary_shards=primary_shards,
            max_shard_size_str=max_shard_size_str,
            recommendation=recommendation,
            detail=detail,
        ))

    return result


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------

def rollover_criteria_str(criteria: dict) -> str:
    if not criteria:
        return "none"
    return "  ".join(f"{k}={v}" for k, v in criteria.items())


def render_report(
    console: Console,
    grouped: dict,
    policies: dict,
    skipped: int,
    profiles: list[DataStreamProfile],
    data_node_count: int,
) -> None:
    total_indices = sum(len(v) for v in grouped.values())
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    console.print()
    console.rule("[bold]Elasticsearch ILM Policy Review[/bold]")
    console.print(
        f"  Generated: [dim]{now_str}[/dim]  |  "
        f"Policies: [bold]{len(grouped)}[/bold]  |  "
        f"Managed indices: [bold]{total_indices}[/bold]  |  "
        f"Data nodes: [bold]{data_node_count}[/bold]"
    )
    console.print()

    for policy_name in sorted(grouped.keys()):
        policy_profiles = grouped[policy_name]
        policy_info = policies.get(policy_name, {})
        criteria = policy_info.get("rollover", {})
        phase_list = policy_info.get("phases", [])

        console.rule(
            f"[bold cyan]{policy_name}[/bold cyan]  "
            f"[dim]{len(policy_profiles)} {'index' if len(policy_profiles) == 1 else 'indices'}[/dim]"
        )

        if policy_name not in policies:
            console.print("  [yellow]Warning: policy definition not found[/yellow]")
        else:
            phases_str = " → ".join(phase_list) if phase_list else "none"
            console.print(f"  Phases: [dim]{phases_str}[/dim]")
            console.print(f"  Rollover criteria: [bold]{rollover_criteria_str(criteria)}[/bold]")
        console.print()

        profiles_sorted = sorted(policy_profiles, key=lambda p: p.name)

        table = Table(
            box=box.SIMPLE_HEAD,
            show_header=True,
            header_style="bold",
            pad_edge=False,
        )
        table.add_column("Index", style="white", no_wrap=True)
        table.add_column("Phase", no_wrap=True)
        table.add_column("Index Age", justify="right")
        table.add_column("Phase Age", justify="right")
        table.add_column("Docs", justify="right")
        table.add_column("Size", justify="right")
        table.add_column("Pri", justify="right")
        table.add_column("Shard Size", justify="right")
        table.add_column("Health", justify="center")
        table.add_column("Status", justify="center")

        total_docs = 0
        total_bytes = 0

        for p in profiles_sorted:
            style = phase_style(p.phase)
            hstyle = health_style(p.health)
            write_marker = " [dim]✎[/dim]" if p.is_write_index else ""
            table.add_row(
                p.name + write_marker,
                f"[{style}]{p.phase}[/{style}]",
                p.index_age,
                p.phase_time,
                f"{p.docs:,}",
                p.size_human,
                str(p.primary_shards),
                p.shard_size_human,
                f"[{hstyle}]{p.health}[/{hstyle}]",
                p.status,
            )
            total_docs += p.docs
            total_bytes += p.size_bytes

        console.print(table)
        console.print(
            f"  Summary: [bold]{len(policy_profiles)}[/bold] indices  |  "
            f"Docs: [bold]{total_docs:,}[/bold]  |  "
            f"Size: [bold]{format_bytes(total_bytes)}[/bold]"
        )
        console.print()

    if skipped:
        console.print(f"[dim]Note: {skipped} indices skipped (disappeared between API calls)[/dim]")
        console.print()

    # -------------------------------------------------------------------------
    # Recommendations
    # -------------------------------------------------------------------------
    console.rule("[bold]ILM Recommendations[/bold]")
    console.print(
        f"  Target cadence: [dim]{TARGET_MIN_HOURS:.0f}h – {TARGET_MAX_HOURS:.0f}h per index[/dim]  |  "
        f"Max shard size: [dim]{MAX_SHARD_GB}gb[/dim]  |  "
        f"Max shard spread: [dim]{MAX_NODE_FRACTION:.0%} of data nodes[/dim]"
    )
    console.print()

    rec_table = Table(
        box=box.SIMPLE_HEAD,
        show_header=True,
        header_style="bold",
        pad_edge=False,
    )
    rec_table.add_column("Data Stream / Index", style="white", no_wrap=True)
    rec_table.add_column("Template", style="dim", no_wrap=True)
    rec_table.add_column("Avg Rotation", justify="right")
    rec_table.add_column("Avg Shard Size", justify="right")
    rec_table.add_column("Pri", justify="right")
    rec_table.add_column("Recommendation", no_wrap=False)

    for dp in profiles:
        rotation_str = (
            f"{dp.avg_rotation_hours:.1f}h" if dp.avg_rotation_hours is not None else "—"
        )
        shard_size_str = (
            format_bytes(dp.avg_shard_size_bytes) if dp.avg_shard_size_bytes is not None else "—"
        )

        if dp.recommendation == "OK":
            rec_str = "[green]OK[/green]"
        elif dp.recommendation in ("insufficient history", "cannot profile"):
            rec_str = f"[dim]{dp.recommendation}[/dim]"
        elif dp.recommendation.startswith("SPLIT"):
            rec_str = f"[bold red]{dp.recommendation}[/bold red]"
        elif dp.recommendation.startswith("increase"):
            rec_str = f"[yellow]{dp.recommendation}[/yellow]"
            if dp.detail:
                rec_str += f" [bold]{dp.detail}[/bold]"
        else:
            rec_str = f"[cyan]{dp.recommendation}[/cyan]"
            if dp.detail:
                rec_str += f" [bold]{dp.detail}[/bold]"

        if dp.recommendation == "SPLIT into multiple independent indices" and dp.detail:
            rec_str += f"\n  [dim]{dp.detail}[/dim]"

        rec_table.add_row(
            dp.name,
            dp.template,
            rotation_str,
            shard_size_str,
            str(dp.primary_shards),
            rec_str,
        )

    console.print(rec_table)
    console.print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    console = Console()

    client = build_client()

    try:
        with console.status("Fetching ILM policies..."):
            policies_raw = fetch_ilm_policies(client)

        with console.status("Fetching ILM state for managed indices..."):
            ilm_raw = fetch_ilm_explain(client)

        with console.status("Fetching index stats..."):
            cat_raw = fetch_cat_indices(client)

        with console.status("Fetching data stream definitions..."):
            data_streams = fetch_data_streams(client)

        with console.status("Fetching cluster node topology..."):
            data_node_count = fetch_cat_nodes(client)

    except AuthenticationException:
        sys.exit("ERROR: Authentication failed — check ES_USERNAME/ES_PASSWORD or ES_API_KEY")
    except ConnectionError as e:
        sys.exit(f"ERROR: Could not connect to Elasticsearch: {e}")
    except TransportError as e:
        sys.exit(f"ERROR: Elasticsearch API error: {e}")

    policies = parse_all_policies(policies_raw)
    cat_by_index = {item["index"]: item for item in cat_raw}
    ilm_indices = ilm_raw.get("indices", {})

    if not ilm_indices:
        console.print("[yellow]No ILM-managed indices found.[/yellow]")
        return

    grouped, skipped = correlate_data(ilm_indices, cat_by_index, policies, data_streams)
    stream_profiles = profile_data_streams(grouped, policies, data_streams, data_node_count)
    render_report(console, grouped, policies, skipped, stream_profiles, data_node_count)


if __name__ == "__main__":
    main()
