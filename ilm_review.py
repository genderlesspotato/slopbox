#!/usr/bin/env python3
"""Elasticsearch ILM policy review tool.

Profiles ILM-managed indices to help identify rollover frequency issues.
Reads credentials from environment variables, makes exactly 3 API calls.

Environment variables:
    ES_HOST       - Elasticsearch URL (e.g. https://localhost:9200)
    ES_CLOUD_ID   - Elastic Cloud ID (overrides ES_HOST)
    ES_API_KEY    - API key (base64 id:key), takes precedence over basic auth
    ES_USERNAME   - Basic auth username
    ES_PASSWORD   - Basic auth password
"""

import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from elasticsearch import Elasticsearch, AuthenticationException, ConnectionError, TransportError
from rich.console import Console
from rich.rule import Rule
from rich.table import Table
from rich import box


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class IndexProfile:
    name: str
    policy: str
    phase: str
    phase_time: str      # human-readable time in current phase
    index_age: str       # human-readable total index age
    index_age_days: float
    docs: int
    size_bytes: int
    size_human: str
    health: str
    status: str


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def format_bytes(n: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def format_duration(days: float) -> str:
    if days < 0:
        return "unknown"
    if days < 1:
        hours = int(days * 24)
        mins = int((days * 24 - hours) * 60)
        if hours == 0:
            return f"{mins}m"
        return f"{hours}h {mins}m"
    d = int(days)
    h = int((days - d) * 24)
    return f"{d}d {h}h" if h else f"{d}d"


def phase_style(phase: str) -> str:
    return {
        "hot": "yellow",
        "warm": "blue",
        "cold": "cyan",
        "frozen": "magenta",
        "delete": "red",
    }.get(phase, "white")


def health_style(health: str) -> str:
    return {
        "green": "green",
        "yellow": "yellow",
        "red": "red",
    }.get(health, "white")


# ---------------------------------------------------------------------------
# Client construction
# ---------------------------------------------------------------------------

def build_client() -> Elasticsearch:
    host = os.environ.get("ES_HOST")
    cloud_id = os.environ.get("ES_CLOUD_ID")
    api_key = os.environ.get("ES_API_KEY")
    username = os.environ.get("ES_USERNAME")
    password = os.environ.get("ES_PASSWORD")

    if not host and not cloud_id:
        sys.exit("ERROR: ES_HOST or ES_CLOUD_ID must be set")
    if not api_key and not (username and password):
        sys.exit("ERROR: ES_API_KEY or both ES_USERNAME and ES_PASSWORD must be set")

    kwargs: dict = {}

    if cloud_id:
        kwargs["cloud_id"] = cloud_id
    else:
        kwargs["hosts"] = [host]

    if api_key:
        kwargs["api_key"] = api_key
    else:
        kwargs["basic_auth"] = (username, password)

    return Elasticsearch(**kwargs)


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
    """GET /_cat/indices with raw byte sizes and creation epoch."""
    return list(
        client.cat.indices(
            index="*",
            format="json",
            h="index,docs.count,store.size,creation.date.epoch,health,status",
            bytes="b",
            expand_wildcards="all",
        )
    )


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
) -> dict:
    """Join ILM explain + cat stats → grouped by policy name."""
    now_ms = time.time() * 1000
    grouped = defaultdict(list)
    skipped = 0

    for index_name, ilm_info in ilm_indices.items():
        cat_info = cat_by_index.get(index_name)
        if cat_info is None:
            skipped += 1
            continue

        policy_name = ilm_info.get("policy", "unknown")
        phase = ilm_info.get("phase", "unknown")

        # Index age from creation epoch
        creation_epoch_ms = int(cat_info.get("creation.date.epoch") or 0)
        age_days = (now_ms - creation_epoch_ms) / 86_400_000 if creation_epoch_ms else -1.0

        # Phase age from ILM phase execution timestamp
        phase_entry_ms = (ilm_info.get("phase_execution") or {}).get("modified_date_in_millis")
        phase_age_days = (now_ms - phase_entry_ms) / 86_400_000 if phase_entry_ms else -1.0

        size_bytes = int(cat_info.get("store.size") or 0)

        profile = IndexProfile(
            name=index_name,
            policy=policy_name,
            phase=phase,
            phase_time=format_duration(phase_age_days),
            index_age=format_duration(age_days),
            index_age_days=age_days,
            docs=int(cat_info.get("docs.count") or 0),
            size_bytes=size_bytes,
            size_human=format_bytes(size_bytes),
            health=cat_info.get("health", "unknown"),
            status=cat_info.get("status", "unknown"),
        )
        grouped[policy_name].append(profile)

    return dict(grouped), skipped


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------

def rollover_criteria_str(criteria: dict) -> str:
    if not criteria:
        return "none"
    return "  ".join(f"{k}={v}" for k, v in criteria.items())


def render_report(console: Console, grouped: dict, policies: dict, skipped: int) -> None:
    total_indices = sum(len(v) for v in grouped.values())
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    console.print()
    console.rule("[bold]Elasticsearch ILM Policy Review[/bold]")
    console.print(
        f"  Generated: [dim]{now_str}[/dim]  |  "
        f"Policies: [bold]{len(grouped)}[/bold]  |  "
        f"Managed indices: [bold]{total_indices}[/bold]"
    )
    console.print()

    for policy_name in sorted(grouped.keys()):
        profiles = grouped[policy_name]
        policy_info = policies.get(policy_name, {})
        criteria = policy_info.get("rollover", {})
        phase_list = policy_info.get("phases", [])

        console.rule(
            f"[bold cyan]{policy_name}[/bold cyan]  "
            f"[dim]{len(profiles)} {'index' if len(profiles) == 1 else 'indices'}[/dim]"
        )

        # Policy metadata line
        if policy_name not in policies:
            console.print("  [yellow]Warning: policy definition not found[/yellow]")
        else:
            phases_str = " → ".join(phase_list) if phase_list else "none"
            console.print(f"  Phases: [dim]{phases_str}[/dim]")
            console.print(f"  Rollover criteria: [bold]{rollover_criteria_str(criteria)}[/bold]")
        console.print()

        # Sort by index name (typically includes generation suffix)
        profiles_sorted = sorted(profiles, key=lambda p: p.name)

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
        table.add_column("Health", justify="center")
        table.add_column("Status", justify="center")

        total_docs = 0
        total_bytes = 0

        for p in profiles_sorted:
            style = phase_style(p.phase)
            hstyle = health_style(p.health)
            table.add_row(
                p.name,
                f"[{style}]{p.phase}[/{style}]",
                p.index_age,
                p.phase_time,
                f"{p.docs:,}",
                p.size_human,
                f"[{hstyle}]{p.health}[/{hstyle}]",
                p.status,
            )
            total_docs += p.docs
            total_bytes += p.size_bytes

        console.print(table)
        console.print(
            f"  Summary: [bold]{len(profiles)}[/bold] indices  |  "
            f"Docs: [bold]{total_docs:,}[/bold]  |  "
            f"Size: [bold]{format_bytes(total_bytes)}[/bold]"
        )
        console.print()

    if skipped:
        console.print(f"[dim]Note: {skipped} indices skipped (disappeared between API calls)[/dim]")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    console = Console()

    client = build_client()

    try:
        with console.status("Fetching ILM policies..."):
            policies_raw = fetch_ilm_policies(client)

        with console.status(f"Fetching ILM state for managed indices..."):
            ilm_raw = fetch_ilm_explain(client)

        with console.status("Fetching index stats..."):
            cat_raw = fetch_cat_indices(client)

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

    grouped, skipped = correlate_data(ilm_indices, cat_by_index, policies)
    render_report(console, grouped, policies, skipped)


if __name__ == "__main__":
    main()
