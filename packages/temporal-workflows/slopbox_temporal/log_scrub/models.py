"""Dataclass models for the log scrub workflow.

All types are plain dataclasses — Temporal serialises these to/from JSON via
its built-in dataclass converter without requiring Pydantic.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


# ---------------------------------------------------------------------------
# Shared value types
# ---------------------------------------------------------------------------


@dataclass
class TimeRange:
    start: datetime  # UTC-aware
    end: datetime    # UTC-aware


# ---------------------------------------------------------------------------
# Input / request models
# ---------------------------------------------------------------------------


@dataclass
class LogScrubRequest:
    index: str              # index pattern, e.g. "logs-myapp-*"
    query: dict             # ES Query DSL fragment (the "query" key value); {} means match_all
    time_range: TimeRange
    requests_per_second: float = 1_000.0  # DBQ throttle — total docs/s across all slices
    dry_run: bool = True                   # safe by default; set False to actually delete


# ---------------------------------------------------------------------------
# Activity parameter models
# ---------------------------------------------------------------------------


@dataclass
class DeleteIndexParams:
    index: str
    query: dict             # ES Query DSL fragment
    time_range: TimeRange
    requests_per_second: float
    dry_run: bool


# ---------------------------------------------------------------------------
# Activity result models
# ---------------------------------------------------------------------------


@dataclass
class DeleteIndexResult:
    index: str
    deleted: int    # docs deleted (0 in dry-run)
    took_ms: int    # ES-reported duration (0 in dry-run)


# ---------------------------------------------------------------------------
# Workflow result model
# ---------------------------------------------------------------------------


@dataclass
class LogScrubResult:
    indices_scrubbed: int
    total_deleted: int
    dry_run: bool
    per_index: list[DeleteIndexResult]
