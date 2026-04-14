"""Dataclass models for the Kibana log export workflow.

All types are plain dataclasses — Temporal serialises these to/from JSON via
its built-in dataclass converter without requiring Pydantic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


# ---------------------------------------------------------------------------
# Input / request models
# ---------------------------------------------------------------------------


@dataclass
class TimeRange:
    start: datetime  # UTC-aware
    end: datetime  # UTC-aware


@dataclass
class KibanaLogExportRequest:
    index: str  # ES index pattern, e.g. "logs-*"
    query: dict  # ES Query DSL fragment (the "query" key value); {} means match_all
    time_range: TimeRange
    s3_bucket: str
    s3_prefix: str  # e.g. "exports/2024/run-abc/"  (trailing slash required)
    chunk_size: int = 10_000
    dry_run: bool = True  # safe by default; set False to actually write to S3


# ---------------------------------------------------------------------------
# Activity parameter models
# ---------------------------------------------------------------------------


@dataclass
class ExportChunkParams:
    pit_id: str
    search_after: list | None
    query: dict
    time_range: TimeRange
    s3_bucket: str
    s3_prefix: str
    chunk_size: int
    chunk_index: int  # global across all indices; drives S3 key naming


@dataclass
class CleanupParams:
    s3_bucket: str
    s3_keys: list[str]  # S3 object keys to delete


@dataclass
class WriteManifestParams:
    request: KibanaLogExportRequest
    resolved_indices: list[str]
    chunks: list[ExportChunkResult]
    total_docs: int


# ---------------------------------------------------------------------------
# Activity result models
# ---------------------------------------------------------------------------


@dataclass
class ExportChunkResult:
    chunk_index: int
    docs_written: int
    s3_key: str | None  # None when the page was empty (no S3 write)
    new_pit_id: str  # PIT id from the response (may differ; use for the next call)
    search_after: list | None  # sort values of the last hit → next cursor
    done: bool  # True when hits < chunk_size (exhausted) or hits == 0


# ---------------------------------------------------------------------------
# Workflow result model
# ---------------------------------------------------------------------------


@dataclass
class KibanaLogExportResult:
    total_docs: int
    total_chunks: int
    s3_prefix: str
    manifest_key: str
