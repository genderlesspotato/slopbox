"""Temporal activities for the Kibana log export workflow.

ES client is constructed inline — avoids pulling slopbox-tools (kubernetes,
rich) into the worker image.  S3 client uses boto3.

Exception handling strategy
----------------------------
Temporal's built-in retry handles transient failures (network blips, 5xx).
Activities only wrap errors that are *permanent* in ``ApplicationError`` with
``non_retryable=True`` so Temporal doesn't waste retry attempts on them:

* ``AuthenticationException``   — bad ES credentials (config problem)
* ``NotFoundError``             — index disappeared between resolve and open_pit
* ``NoCredentialsError``        — boto3 can't find AWS credentials (config problem)

All other exceptions propagate naturally and are retried by Temporal.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
from datetime import timezone

import boto3
import botocore.exceptions
from elasticsearch import AuthenticationException, Elasticsearch, NotFoundError
from temporalio import activity
from temporalio.exceptions import ApplicationError

from .models import (
    CleanupParams,
    ExportChunkParams,
    ExportChunkResult,
    KibanaLogExportRequest,
    WriteManifestParams,
)

logger = logging.getLogger("kibana_export")

# ---------------------------------------------------------------------------
# Max export window and document ceiling
# ---------------------------------------------------------------------------

MAX_EXPORT_DAYS = 14
MAX_EXPORT_DOCS = 250_000


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_es_client() -> Elasticsearch:
    """Construct an Elasticsearch client from environment variables."""
    host = os.environ.get("ES_HOST")
    cloud_id = os.environ.get("ES_CLOUD_ID")
    api_key = os.environ.get("ES_API_KEY")
    username = os.environ.get("ES_USERNAME")
    password = os.environ.get("ES_PASSWORD")

    kwargs: dict = {}
    if api_key:
        kwargs["api_key"] = api_key
    elif username and password:
        kwargs["basic_auth"] = (username, password)

    if cloud_id:
        return Elasticsearch(cloud_id=cloud_id, **kwargs)
    if host:
        return Elasticsearch(host, **kwargs)
    raise ApplicationError(
        "ES_HOST or ES_CLOUD_ID must be set",
        non_retryable=True,
    )


def _build_query(params: ExportChunkParams) -> dict:
    """Build the ES search body for a PIT-paginated chunk request."""
    time_filter = {
        "range": {
            "@timestamp": {
                "gte": params.time_range.start.astimezone(timezone.utc).isoformat(),
                "lte": params.time_range.end.astimezone(timezone.utc).isoformat(),
            }
        }
    }
    must_clauses = [params.query] if params.query else []
    body: dict = {
        "pit": {"id": params.pit_id, "keep_alive": "5m"},
        "query": {
            "bool": {
                "must": must_clauses,
                "filter": [time_filter],
            }
        },
        "sort": [{"@timestamp": "asc"}, {"_shard_doc": "asc"}],
        "_source": True,
        "size": params.chunk_size,
    }
    if params.search_after is not None:
        body["search_after"] = params.search_after
    return body


# ---------------------------------------------------------------------------
# Activities
# ---------------------------------------------------------------------------


@activity.defn
async def validate_export_request(request: KibanaLogExportRequest) -> int:
    """Validate the export request and return the matching document count.

    Raises ``ApplicationError`` (non-retryable) if:
    - The time range exceeds MAX_EXPORT_DAYS
    - The matching document count exceeds MAX_EXPORT_DOCS
    """
    delta = request.time_range.end - request.time_range.start
    if delta.total_seconds() > MAX_EXPORT_DAYS * 86_400:
        raise ApplicationError(
            f"Exports longer than {MAX_EXPORT_DAYS} days aren't allowed, "
            "use System Acme instead",
            non_retryable=True,
        )

    if request.dry_run:
        logger.info(
            "running in dry-run mode (set dry_run=False to enable S3 export)"
        )

    try:
        es = _build_es_client()
    except ApplicationError:
        raise

    time_filter = {
        "range": {
            "@timestamp": {
                "gte": request.time_range.start.astimezone(timezone.utc).isoformat(),
                "lte": request.time_range.end.astimezone(timezone.utc).isoformat(),
            }
        }
    }
    must_clauses = [request.query] if request.query else []
    count_body = {
        "query": {
            "bool": {
                "must": must_clauses,
                "filter": [time_filter],
            }
        }
    }

    try:
        result = es.count(index=request.index, body=count_body)
    except AuthenticationException as exc:
        raise ApplicationError(
            f"ES authentication failed: {exc}", non_retryable=True
        ) from exc

    count: int = result["count"]
    if count > MAX_EXPORT_DOCS:
        raise ApplicationError(
            f"The query matches {count:,} documents (>{MAX_EXPORT_DOCS:,}), "
            "use System Fu instead",
            non_retryable=True,
        )

    logger.info("export validated: index=%s count=%d", request.index, count)
    return count


@activity.defn
async def resolve_indices(index: str, time_range) -> list[str]:  # type: ignore[type-arg]
    """Resolve the index pattern to concrete index names, sorted alphabetically.

    Alphabetical order is chronological for time-based names like
    ``logs-2024.01.15-000001``.  Returns an empty list when no indices match.
    """
    try:
        es = _build_es_client()
    except ApplicationError:
        raise

    try:
        result = es.cat.indices(index=index, h="index", s="index", format="json")
    except AuthenticationException as exc:
        raise ApplicationError(
            f"ES authentication failed: {exc}", non_retryable=True
        ) from exc
    except NotFoundError:
        # Pattern matches nothing — not an error.
        logger.info("resolve_indices: no indices match pattern %s", index)
        return []

    names = sorted(entry["index"] for entry in result)
    logger.info("resolved %d indices for pattern %s", len(names), index)
    return names


@activity.defn
async def open_pit(index: str) -> str:
    """Open a Point-in-Time on *index* and return the PIT id."""
    try:
        es = _build_es_client()
    except ApplicationError:
        raise

    try:
        result = es.open_point_in_time(index=index, keep_alive="5m")
    except AuthenticationException as exc:
        raise ApplicationError(
            f"ES authentication failed: {exc}", non_retryable=True
        ) from exc
    except NotFoundError as exc:
        raise ApplicationError(
            f"Index not found when opening PIT: {index} — {exc}",
            non_retryable=True,
        ) from exc

    pit_id: str = result["id"]
    logger.info("opened PIT on %s", index)
    return pit_id


@activity.defn
async def export_chunk(params: ExportChunkParams) -> ExportChunkResult:
    """Fetch one page of results via PIT + search_after and write it to S3.

    The BytesIO buffer holds one chunk (~chunk_size × avg_doc_size).  At
    10 000 docs × 2 KB average that is ~20 MB before compression — well within
    activity memory limits.  ``s3.put_object`` is atomic: either the full
    object is written or the call raises; there are no truncated partial
    objects in S3.

    Returns ``ExportChunkResult`` with:
    - ``new_pit_id``   — updated PIT id from the response (use for the next call)
    - ``search_after`` — sort values of the last hit; ``None`` when exhausted
    - ``done``         — ``True`` when hits < chunk_size (or 0)
    - ``s3_key``       — ``None`` when the page was empty (no S3 write)
    """
    try:
        es = _build_es_client()
    except ApplicationError:
        raise

    body = _build_query(params)

    try:
        result = es.search(body=body)
    except AuthenticationException as exc:
        raise ApplicationError(
            f"ES authentication failed: {exc}", non_retryable=True
        ) from exc

    hits = result["hits"]["hits"]
    new_pit_id: str = result["pit_id"]

    if not hits:
        return ExportChunkResult(
            chunk_index=params.chunk_index,
            docs_written=0,
            s3_key=None,
            new_pit_id=new_pit_id,
            search_after=None,
            done=True,
        )

    # Build a gzip-compressed NDJSON buffer (one chunk, bounded memory).
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for hit in hits:
            gz.write(json.dumps(hit["_source"]).encode())
            gz.write(b"\n")
    buf.seek(0)

    s3_key = f"{params.s3_prefix}chunk-{params.chunk_index:04d}.ndjson.gz"
    try:
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=params.s3_bucket,
            Key=s3_key,
            Body=buf,
            ContentEncoding="gzip",
            ContentType="application/x-ndjson",
        )
    except botocore.exceptions.NoCredentialsError as exc:
        raise ApplicationError(
            f"S3 credentials not found: {exc}", non_retryable=True
        ) from exc

    last_sort = hits[-1]["sort"]
    done = len(hits) < params.chunk_size

    logger.info(
        "chunk %04d: wrote %d docs to %s (done=%s)",
        params.chunk_index,
        len(hits),
        s3_key,
        done,
    )
    return ExportChunkResult(
        chunk_index=params.chunk_index,
        docs_written=len(hits),
        s3_key=s3_key,
        new_pit_id=new_pit_id,
        search_after=last_sort,
        done=done,
    )


@activity.defn
async def close_pit(pit_id: str) -> None:
    """Close a PIT to release ES segment locks."""
    try:
        es = _build_es_client()
    except ApplicationError:
        raise

    try:
        es.close_point_in_time(body={"id": pit_id})
    except Exception:
        # Best-effort: PIT expires on its own after keep_alive; swallow errors.
        logger.warning("close_pit failed for id %s (will expire naturally)", pit_id[:16])


@activity.defn
async def cleanup_partial_export(params: CleanupParams) -> None:
    """Delete S3 objects left behind by a failed or cancelled export.

    Uses ``delete_objects`` (batch delete, up to 1 000 keys per call).
    Missing keys are silently ignored — the call is idempotent.
    """
    if not params.s3_keys:
        return

    try:
        s3 = boto3.client("s3")
    except botocore.exceptions.NoCredentialsError as exc:
        raise ApplicationError(
            f"S3 credentials not found: {exc}", non_retryable=True
        ) from exc

    # delete_objects accepts at most 1 000 keys per request.
    batch_size = 1_000
    for i in range(0, len(params.s3_keys), batch_size):
        batch = params.s3_keys[i : i + batch_size]
        s3.delete_objects(
            Bucket=params.s3_bucket,
            Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
        )

    logger.info("cleanup: deleted %d S3 objects from %s", len(params.s3_keys), params.s3_bucket)


@activity.defn
async def write_manifest(params: WriteManifestParams) -> str:
    """Write ``manifest.json`` and ``README.txt`` to the S3 export prefix.

    Returns the manifest S3 key.
    """
    from datetime import datetime as _datetime

    exported_at = _datetime.now(tz=timezone.utc).isoformat()
    req = params.request

    chunk_entries = [
        {
            "index": c.chunk_index,
            "s3_key": c.s3_key,
            "docs": c.docs_written,
        }
        for c in params.chunks
        if c.s3_key is not None
    ]

    manifest = {
        "index": req.index,
        "query": req.query,
        "time_range": {
            "start": req.time_range.start.astimezone(timezone.utc).isoformat(),
            "end": req.time_range.end.astimezone(timezone.utc).isoformat(),
        },
        "resolved_indices": params.resolved_indices,
        "total_docs": params.total_docs,
        "total_chunks": len(chunk_entries),
        "chunk_size": req.chunk_size,
        "chunks": chunk_entries,
        "exported_at": exported_at,
    }

    first_key = chunk_entries[0]["s3_key"] if chunk_entries else f"{req.s3_prefix}chunk-0000.ndjson.gz"
    last_key = chunk_entries[-1]["s3_key"] if chunk_entries else first_key
    first_name = first_key.split("/")[-1]
    last_name = last_key.split("/")[-1]

    readme_lines = [
        "Kibana Log Export",
        "=================",
        "",
        f"Index:        {req.index}",
        f"Time range:   {req.time_range.start.astimezone(timezone.utc).isoformat()} → {req.time_range.end.astimezone(timezone.utc).isoformat()}",
        f"Total docs:   {params.total_docs:,}",
        f"Chunks:       {len(chunk_entries)} file(s) ({first_name} … {last_name})",
        f"Exported at:  {exported_at}",
        "",
        "Each file is a gzip-compressed newline-delimited JSON file (.ndjson.gz).",
        "",
        "--- Download & combine all chunks (macOS Terminal / zsh) ---",
        "",
        "# 1. Download everything",
        f"aws s3 cp --recursive s3://{req.s3_bucket}/{req.s3_prefix} ./export/",
        "",
        "# 2. Combine into a single compressed file (gzip members concatenate correctly)",
        "cat export/chunk-*.ndjson.gz > export/combined.ndjson.gz",
        "",
        "# 3. Or decompress directly while combining",
        "zcat export/chunk-*.ndjson.gz > export/combined.ndjson",
        "",
        "# 4. Inspect the first few records",
        "zcat export/chunk-0000.ndjson.gz | head -5 | python3 -m json.tool",
    ]
    readme_body = "\n".join(readme_lines) + "\n"

    try:
        s3 = boto3.client("s3")
        manifest_key = f"{req.s3_prefix}manifest.json"
        s3.put_object(
            Bucket=req.s3_bucket,
            Key=manifest_key,
            Body=json.dumps(manifest, indent=2).encode(),
            ContentType="application/json",
        )
        readme_key = f"{req.s3_prefix}README.txt"
        s3.put_object(
            Bucket=req.s3_bucket,
            Key=readme_key,
            Body=readme_body.encode(),
            ContentType="text/plain",
        )
    except botocore.exceptions.NoCredentialsError as exc:
        raise ApplicationError(
            f"S3 credentials not found: {exc}", non_retryable=True
        ) from exc

    logger.info("manifest written to s3://%s/%s", req.s3_bucket, manifest_key)
    return manifest_key
