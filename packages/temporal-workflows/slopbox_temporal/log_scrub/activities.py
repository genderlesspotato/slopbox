"""Temporal activities for the log scrub workflow.

ES client is constructed inline — avoids pulling slopbox-tools (kubernetes,
rich) into the worker image.

Exception handling strategy
----------------------------
Temporal's built-in retry handles transient failures (network blips, 5xx).
Activities only wrap errors that are *permanent* in ``ApplicationError`` with
``non_retryable=True`` so Temporal doesn't waste retry attempts on them:

* ``AuthenticationException``  — bad ES credentials (config problem)
* ``NotFoundError``            — index disappeared between resolve and delete

Heartbeat-based task recovery
------------------------------
``delete_index_docs`` submits ``_delete_by_query`` with
``wait_for_completion=false`` and immediately heartbeats the returned task id.
On every poll iteration the task id is re-heartbeated.  If the activity worker
crashes and Temporal retries the activity, the new attempt reads the task id
from ``activity.info().heartbeat_details`` and resumes polling the existing
task instead of submitting a second DBQ.

If the original task can no longer be found (the ES node that owned it was
restarted or the task aged out of the tasks store) the activity falls back to
submitting a fresh DBQ.  Re-running DBQ on an already-scrubbed index is safe —
the query will match zero remaining documents and complete instantly.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timezone

from elasticsearch import AuthenticationException, NotFoundError
from temporalio import activity
from temporalio.exceptions import ApplicationError

from slopbox_temporal._shared.es_client import build_es_client as _build_es_client

from .models import DeleteIndexParams, DeleteIndexResult, LogScrubRequest

logger = logging.getLogger("log_scrub")

# ---------------------------------------------------------------------------
# Tuneable constants
# ---------------------------------------------------------------------------

MAX_SCRUB_DAYS = 366
TASK_POLL_INTERVAL_SECONDS = 15


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_delete_query(params: DeleteIndexParams) -> dict:
    """Build the ES query body for a delete_by_query request.

    The caller-supplied query fragment is placed in ``must``; the time range
    filter is always applied so deletes are strictly bounded to the window.
    An empty query dict (``{}``) is treated as match_all within the window.
    """
    time_filter = {
        "range": {
            "@timestamp": {
                "gte": params.time_range.start.astimezone(timezone.utc).isoformat(),
                "lte": params.time_range.end.astimezone(timezone.utc).isoformat(),
            }
        }
    }
    must_clauses = [params.query] if params.query else []
    return {
        "bool": {
            "must": must_clauses,
            "filter": [time_filter],
        }
    }


# ---------------------------------------------------------------------------
# Activities
# ---------------------------------------------------------------------------


@activity.defn
async def validate_scrub_request(request: LogScrubRequest) -> int:
    """Validate the scrub request and return the matching document count.

    The count gives the operator visibility into the scope of the operation
    before any deletes are issued.

    Raises ``ApplicationError`` (non-retryable) if the time range exceeds
    ``MAX_SCRUB_DAYS`` — split large windows into multiple workflow runs.
    """
    delta = request.time_range.end - request.time_range.start
    if delta.total_seconds() > MAX_SCRUB_DAYS * 86_400:
        raise ApplicationError(
            f"Scrub window exceeds {MAX_SCRUB_DAYS} days — split into multiple runs",
            non_retryable=True,
        )

    if request.dry_run:
        logger.info(
            "running in dry-run mode (set dry_run=False to enable deletions)"
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
    logger.info(
        "scrub validated: index=%s count=%d dry_run=%s",
        request.index,
        count,
        request.dry_run,
    )
    return count


@activity.defn
async def resolve_backing_indices(request: LogScrubRequest) -> list[str]:
    """Resolve the index pattern to a filtered list of concrete index names.

    Filters applied:

    1. Indices created *after* ``time_range.end`` are skipped — they cannot
       contain documents within the scrub window (assumes time-ordered data).
    2. The current write index of each matching data stream is excluded — it
       is actively receiving data and scrubbing it is almost always wrong.
       Write-index detection is best-effort: if the ``_data_stream`` API is
       unavailable or the pattern matches plain (non-datastream) indices, the
       step is skipped with a warning rather than aborting.

    Returns indices sorted by creation date ascending (oldest first), which
    is the natural processing order for a time-bounded scrub.
    """
    try:
        es = _build_es_client()
    except ApplicationError:
        raise

    # Step 1: fetch all concrete index names with their creation timestamps.
    try:
        cat_result = es.cat.indices(
            index=request.index,
            h="index,creation.date",
            format="json",
        )
    except AuthenticationException as exc:
        raise ApplicationError(
            f"ES authentication failed: {exc}", non_retryable=True
        ) from exc
    except NotFoundError:
        logger.info(
            "resolve_backing_indices: no indices match pattern %s", request.index
        )
        return []

    # Step 2: identify write indices via _data_stream API (best-effort).
    write_index_names: set[str] = set()
    try:
        ds_result = es.indices.get_data_stream(name=request.index)
        for ds in ds_result.get("data_streams", []):
            indices = ds.get("indices", [])
            if indices:
                write_index_names.add(indices[-1]["index_name"])
        if write_index_names:
            logger.info(
                "identified %d write index(es) to skip: %s",
                len(write_index_names),
                ", ".join(sorted(write_index_names)),
            )
    except Exception:
        # Pattern may not correspond to a data stream — not an error.
        logger.warning(
            "could not determine write indices for %s; write-index exclusion skipped",
            request.index,
        )

    # Step 3: apply filters, sort oldest-first.
    window_end_ms = int(request.time_range.end.timestamp() * 1000)
    entries = sorted(cat_result, key=lambda e: int(e.get("creation.date") or 0))

    kept: list[str] = []
    for entry in entries:
        name: str = entry["index"]
        creation_ms = int(entry.get("creation.date") or 0)

        if name in write_index_names:
            logger.info("skipping write index %s", name)
            continue
        if creation_ms > window_end_ms:
            logger.info(
                "skipping index %s (created after window end: %d > %d)",
                name,
                creation_ms,
                window_end_ms,
            )
            continue
        kept.append(name)

    logger.info(
        "resolved %d indices to scrub for pattern %s", len(kept), request.index
    )
    return kept


@activity.defn
async def delete_index_docs(params: DeleteIndexParams) -> DeleteIndexResult:
    """Delete matching documents from a single index via async delete_by_query.

    Dry-run path
    ------------
    When ``params.dry_run`` is ``True`` the activity returns immediately with
    ``deleted=0`` and ``took_ms=0`` — no ES mutation is performed.

    Async submission
    ----------------
    ``_delete_by_query`` is submitted with ``wait_for_completion=false`` to
    avoid HTTP timeouts on large indices.  ``conflicts=proceed`` lets
    concurrent indexing into the same index continue uninterrupted.
    ``slices=auto`` distributes work across shards without manual tuning.
    ``requests_per_second`` throttles the underlying scroll to keep the
    cluster healthy during the operation.

    Task recovery
    -------------
    The ES task id is heartbeated immediately after submission and on every
    poll iteration.  If the activity worker crashes and Temporal retries the
    activity, the new attempt reads the task id from
    ``activity.info().heartbeat_details`` and resumes polling the existing
    task.  If the task can no longer be found (ES node restart, task aged
    out), the activity resubmits a fresh DBQ — safe because re-running DBQ
    on an already-scrubbed index simply matches zero documents.
    """
    if params.dry_run:
        logger.info("[dry-run] would delete_by_query on index=%s", params.index)
        return DeleteIndexResult(index=params.index, deleted=0, took_ms=0)

    try:
        es = _build_es_client()
    except ApplicationError:
        raise

    # Attempt to recover a task id from a previous (crashed) attempt.
    details = activity.info().heartbeat_details
    task_id: str | None = details[0] if details else None

    if task_id is not None:
        # Verify the task still exists before committing to polling it.
        try:
            initial_resp = es.tasks.get(task_id=task_id)
            if initial_resp.get("completed"):
                # Task already finished in a prior attempt — return its result.
                r = initial_resp["response"]
                logger.info(
                    "DBQ task %s on %s was already complete: deleted=%d took=%dms",
                    task_id,
                    params.index,
                    r["deleted"],
                    r["took"],
                )
                return DeleteIndexResult(
                    index=params.index, deleted=r["deleted"], took_ms=r["took"]
                )
            logger.info("resuming existing DBQ task %s on %s", task_id, params.index)
        except Exception:
            # Task not found — fall through to submit a fresh DBQ.
            logger.warning(
                "DBQ task %s no longer found; resubmitting for %s",
                task_id,
                params.index,
            )
            task_id = None

    if task_id is None:
        query = _build_delete_query(params)
        try:
            resp = es.delete_by_query(
                index=params.index,
                body={"query": query},
                wait_for_completion=False,
                conflicts="proceed",
                requests_per_second=params.requests_per_second,
                slices="auto",
            )
        except AuthenticationException as exc:
            raise ApplicationError(
                f"ES authentication failed: {exc}", non_retryable=True
            ) from exc
        except NotFoundError as exc:
            raise ApplicationError(
                f"Index not found: {params.index} — {exc}", non_retryable=True
            ) from exc
        task_id = resp["task"]
        logger.info("submitted DBQ task %s on %s", task_id, params.index)

    # Heartbeat the task id immediately so it survives a worker restart.
    activity.heartbeat(task_id)

    # Poll until the task reports completion.
    while True:
        await asyncio.sleep(TASK_POLL_INTERVAL_SECONDS)
        task_resp = es.tasks.get(task_id=task_id)
        activity.heartbeat(task_id)

        if task_resp["completed"]:
            r = task_resp["response"]
            deleted: int = r["deleted"]
            took_ms: int = r["took"]
            logger.info(
                "DBQ task %s on %s complete: deleted=%d took=%dms",
                task_id,
                params.index,
                deleted,
                took_ms,
            )
            return DeleteIndexResult(
                index=params.index, deleted=deleted, took_ms=took_ms
            )
