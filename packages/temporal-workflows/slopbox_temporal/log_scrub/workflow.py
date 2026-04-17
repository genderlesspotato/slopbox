"""Temporal workflow for log scrubbing.

Overview
--------
1. Validate the scrub request (time range ≤ 366 days) and count matching docs
   so the operator knows the scope before any deletions are issued.
2. Resolve the index pattern to concrete backing indices, excluding the active
   write index and any indices created after the scrub window ends.
3. For each index (oldest first), submit an async delete_by_query, then poll
   the ES task to completion — heartbeating the task id on every iteration so
   a worker restart can resume the same task rather than re-submitting.
4. Return a per-index summary with total docs deleted.

Reliability properties
----------------------
* **No HTTP timeouts**: ``_delete_by_query`` is submitted with
  ``wait_for_completion=false``; the activity polls ``_tasks/<id>`` and
  heartbeats the task id on each iteration.  Temporal detects stalled workers
  via ``heartbeat_timeout`` and retries the activity on a healthy worker, which
  re-attaches to the existing ES task.

* **ES node restart recovery**: if the ES node that owned the task is
  restarted, ``_tasks/<id>`` returns 404.  The activity detects this, resets
  its task id, and resubmits a fresh DBQ.  Re-running DBQ on an already-scrubbed
  index is safe — the query matches zero remaining documents.

* **Serial per-index processing**: indices are processed one at a time to keep
  the cluster load predictable.  ``requests_per_second`` provides additional
  throttling at the ES layer.

* **Safe by default**: ``dry_run=True`` in the request skips all mutations;
  the validation and index-resolution steps still run so the operator can
  review the scope before committing.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from .activities import (
        delete_index_docs,
        resolve_backing_indices,
        validate_scrub_request,
    )

from slopbox_temporal._shared.retry import DEFAULT_RETRY as _DEFAULT_RETRY

from .models import (
    DeleteIndexParams,
    DeleteIndexResult,
    LogScrubRequest,
    LogScrubResult,
)

logger = logging.getLogger("log_scrub")

# ---------------------------------------------------------------------------
# Retry policies
# ---------------------------------------------------------------------------

# Long-running delete activity: more attempts since each retry re-attaches to
# the existing ES task via heartbeat recovery rather than starting from scratch.
_DELETE_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=10),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=5,
)


@workflow.defn
class LogScrubWorkflow:
    @workflow.run
    async def run(self, request: LogScrubRequest) -> LogScrubResult:
        await workflow.execute_activity(
            validate_scrub_request,
            request,
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=_DEFAULT_RETRY,
        )

        indices: list[str] = await workflow.execute_activity(
            resolve_backing_indices,
            request,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=_DEFAULT_RETRY,
        )

        results: list[DeleteIndexResult] = []
        for index in indices:
            result: DeleteIndexResult = await workflow.execute_activity(
                delete_index_docs,
                DeleteIndexParams(
                    index=index,
                    query=request.query,
                    time_range=request.time_range,
                    requests_per_second=request.requests_per_second,
                    dry_run=request.dry_run,
                ),
                # Generous timeout: a massive index can take hours to scrub.
                start_to_close_timeout=timedelta(hours=6),
                # Tight heartbeat window: detect a dead worker within 60 s and
                # retry on a healthy one, which re-attaches to the ES task.
                heartbeat_timeout=timedelta(seconds=60),
                retry_policy=_DELETE_RETRY,
            )
            results.append(result)

        return LogScrubResult(
            indices_scrubbed=len(results),
            total_deleted=sum(r.deleted for r in results),
            dry_run=request.dry_run,
            per_index=results,
        )
