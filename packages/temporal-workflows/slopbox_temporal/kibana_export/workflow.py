"""Temporal workflow for the Kibana log export.

Overview
--------
1. Validate the export request (time range ≤ 14 days, doc count ≤ 250 000).
2. Resolve the index pattern to concrete index names (alphabetical = chronological).
3. For each index, open a PIT then drain it with search_after pagination, writing
   each page as a gzip-compressed NDJSON chunk to S3.
4. Close the PIT after each index (``try/finally``).
5. Write ``manifest.json`` and ``README.txt`` to the S3 prefix.

Failure / cancellation safety
------------------------------
S3 ``put_object`` is atomic — no truncated files are possible.  The risk is
*orphaned* chunk files from a failed run (chunks present, no manifest).

``chunks`` is tracked in workflow state (Temporal checkpoints it).  On any
``BaseException`` (regular error *or* asyncio cancellation) the workflow calls
``cleanup_partial_export`` via ``asyncio.shield`` to delete the written objects
before re-raising.  ``asyncio.shield`` prevents the cleanup activity from
being cancelled while running.

PIT discipline
--------------
At most one PIT is open at a time.  Each index is fully drained before moving
to the next.  The PIT is closed in a ``try/finally`` so it is released even
when ``export_chunk`` raises.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from .activities import (
        cleanup_partial_export,
        close_pit,
        export_chunk,
        open_pit,
        resolve_indices,
        validate_export_request,
        write_manifest,
    )
    from .models import (
        CleanupParams,
        ExportChunkParams,
        ExportChunkResult,
        KibanaLogExportRequest,
        KibanaLogExportResult,
        WriteManifestParams,
    )

logger = logging.getLogger("kibana_export")

# ---------------------------------------------------------------------------
# Retry policy shared by all activities
# ---------------------------------------------------------------------------

_DEFAULT_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=3,
)


@workflow.defn
class KibanaLogExportWorkflow:
    @workflow.run
    async def run(self, request: KibanaLogExportRequest) -> KibanaLogExportResult:
        chunks: list[ExportChunkResult] = []

        try:
            total_docs = await workflow.execute_activity(
                validate_export_request,
                request,
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=_DEFAULT_RETRY,
            )

            indices: list[str] = await workflow.execute_activity(
                resolve_indices,
                args=[request.index, request.time_range],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=_DEFAULT_RETRY,
            )

            chunk_index = 0
            for index in indices:
                pit_id: str = await workflow.execute_activity(
                    open_pit,
                    index,
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=_DEFAULT_RETRY,
                )
                try:
                    search_after = None
                    while True:
                        params = ExportChunkParams(
                            pit_id=pit_id,
                            search_after=search_after,
                            query=request.query,
                            time_range=request.time_range,
                            s3_bucket=request.s3_bucket,
                            s3_prefix=request.s3_prefix,
                            chunk_size=request.chunk_size,
                            chunk_index=chunk_index,
                        )
                        result: ExportChunkResult = await workflow.execute_activity(
                            export_chunk,
                            params,
                            start_to_close_timeout=timedelta(minutes=10),
                            retry_policy=_DEFAULT_RETRY,
                        )
                        # Always use the PIT id from the response for the next call.
                        pit_id = result.new_pit_id
                        if result.s3_key is not None:
                            chunks.append(result)
                        if result.done:
                            break
                        chunk_index += 1
                        search_after = result.search_after
                finally:
                    await workflow.execute_activity(
                        close_pit,
                        pit_id,
                        start_to_close_timeout=timedelta(seconds=30),
                        retry_policy=_DEFAULT_RETRY,
                    )

            manifest_key: str = await workflow.execute_activity(
                write_manifest,
                WriteManifestParams(
                    request=request,
                    resolved_indices=indices,
                    chunks=chunks,
                    total_docs=total_docs,
                ),
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=_DEFAULT_RETRY,
            )

            return KibanaLogExportResult(
                total_docs=total_docs,
                total_chunks=len(chunks),
                s3_prefix=request.s3_prefix,
                manifest_key=manifest_key,
            )

        except BaseException:
            # Clean up any chunk files already written to S3 before propagating.
            written_keys = [c.s3_key for c in chunks if c.s3_key is not None]
            if written_keys:
                await asyncio.shield(
                    workflow.execute_activity(
                        cleanup_partial_export,
                        CleanupParams(
                            s3_bucket=request.s3_bucket,
                            s3_keys=written_keys,
                        ),
                        start_to_close_timeout=timedelta(minutes=5),
                        retry_policy=RetryPolicy(maximum_attempts=3),
                    )
                )
            raise
