"""Worker entrypoint for the ``log-ops`` task queue.

Registers the log-operations workflows (Kibana log export, log scrub) and all
of their activities.  Invocation::

    python -m slopbox_temporal.workers.log_ops

Environment variables:

* ``TEMPORAL_ADDRESS``   — Temporal frontend address (default ``localhost:7233``)
* ``TEMPORAL_NAMESPACE`` — Temporal namespace (default ``default``)
* ``ES_HOST`` / ``ES_CLOUD_ID`` / ``ES_API_KEY`` / ``ES_USERNAME`` / ``ES_PASSWORD``
  — consumed by activities at call time (see ``_shared.es_client``).
* AWS credentials (standard boto3 chain) — consumed by S3 activities.
"""

from __future__ import annotations

import asyncio
import logging
import os

from temporalio.client import Client
from temporalio.worker import Worker

from slopbox_temporal._shared.logging import configure_worker_logging
from slopbox_temporal.kibana_export.activities import (
    cleanup_partial_export,
    close_pit,
    export_chunk,
    open_pit,
    resolve_indices,
    validate_export_request,
    write_manifest,
)
from slopbox_temporal.kibana_export.workflow import KibanaLogExportWorkflow
from slopbox_temporal.log_scrub.activities import (
    delete_index_docs,
    resolve_backing_indices,
    validate_scrub_request,
)
from slopbox_temporal.log_scrub.workflow import LogScrubWorkflow

logger = logging.getLogger("slopbox_temporal.workers.log_ops")

TASK_QUEUE = "log-ops"

WORKFLOWS = [
    KibanaLogExportWorkflow,
    LogScrubWorkflow,
]

ACTIVITIES = [
    # kibana_export
    validate_export_request,
    resolve_indices,
    open_pit,
    export_chunk,
    close_pit,
    cleanup_partial_export,
    write_manifest,
    # log_scrub
    validate_scrub_request,
    resolve_backing_indices,
    delete_index_docs,
]


async def run() -> None:
    address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    namespace = os.environ.get("TEMPORAL_NAMESPACE", "default")

    logger.info(
        "connecting to Temporal: address=%s namespace=%s queue=%s",
        address,
        namespace,
        TASK_QUEUE,
    )
    client = await Client.connect(address, namespace=namespace)

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=WORKFLOWS,
        activities=ACTIVITIES,
    )
    logger.info(
        "worker started: workflows=%d activities=%d",
        len(WORKFLOWS),
        len(ACTIVITIES),
    )
    await worker.run()


def main() -> None:
    configure_worker_logging()
    asyncio.run(run())


if __name__ == "__main__":
    main()
