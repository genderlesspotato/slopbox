"""Worker entrypoint for the ``maintenance`` task queue.

Runs on a host that has the rights to drive maintenance actions: an SSH
private key installed for ``ansible_user`` on target nodes, a checked-out
Ansible playbook repo at ``$MAINTENANCE_PLAYBOOK_REPO_PATH``, and a readable
``clusters.yaml``.  Invocation::

    python -m slopbox_temporal.workers.maintenance

Environment variables:

* ``TEMPORAL_ADDRESS``              — Temporal frontend (default ``localhost:7233``)
* ``TEMPORAL_NAMESPACE``            — Temporal namespace (default ``default``)
* ``CLUSTERS_YAML``                 — path to clusters.yaml (default ``./clusters.yaml``)
* ``MAINTENANCE_PLAYBOOK_REPO_PATH`` — required; path to the playbook repo
"""

from __future__ import annotations

import asyncio
import logging
import os

from temporalio.client import Client
from temporalio.worker import Worker

from slopbox_temporal._shared.logging import configure_worker_logging
from slopbox_temporal.maintenance.node_drain_reboot.activities import (
    cordon_node,
    drain_node,
    reboot_node,
    resolve_target_activity,
    uncordon_node,
    wait_node_ready,
)
from slopbox_temporal.maintenance.node_drain_reboot.workflow import (
    NodeDrainRebootWorkflow,
)

logger = logging.getLogger("slopbox_temporal.workers.maintenance")

TASK_QUEUE = "maintenance"

WORKFLOWS = [
    NodeDrainRebootWorkflow,
]

ACTIVITIES = [
    resolve_target_activity,
    cordon_node,
    drain_node,
    reboot_node,
    wait_node_ready,
    uncordon_node,
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
