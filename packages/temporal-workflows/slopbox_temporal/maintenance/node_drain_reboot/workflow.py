"""Workflow: drain a single Kubernetes node, reboot it, and uncordon it.

Sequence::

    resolve_target → cordon → drain → reboot → wait_ready → uncordon

Reliability properties
----------------------
* **Step-level retries.** Each step is a separate activity with the shared
  ``DEFAULT_RETRY`` policy so transient Ansible failures (SSH connection
  reset, briefly unreachable API server) retry automatically.
* **Safe-by-default dry-run.** ``request.dry_run`` (default ``True``)
  propagates into Ansible's ``--check`` mode for every step, so a misfired
  trigger exercises all the plumbing without changing cluster state.
* **Best-effort uncordon on failure.** If any step past ``cordon`` raises,
  the workflow attempts an ``uncordon`` via ``asyncio.shield`` before
  re-raising — so a partially-failed run doesn't leave the node locked
  unschedulable.  The attempt is allowed one retry and its result is not
  part of ``NodeDrainRebootResult``; it's purely a cleanup.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from .activities import (
        cordon_node,
        drain_node,
        reboot_node,
        resolve_target_activity,
        uncordon_node,
        wait_node_ready,
    )

from slopbox_temporal._shared.retry import DEFAULT_RETRY as _DEFAULT_RETRY

from .models import (
    NodeDrainRebootRequest,
    NodeDrainRebootResult,
    ResolveTargetParams,
    StepParams,
    StepResult,
)

logger = logging.getLogger("node_drain_reboot")


# Generous upper bound: drain can stall on PodDisruptionBudgets, reboot
# waits for SSH to come back, wait_ready waits for the node to rejoin the
# API server.  The per-step heartbeat floor (15 s) keeps the activity alive
# throughout; this timeout only fires if the playbook hangs indefinitely.
_STEP_TIMEOUT = timedelta(hours=1)
_STEP_HEARTBEAT = timedelta(seconds=60)


@workflow.defn
class NodeDrainRebootWorkflow:
    @workflow.run
    async def run(self, request: NodeDrainRebootRequest) -> NodeDrainRebootResult:
        target = await workflow.execute_activity(
            resolve_target_activity,
            ResolveTargetParams(
                cluster_name=request.cluster_name,
                node_name=request.node_name,
                ansible_host=request.ansible_host,
                ansible_user=request.ansible_user,
            ),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=_DEFAULT_RETRY,
        )

        step_params = StepParams(
            target=target,
            dry_run=request.dry_run,
            reboot_timeout_seconds=request.reboot_timeout_seconds,
        )

        result = NodeDrainRebootResult(target=target, dry_run=request.dry_run)

        cordon_result: StepResult = await workflow.execute_activity(
            cordon_node,
            step_params,
            start_to_close_timeout=_STEP_TIMEOUT,
            heartbeat_timeout=_STEP_HEARTBEAT,
            retry_policy=_DEFAULT_RETRY,
        )
        result.steps.append(cordon_result)

        try:
            for step_fn in (drain_node, reboot_node, wait_node_ready, uncordon_node):
                step_result: StepResult = await workflow.execute_activity(
                    step_fn,
                    step_params,
                    start_to_close_timeout=_STEP_TIMEOUT,
                    heartbeat_timeout=_STEP_HEARTBEAT,
                    retry_policy=_DEFAULT_RETRY,
                )
                result.steps.append(step_result)
            return result
        except BaseException:
            # The uncordon step is the happy-path finaliser; if anything in
            # drain/reboot/wait_ready fails we still try to leave the node
            # schedulable so it doesn't drop out of the fleet silently.
            # Guarded by asyncio.shield so cancellation of the workflow
            # doesn't short-circuit the cleanup.
            if not _already_uncordoned(result):
                try:
                    await asyncio.shield(
                        workflow.execute_activity(
                            uncordon_node,
                            step_params,
                            start_to_close_timeout=_STEP_TIMEOUT,
                            heartbeat_timeout=_STEP_HEARTBEAT,
                            retry_policy=RetryPolicy(maximum_attempts=2),
                        )
                    )
                except Exception:
                    # Best-effort cleanup — swallow and re-raise the original.
                    logger.exception("best-effort uncordon after failure also failed")
            raise


def _already_uncordoned(result: NodeDrainRebootResult) -> bool:
    return any(s.step == "uncordon" for s in result.steps)
