"""Temporal activities for the node drain + reboot workflow.

Each activity wraps exactly one Ansible playbook call via
:func:`slopbox_temporal.maintenance._shared.ansible.run_playbook`.  The
playbooks themselves live in the external repo referenced by
``MAINTENANCE_PLAYBOOK_REPO_PATH`` (see ``_shared/config.py``).

Playbook contract
-----------------
The workflow expects these playbooks to exist at the root of the repo:

* ``cordon_node.yml``      — mark node unschedulable
* ``drain_node.yml``       — evict pods, respecting PDBs
* ``reboot_node.yml``      — reboot + wait for SSH to return
* ``wait_node_ready.yml``  — block until ``kubectl get node`` reports Ready
* ``uncordon_node.yml``    — mark node schedulable again

All playbooks receive ``node_name`` as an extra-var; ``reboot_node.yml``
also receives ``reboot_timeout_seconds``.
"""

from __future__ import annotations

import logging

from temporalio import activity

from slopbox_temporal.maintenance._shared.ansible import AnsibleResult, run_playbook
from slopbox_temporal.maintenance._shared.inventory import build_inventory, resolve_target
from slopbox_temporal.maintenance._shared.targets import MaintenanceTarget

from .models import ResolveTargetParams, StepParams, StepResult

logger = logging.getLogger("node_drain_reboot")

# Playbook filename for each step — kept next to the activities that invoke
# them so the mapping is obvious from the workflow file.
PLAYBOOK_CORDON = "cordon_node.yml"
PLAYBOOK_DRAIN = "drain_node.yml"
PLAYBOOK_REBOOT = "reboot_node.yml"
PLAYBOOK_WAIT_READY = "wait_node_ready.yml"
PLAYBOOK_UNCORDON = "uncordon_node.yml"


# ---------------------------------------------------------------------------
# Target resolution
# ---------------------------------------------------------------------------


@activity.defn
async def resolve_target_activity(params: ResolveTargetParams) -> MaintenanceTarget:
    """Validate the cluster name against ``clusters.yaml`` and assemble a target."""
    return resolve_target(
        cluster_name=params.cluster_name,
        node_name=params.node_name,
        ansible_host=params.ansible_host,
        ansible_user=params.ansible_user,
    )


# ---------------------------------------------------------------------------
# Steps — one activity per playbook
# ---------------------------------------------------------------------------


async def _run_step(
    *,
    step: str,
    playbook: str,
    params: StepParams,
    extra_vars: dict | None = None,
) -> StepResult:
    inventory = build_inventory(params.target)
    vars_: dict = {"node_name": params.target.node_name}
    if extra_vars:
        vars_.update(extra_vars)
    result: AnsibleResult = await run_playbook(
        playbook=playbook,
        inventory=inventory,
        extra_vars=vars_,
        dry_run=params.dry_run,
    )
    return StepResult(
        step=step,
        rc=result.rc,
        status=result.status,
        duration_seconds=result.duration_seconds,
        artifact_dir=str(result.artifact_dir),
        failed_tasks=result.failed_tasks,
    )


@activity.defn
async def cordon_node(params: StepParams) -> StepResult:
    return await _run_step(step="cordon", playbook=PLAYBOOK_CORDON, params=params)


@activity.defn
async def drain_node(params: StepParams) -> StepResult:
    return await _run_step(step="drain", playbook=PLAYBOOK_DRAIN, params=params)


@activity.defn
async def reboot_node(params: StepParams) -> StepResult:
    return await _run_step(
        step="reboot",
        playbook=PLAYBOOK_REBOOT,
        params=params,
        extra_vars={"reboot_timeout_seconds": params.reboot_timeout_seconds},
    )


@activity.defn
async def wait_node_ready(params: StepParams) -> StepResult:
    return await _run_step(
        step="wait_ready", playbook=PLAYBOOK_WAIT_READY, params=params
    )


@activity.defn
async def uncordon_node(params: StepParams) -> StepResult:
    return await _run_step(
        step="uncordon", playbook=PLAYBOOK_UNCORDON, params=params
    )
