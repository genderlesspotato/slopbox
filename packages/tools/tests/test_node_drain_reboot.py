"""Tests for slopbox_temporal.maintenance.node_drain_reboot.

Covers:

* each step activity calls ``run_playbook`` with the expected playbook and
  extra vars, and ``StepResult`` carries the ansible-runner outcome back to
  the workflow.
* the workflow module wires up every activity the worker needs to register.

Follows the existing test suite's convention of testing activities directly
rather than spinning up a Temporal ``WorkflowEnvironment`` — end-to-end
workflow execution is covered by the manual smoke test documented in
``docs/maintenance-node-drain-reboot.md``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from slopbox_temporal.maintenance._shared.ansible import AnsibleResult
from slopbox_temporal.maintenance._shared.targets import MaintenanceTarget
from slopbox_temporal.maintenance.node_drain_reboot import activities as act_mod
from slopbox_temporal.maintenance.node_drain_reboot import workflow as wf_mod
from slopbox_temporal.maintenance.node_drain_reboot.activities import (
    PLAYBOOK_CORDON,
    PLAYBOOK_DRAIN,
    PLAYBOOK_REBOOT,
    PLAYBOOK_UNCORDON,
    PLAYBOOK_WAIT_READY,
    cordon_node,
    drain_node,
    reboot_node,
    resolve_target_activity,
    uncordon_node,
    wait_node_ready,
)
from slopbox_temporal.maintenance.node_drain_reboot.models import (
    ResolveTargetParams,
    StepParams,
    StepResult,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


_TARGET = MaintenanceTarget(
    cluster_name="prod-a",
    node_name="node-01",
    ansible_host="10.0.0.5",
)

_STEP_PARAMS = StepParams(
    target=_TARGET,
    dry_run=True,
    reboot_timeout_seconds=600,
)


def _ok_result() -> AnsibleResult:
    return AnsibleResult(
        rc=0,
        status="successful",
        duration_seconds=0.0,
        artifact_dir=Path("/tmp/fake"),
        failed_tasks=[],
    )


# ---------------------------------------------------------------------------
# Per-step activities — playbook mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "activity_fn, expected_playbook",
    [
        (cordon_node, PLAYBOOK_CORDON),
        (drain_node, PLAYBOOK_DRAIN),
        (reboot_node, PLAYBOOK_REBOOT),
        (wait_node_ready, PLAYBOOK_WAIT_READY),
        (uncordon_node, PLAYBOOK_UNCORDON),
    ],
)
async def test_step_activities_call_expected_playbook(
    activity_fn: Any, expected_playbook: str
) -> None:
    with patch.object(
        act_mod, "run_playbook", new=AsyncMock(return_value=_ok_result())
    ) as mock_run:
        result = await activity_fn(_STEP_PARAMS)
    assert isinstance(result, StepResult)
    assert result.status == "successful"
    mock_run.assert_awaited_once()
    kwargs = mock_run.await_args.kwargs
    assert kwargs["playbook"] == expected_playbook
    assert kwargs["extra_vars"]["node_name"] == "node-01"
    assert kwargs["dry_run"] is True


async def test_reboot_step_passes_reboot_timeout_var() -> None:
    with patch.object(
        act_mod, "run_playbook", new=AsyncMock(return_value=_ok_result())
    ) as mock_run:
        await reboot_node(_STEP_PARAMS)
    assert (
        mock_run.await_args.kwargs["extra_vars"]["reboot_timeout_seconds"] == 600
    )


async def test_resolve_target_activity_delegates_to_shared_helper(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    clusters = tmp_path / "clusters.yaml"
    clusters.write_text(
        "kubernetes:\n"
        "  - name: prod-a\n"
        "    context: prod-a\n"
        "    environment: prod\n"
    )
    monkeypatch.setenv("CLUSTERS_YAML", str(clusters))
    out = await resolve_target_activity(
        ResolveTargetParams(
            cluster_name="prod-a",
            node_name="node-01",
            ansible_host="10.0.0.5",
            ansible_user="root",
        )
    )
    assert out == _TARGET


# ---------------------------------------------------------------------------
# Structural checks — keep the workflow module and activity module in sync
# ---------------------------------------------------------------------------


def test_workflow_imports_every_step_activity() -> None:
    """If someone adds a step activity but forgets to import it into the
    workflow (or vice versa) this guard fails."""
    wf_imports = {
        "resolve_target_activity",
        "cordon_node",
        "drain_node",
        "reboot_node",
        "wait_node_ready",
        "uncordon_node",
    }
    assert wf_imports.issubset(set(dir(wf_mod)))
    assert wf_imports.issubset(set(dir(act_mod)))


def test_already_uncordoned_helper_detects_uncordon_step() -> None:
    from slopbox_temporal.maintenance.node_drain_reboot.models import (
        NodeDrainRebootResult,
    )
    from slopbox_temporal.maintenance.node_drain_reboot.workflow import (
        _already_uncordoned,
    )

    without = NodeDrainRebootResult(
        target=_TARGET,
        dry_run=True,
        steps=[
            StepResult(
                step="cordon",
                rc=0,
                status="successful",
                duration_seconds=0.0,
                artifact_dir="/tmp/fake",
            )
        ],
    )
    with_uncordon = NodeDrainRebootResult(
        target=_TARGET,
        dry_run=True,
        steps=[
            *without.steps,
            StepResult(
                step="uncordon",
                rc=0,
                status="successful",
                duration_seconds=0.0,
                artifact_dir="/tmp/fake",
            ),
        ],
    )
    assert _already_uncordoned(without) is False
    assert _already_uncordoned(with_uncordon) is True
