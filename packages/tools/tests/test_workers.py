"""Tests for worker registration — catch drift when adding a workflow.

Each worker module declares ``WORKFLOWS`` and ``ACTIVITIES`` lists used by
``Worker(...)``.  These tests assert the declared sets match the per-theme
workflow inventory — so forgetting to wire a new workflow into a worker
fails in CI rather than silently at runtime.
"""

from __future__ import annotations

from slopbox_temporal.workers import log_ops, maintenance


def test_log_ops_worker_registers_all_log_ops_workflows() -> None:
    from slopbox_temporal.kibana_export.workflow import KibanaLogExportWorkflow
    from slopbox_temporal.log_scrub.workflow import LogScrubWorkflow

    assert log_ops.TASK_QUEUE == "log-ops"
    assert set(log_ops.WORKFLOWS) == {KibanaLogExportWorkflow, LogScrubWorkflow}


def test_log_ops_worker_registers_all_log_ops_activities() -> None:
    activity_names = {fn.__name__ for fn in log_ops.ACTIVITIES}
    # kibana_export
    assert {
        "validate_export_request",
        "resolve_indices",
        "open_pit",
        "export_chunk",
        "close_pit",
        "cleanup_partial_export",
        "write_manifest",
    }.issubset(activity_names)
    # log_scrub
    assert {
        "validate_scrub_request",
        "resolve_backing_indices",
        "delete_index_docs",
    }.issubset(activity_names)


def test_maintenance_worker_registers_all_maintenance_workflows() -> None:
    from slopbox_temporal.maintenance.node_drain_reboot.workflow import (
        NodeDrainRebootWorkflow,
    )

    assert maintenance.TASK_QUEUE == "maintenance"
    assert set(maintenance.WORKFLOWS) == {NodeDrainRebootWorkflow}


def test_maintenance_worker_registers_all_maintenance_activities() -> None:
    activity_names = {fn.__name__ for fn in maintenance.ACTIVITIES}
    assert activity_names == {
        "resolve_target_activity",
        "cordon_node",
        "drain_node",
        "reboot_node",
        "wait_node_ready",
        "uncordon_node",
    }


def test_worker_task_queues_do_not_collide() -> None:
    assert log_ops.TASK_QUEUE != maintenance.TASK_QUEUE
