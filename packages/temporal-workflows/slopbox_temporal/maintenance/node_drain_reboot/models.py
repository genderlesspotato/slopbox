"""Dataclass models for the node drain + reboot workflow."""

from __future__ import annotations

from dataclasses import dataclass, field

from slopbox_temporal.maintenance._shared.targets import MaintenanceTarget


# ---------------------------------------------------------------------------
# Request / response
# ---------------------------------------------------------------------------


@dataclass
class NodeDrainRebootRequest:
    cluster_name: str      # must exist in clusters.yaml under `kubernetes`
    node_name: str         # kubernetes node name
    ansible_host: str      # IP or DNS Ansible should SSH to (v1 — k8s lookup is a follow-up)
    ansible_user: str = "root"
    reboot_timeout_seconds: int = 600
    dry_run: bool = True   # safe by default; set False to actually drain/reboot


@dataclass
class StepResult:
    step: str              # "cordon" / "drain" / "reboot" / "wait_ready" / "uncordon"
    rc: int
    status: str            # ansible-runner status — successful / failed / ...
    duration_seconds: float
    artifact_dir: str      # private_data_dir for forensic inspection
    failed_tasks: list[str] = field(default_factory=list)


@dataclass
class NodeDrainRebootResult:
    target: MaintenanceTarget
    dry_run: bool
    steps: list[StepResult] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Activity parameter models
# ---------------------------------------------------------------------------


@dataclass
class ResolveTargetParams:
    cluster_name: str
    node_name: str
    ansible_host: str
    ansible_user: str


@dataclass
class StepParams:
    target: MaintenanceTarget
    dry_run: bool
    reboot_timeout_seconds: int
