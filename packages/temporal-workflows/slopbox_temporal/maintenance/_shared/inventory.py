"""Build one-host Ansible inventories from a ``MaintenanceTarget``.

The workflow generates an inventory per step rather than using a static file,
so playbooks are inventory-agnostic and the workflow owns the "which host"
decision.

``resolve_target`` is the bridge between the workflow request (cluster +
node names as strings) and the serialisable ``MaintenanceTarget`` consumed
by every downstream activity.  It validates the cluster name against
``clusters.yaml``; the node name and ansible_host come from the request
(live k8s-API node lookup is a planned follow-up).
"""

from __future__ import annotations

from temporalio.exceptions import ApplicationError

from slopbox_domain.registry import ClusterRegistry

from .config import clusters_yaml_path
from .targets import MaintenanceTarget


def build_inventory(target: MaintenanceTarget) -> str:
    """Render an INI-format Ansible inventory for a single host.

    Ansible accepts the ``node_name`` as an inventory alias — the actual SSH
    target is set via ``ansible_host``.  This keeps the playbook output
    readable (uses the operator-meaningful node name) regardless of whether
    the host happens to have reverse DNS.
    """
    lines = [
        "[target]",
        (
            f"{target.node_name}"
            f" ansible_host={target.ansible_host}"
            f" ansible_user={target.ansible_user}"
        ),
    ]
    if target.become:
        lines.append("")
        lines.append("[target:vars]")
        lines.append("ansible_become=true")
    return "\n".join(lines) + "\n"


def resolve_target(
    cluster_name: str,
    node_name: str,
    ansible_host: str,
    *,
    ansible_user: str = "root",
    become: bool = True,
) -> MaintenanceTarget:
    """Validate ``cluster_name`` against ``clusters.yaml`` and assemble a target.

    Raises ``ApplicationError(non_retryable=True)`` when the cluster is not
    in the registry — retrying won't create the cluster.
    """
    registry = ClusterRegistry.from_yaml(clusters_yaml_path())
    matches = registry.k8s(name=cluster_name)
    if not matches:
        known = ", ".join(sorted(c.name for c in registry.kubernetes)) or "<none>"
        raise ApplicationError(
            f"unknown kubernetes cluster {cluster_name!r}; known clusters: {known}",
            non_retryable=True,
        )
    return MaintenanceTarget(
        cluster_name=cluster_name,
        node_name=node_name,
        ansible_host=ansible_host,
        ansible_user=ansible_user,
        become=become,
    )
