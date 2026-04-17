"""Serialisable target dataclass for maintenance workflows.

A ``MaintenanceTarget`` identifies exactly one node and carries the minimal
information Ansible needs to reach it.  It is passed across activity
boundaries, so it stays a plain dataclass — Temporal handles the JSON
round-trip without Pydantic.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MaintenanceTarget:
    cluster_name: str      # must match an entry in clusters.yaml
    node_name: str         # kubernetes node name (display + inventory alias)
    ansible_host: str      # IP or DNS name Ansible should SSH to
    ansible_user: str = "root"
    become: bool = True    # run tasks with privilege escalation
