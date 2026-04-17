"""Maintenance-theme Temporal workflows.

Workflows in this package automate maintenance operations on cluster nodes
and hosts — drain-and-reboot, rke2 upgrades, OS upgrades, firmware upgrades.
All actions execute via Ansible playbooks invoked through ``ansible-runner``.

Subpackages:

* ``_shared``       — theme primitives: ``MaintenanceTarget`` dataclass,
                      one-host inventory builder, playbook-repo config,
                      the ``run_playbook`` helper used by every activity.
* ``node_drain_reboot`` — single-node drain-and-reboot workflow (first
                          concrete maintenance workflow).

To add a new maintenance workflow: create a sibling subpackage with the
standard ``workflow.py`` / ``activities.py`` / ``models.py`` layout, import
``_shared.ansible.run_playbook`` from each activity, and register the
workflow + activities in ``slopbox_temporal.workers.maintenance``.
"""
