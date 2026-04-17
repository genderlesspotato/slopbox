"""Theme-shared primitives for maintenance workflows.

Every maintenance workflow reuses four building blocks:

* ``targets.MaintenanceTarget`` — serialisable dataclass identifying a node
  (cluster name, node name, Ansible-reachable host).
* ``inventory.build_inventory`` / ``resolve_target`` — render a one-host
  Ansible inventory, validate cluster identity against ``clusters.yaml``.
* ``config.playbook_repo_path`` — read ``MAINTENANCE_PLAYBOOK_REPO_PATH``
  and return the on-disk location of the external playbook repo.
* ``ansible.run_playbook`` — ``ansible-runner``-backed helper that runs a
  playbook, heartbeats per event, honours activity cancellation, and maps
  terminal statuses to Temporal exceptions.
"""
