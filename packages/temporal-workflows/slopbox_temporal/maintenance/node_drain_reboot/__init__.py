"""Drain-and-reboot a single Kubernetes node via Ansible playbooks.

The first concrete maintenance workflow; doubles as the template for future
maintenance workflows (rke2 upgrade, OS patch apply, firmware).  Each step
is a separate activity wrapping a single playbook invocation, so retries
operate at step granularity and the event stream per step is independently
inspectable.
"""
