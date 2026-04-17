"""Environment-driven configuration for maintenance activities."""

from __future__ import annotations

import os
from pathlib import Path

from temporalio.exceptions import ApplicationError


CLUSTERS_YAML_ENV = "CLUSTERS_YAML"
CLUSTERS_YAML_DEFAULT = "clusters.yaml"

PLAYBOOK_REPO_ENV = "MAINTENANCE_PLAYBOOK_REPO_PATH"


def clusters_yaml_path() -> Path:
    """Return the path to ``clusters.yaml``.

    Reads ``CLUSTERS_YAML``; defaults to ``./clusters.yaml`` relative to the
    worker's working directory.
    """
    return Path(os.environ.get(CLUSTERS_YAML_ENV, CLUSTERS_YAML_DEFAULT))


def playbook_repo_path() -> Path:
    """Return the on-disk location of the external Ansible playbook repo.

    Reads ``MAINTENANCE_PLAYBOOK_REPO_PATH``.  No default — we refuse to run
    playbooks from an implicit location, because picking the wrong repo can
    mean running the wrong playbook on the wrong host.

    Raises ``ApplicationError(non_retryable=True)`` when the variable is
    unset or the directory does not exist — neither is fixable by retrying.
    """
    raw = os.environ.get(PLAYBOOK_REPO_ENV)
    if not raw:
        raise ApplicationError(
            f"{PLAYBOOK_REPO_ENV} must be set to the checked-out playbook repo",
            non_retryable=True,
        )
    path = Path(raw)
    if not path.is_dir():
        raise ApplicationError(
            f"{PLAYBOOK_REPO_ENV}={raw!r} is not a directory",
            non_retryable=True,
        )
    return path
