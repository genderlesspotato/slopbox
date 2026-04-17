"""Tests for maintenance theme primitives: targets, inventory, config."""

from __future__ import annotations

from pathlib import Path

import pytest
from temporalio.exceptions import ApplicationError

from slopbox_temporal.maintenance._shared.config import (
    PLAYBOOK_REPO_ENV,
    clusters_yaml_path,
    playbook_repo_path,
)
from slopbox_temporal.maintenance._shared.inventory import (
    build_inventory,
    resolve_target,
)
from slopbox_temporal.maintenance._shared.targets import MaintenanceTarget


# ---------------------------------------------------------------------------
# build_inventory
# ---------------------------------------------------------------------------


def test_build_inventory_renders_single_host_group() -> None:
    target = MaintenanceTarget(
        cluster_name="prod-a",
        node_name="node-01",
        ansible_host="10.0.0.5",
        ansible_user="deploy",
        become=False,
    )
    out = build_inventory(target)
    assert "[target]" in out
    assert "node-01 ansible_host=10.0.0.5 ansible_user=deploy" in out
    assert "ansible_become" not in out


def test_build_inventory_emits_become_vars_when_set() -> None:
    target = MaintenanceTarget(
        cluster_name="prod-a",
        node_name="node-01",
        ansible_host="10.0.0.5",
    )
    out = build_inventory(target)
    assert "[target:vars]" in out
    assert "ansible_become=true" in out


# ---------------------------------------------------------------------------
# resolve_target
# ---------------------------------------------------------------------------


_CLUSTERS_YAML = """\
kubernetes:
  - name: prod-a
    context: prod-a
    environment: prod
  - name: staging-a
    context: staging-a
    environment: staging
"""


def _write_clusters(tmp_path: Path) -> Path:
    path = tmp_path / "clusters.yaml"
    path.write_text(_CLUSTERS_YAML)
    return path


def test_resolve_target_happy_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("CLUSTERS_YAML", str(_write_clusters(tmp_path)))
    target = resolve_target(
        cluster_name="prod-a",
        node_name="prod-a-node-01",
        ansible_host="10.0.0.5",
    )
    assert target == MaintenanceTarget(
        cluster_name="prod-a",
        node_name="prod-a-node-01",
        ansible_host="10.0.0.5",
        ansible_user="root",
        become=True,
    )


def test_resolve_target_unknown_cluster_is_non_retryable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("CLUSTERS_YAML", str(_write_clusters(tmp_path)))
    with pytest.raises(ApplicationError) as exc_info:
        resolve_target(
            cluster_name="does-not-exist",
            node_name="x",
            ansible_host="1.2.3.4",
        )
    assert exc_info.value.non_retryable is True
    # Known clusters are listed so the operator can see the typo at a glance.
    assert "prod-a" in str(exc_info.value)
    assert "staging-a" in str(exc_info.value)


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------


def test_clusters_yaml_path_defaults_to_cwd_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CLUSTERS_YAML", raising=False)
    assert clusters_yaml_path() == Path("clusters.yaml")


def test_clusters_yaml_path_honours_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    override = tmp_path / "custom.yaml"
    monkeypatch.setenv("CLUSTERS_YAML", str(override))
    assert clusters_yaml_path() == override


def test_playbook_repo_path_unset_is_non_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(PLAYBOOK_REPO_ENV, raising=False)
    with pytest.raises(ApplicationError) as exc_info:
        playbook_repo_path()
    assert exc_info.value.non_retryable is True


def test_playbook_repo_path_missing_directory_is_non_retryable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(PLAYBOOK_REPO_ENV, str(tmp_path / "nope"))
    with pytest.raises(ApplicationError) as exc_info:
        playbook_repo_path()
    assert exc_info.value.non_retryable is True


def test_playbook_repo_path_valid_directory_returned(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(PLAYBOOK_REPO_ENV, str(tmp_path))
    assert playbook_repo_path() == tmp_path
