"""Unit tests for slopbox.k8s_client.build_client() and build_api_bundle()."""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

import pytest

from slopbox.k8s_client import KubernetesApiBundle, build_api_bundle, build_client
from slopbox_domain.k8s.cluster import KubernetesClusterConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _config(name: str = "prod", context: str = "prod-context") -> KubernetesClusterConfig:
    return KubernetesClusterConfig(
        name=name,
        context=context,
        environment="prod",
    )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

@patch("slopbox.k8s_client.kubernetes.config.new_client_from_config")
@patch("slopbox.k8s_client.kubernetes.client.CoreV1Api")
def test_build_client_returns_core_v1_api(mock_core_v1, mock_new_client):
    config = _config()
    mock_api_client = MagicMock()
    mock_new_client.return_value = mock_api_client

    result = build_client(config)

    mock_new_client.assert_called_once_with(context="prod-context")
    mock_core_v1.assert_called_once_with(api_client=mock_api_client)
    assert result is mock_core_v1.return_value


# ---------------------------------------------------------------------------
# ConfigException → sys.exit(1)
# ---------------------------------------------------------------------------

@patch("slopbox.k8s_client.kubernetes.config.new_client_from_config")
def test_build_client_exits_on_config_exception(mock_new_client, caplog):
    from kubernetes.config import ConfigException

    mock_new_client.side_effect = ConfigException("context 'missing' not found")
    config = _config(name="staging", context="missing")

    with caplog.at_level(logging.ERROR, logger="slopbox.k8s_client"):
        with pytest.raises(SystemExit) as exc_info:
            build_client(config)

    assert exc_info.value.code == 1
    assert "staging" in caplog.text
    assert "missing" in caplog.text


# ---------------------------------------------------------------------------
# Parallel safety
# ---------------------------------------------------------------------------

@patch("slopbox.k8s_client.kubernetes.client.CoreV1Api")
@patch("slopbox.k8s_client.kubernetes.config.new_client_from_config")
def test_build_client_parallel_no_cross_contamination(mock_new_client, mock_core_v1):
    """10 threads each call build_client with a distinct context; every call
    must receive the context from its own config, not another thread's."""
    n = 10
    received_contexts: list[str | None] = [None] * n
    lock = threading.Lock()

    def side_effect(*, context: str) -> MagicMock:
        # Record which context this call got, keyed by the context value itself.
        # Each thread's config has context "ctx-<i>", so we can verify later.
        return MagicMock()

    call_log: list[str] = []

    def recording_side_effect(*, context: str) -> MagicMock:
        with lock:
            call_log.append(context)
        return MagicMock()

    mock_new_client.side_effect = recording_side_effect

    configs = [
        KubernetesClusterConfig(name=f"cluster-{i}", context=f"ctx-{i}", environment="test")
        for i in range(n)
    ]

    results = []
    with ThreadPoolExecutor(max_workers=n) as pool:
        futures = [pool.submit(build_client, cfg) for cfg in configs]
        for f in futures:
            results.append(f.result())

    # Every distinct context must appear exactly once
    assert sorted(call_log) == sorted(f"ctx-{i}" for i in range(n))
    # Each call's context must be one of our known contexts (no None / corruption)
    for ctx in call_log:
        assert ctx.startswith("ctx-")


# ---------------------------------------------------------------------------
# build_api_bundle — happy path
# ---------------------------------------------------------------------------

@patch("slopbox.k8s_client.kubernetes.config.new_client_from_config")
@patch("slopbox.k8s_client.kubernetes.client.VersionApi")
@patch("slopbox.k8s_client.kubernetes.client.CustomObjectsApi")
@patch("slopbox.k8s_client.kubernetes.client.AppsV1Api")
@patch("slopbox.k8s_client.kubernetes.client.CoreV1Api")
def test_build_api_bundle_returns_all_four_apis(
    mock_core, mock_apps, mock_custom, mock_version, mock_new_client
):
    config = _config()
    mock_api_client = MagicMock()
    mock_new_client.return_value = mock_api_client

    bundle = build_api_bundle(config)

    mock_new_client.assert_called_once_with(context="prod-context")
    # All four API classes constructed with the same api_client
    mock_core.assert_called_once_with(api_client=mock_api_client)
    mock_apps.assert_called_once_with(api_client=mock_api_client)
    mock_custom.assert_called_once_with(api_client=mock_api_client)
    mock_version.assert_called_once_with(api_client=mock_api_client)

    assert isinstance(bundle, KubernetesApiBundle)
    assert bundle.core is mock_core.return_value
    assert bundle.apps is mock_apps.return_value
    assert bundle.custom is mock_custom.return_value
    assert bundle.version is mock_version.return_value


@patch("slopbox.k8s_client.kubernetes.config.new_client_from_config")
@patch("slopbox.k8s_client.kubernetes.client.VersionApi")
@patch("slopbox.k8s_client.kubernetes.client.CustomObjectsApi")
@patch("slopbox.k8s_client.kubernetes.client.AppsV1Api")
@patch("slopbox.k8s_client.kubernetes.client.CoreV1Api")
def test_build_api_bundle_new_client_called_once(
    mock_core, mock_apps, mock_custom, mock_version, mock_new_client
):
    """new_client_from_config must be called exactly once — one connection per cluster."""
    build_api_bundle(_config())
    mock_new_client.assert_called_once()


# ---------------------------------------------------------------------------
# build_api_bundle — ConfigException → sys.exit(1)
# ---------------------------------------------------------------------------

@patch("slopbox.k8s_client.kubernetes.config.new_client_from_config")
def test_build_api_bundle_exits_on_config_exception(mock_new_client, caplog):
    from kubernetes.config import ConfigException

    mock_new_client.side_effect = ConfigException("context 'missing' not found")
    config = _config(name="staging", context="missing")

    with caplog.at_level(logging.ERROR, logger="slopbox.k8s_client"):
        with pytest.raises(SystemExit) as exc_info:
            build_api_bundle(config)

    assert exc_info.value.code == 1
    assert "staging" in caplog.text
    assert "missing" in caplog.text
