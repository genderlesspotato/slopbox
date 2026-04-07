"""Tests for VaultBackend, SecretsBackend Protocol, and build_client_for()."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from slopbox_domain.es.cluster import ElasticsearchClusterConfig
from slopbox.client import build_client_for
from slopbox.vault import SecretsBackend, VaultBackend


# ---------------------------------------------------------------------------
# In-test secrets stub
# ---------------------------------------------------------------------------

class _DictSecretsBackend:
    """Simple SecretsBackend implementation backed by an in-memory dict."""

    def __init__(self, store: dict[str, dict[str, str]]) -> None:
        self._store = store

    def get_kv(self, path: str) -> dict[str, str]:
        if path not in self._store:
            raise KeyError(f"no secret at path '{path}'")
        return self._store[path]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_ES_HOST = "https://prod-metrics.example.com:9200"
_VAULT_PATH = "slopbox/es/prod-metrics"

_CLUSTER = ElasticsearchClusterConfig(
    name="prod-metrics",
    environment="prod",
    workload="metrics",
    host=_ES_HOST,
    vault_path=_VAULT_PATH,
)

_CLUSTER_CLOUD = ElasticsearchClusterConfig(
    name="prod-logs",
    environment="prod",
    workload="logs",
    cloud_id="prod-logs:dXMtZWFzdC0x",
    vault_path="slopbox/es/prod-logs",
)


# ---------------------------------------------------------------------------
# SecretsBackend Protocol
# ---------------------------------------------------------------------------

class TestSecretsBackendProtocol:
    def test_dict_stub_satisfies_protocol(self) -> None:
        backend: SecretsBackend = _DictSecretsBackend({})
        assert isinstance(backend, SecretsBackend)

    def test_vault_backend_satisfies_protocol(self) -> None:
        # VaultBackend must structurally satisfy SecretsBackend.
        # We patch VAULT_ADDR so __init__ does not raise.
        with patch.dict(os.environ, {"VAULT_ADDR": "https://vault.example.com"}):
            backend: SecretsBackend = VaultBackend()
        assert isinstance(backend, SecretsBackend)


# ---------------------------------------------------------------------------
# build_client_for() — using dict stub (no Vault, no ES)
# ---------------------------------------------------------------------------

class TestBuildClientFor:
    def _make_backend(self, **secret_kwargs: str) -> _DictSecretsBackend:
        return _DictSecretsBackend({_VAULT_PATH: secret_kwargs})

    @patch("slopbox.client.Elasticsearch")
    def test_uses_api_key(self, mock_es: MagicMock) -> None:
        backend = self._make_backend(api_key="id:key==")
        build_client_for(_CLUSTER, backend)
        mock_es.assert_called_once_with(hosts=[_ES_HOST], api_key="id:key==")

    @patch("slopbox.client.Elasticsearch")
    def test_uses_basic_auth(self, mock_es: MagicMock) -> None:
        backend = self._make_backend(username="elastic", password="hunter2")
        build_client_for(_CLUSTER, backend)
        mock_es.assert_called_once_with(
            hosts=[_ES_HOST], basic_auth=("elastic", "hunter2")
        )

    @patch("slopbox.client.Elasticsearch")
    def test_api_key_takes_precedence_over_basic_auth(self, mock_es: MagicMock) -> None:
        backend = self._make_backend(
            api_key="id:key==", username="elastic", password="hunter2"
        )
        build_client_for(_CLUSTER, backend)
        mock_es.assert_called_once_with(hosts=[_ES_HOST], api_key="id:key==")

    @patch("slopbox.client.Elasticsearch")
    def test_uses_cloud_id(self, mock_es: MagicMock) -> None:
        backend = _DictSecretsBackend({"slopbox/es/prod-logs": {"api_key": "id:key=="}})
        build_client_for(_CLUSTER_CLOUD, backend)
        mock_es.assert_called_once_with(
            cloud_id="prod-logs:dXMtZWFzdC0x", api_key="id:key=="
        )

    def test_raises_on_missing_credentials(self) -> None:
        backend = self._make_backend()   # empty secret
        with pytest.raises(ValueError, match="must contain 'api_key'"):
            build_client_for(_CLUSTER, backend)

    def test_raises_on_partial_basic_auth_username_only(self) -> None:
        backend = self._make_backend(username="elastic")   # no password
        with pytest.raises(ValueError, match="must contain 'api_key'"):
            build_client_for(_CLUSTER, backend)


# ---------------------------------------------------------------------------
# VaultBackend — auth path selection
# ---------------------------------------------------------------------------

class TestVaultBackendAuth:
    _VAULT_URL = "https://vault.example.com"

    @patch("slopbox.vault.hvac.Client")
    def test_token_auth_from_env(self, mock_client_cls: MagicMock) -> None:
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client.secrets.kv.v2.read_secret.return_value = {
            "data": {"data": {"api_key": "id:key=="}}
        }
        mock_client_cls.return_value = mock_client

        with patch.dict(
            os.environ,
            {"VAULT_ADDR": self._VAULT_URL, "VAULT_TOKEN": "s.test-token"},
            clear=False,
        ):
            backend = VaultBackend()
            result = backend.get_kv("slopbox/es/prod-metrics")

        # Token must be set; k8s auth must NOT be called.
        assert mock_client.token == "s.test-token"
        mock_client.auth.kubernetes.login.assert_not_called()
        assert result == {"api_key": "id:key=="}

    @patch("slopbox.vault.hvac.Client")
    def test_k8s_sa_auth_when_no_token(
        self, mock_client_cls: MagicMock, tmp_path: Path
    ) -> None:
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client.secrets.kv.v2.read_secret.return_value = {
            "data": {"data": {"api_key": "id:key=="}}
        }
        mock_client_cls.return_value = mock_client

        sa_jwt = tmp_path / "token"
        sa_jwt.write_text("fake.sa.jwt")

        env = {"VAULT_ADDR": self._VAULT_URL}
        env.pop("VAULT_TOKEN", None)   # ensure not set

        with patch.dict(os.environ, env, clear=False), \
             patch.dict(os.environ, {}, clear=False), \
             patch("slopbox.vault.os.getenv", side_effect=_env_without_token), \
             patch("slopbox.vault._K8S_SA_TOKEN_PATH", sa_jwt):
            backend = VaultBackend(vault_url=self._VAULT_URL)
            backend.get_kv("slopbox/es/prod-metrics")

        mock_client.auth.kubernetes.login.assert_called_once_with(
            role="slopbox", jwt="fake.sa.jwt"
        )

    @patch("slopbox.vault.hvac.Client")
    def test_raises_when_no_auth_available(
        self, mock_client_cls: MagicMock, tmp_path: Path
    ) -> None:
        mock_client_cls.return_value = MagicMock()

        # No VAULT_TOKEN, SA token path points to non-existent file.
        missing_sa = tmp_path / "no_token_here"

        with patch("slopbox.vault.os.getenv", side_effect=_env_without_token), \
             patch("slopbox.vault._K8S_SA_TOKEN_PATH", missing_sa):
            backend = VaultBackend(vault_url=self._VAULT_URL)
            with pytest.raises(RuntimeError, match="no Vault authentication available"):
                backend.get_kv("slopbox/es/prod-metrics")


# ---------------------------------------------------------------------------
# VaultBackend — KV version handling
# ---------------------------------------------------------------------------

class TestVaultBackendKvVersion:
    _VAULT_URL = "https://vault.example.com"

    @patch("slopbox.vault.hvac.Client")
    def test_kv_v2_extracts_nested_data(self, mock_client_cls: MagicMock) -> None:
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client.secrets.kv.v2.read_secret.return_value = {
            "data": {"data": {"api_key": "id:key=="}, "metadata": {}}
        }
        mock_client_cls.return_value = mock_client

        with patch.dict(
            os.environ,
            {"VAULT_ADDR": self._VAULT_URL, "VAULT_TOKEN": "s.tok"},
            clear=False,
        ):
            backend = VaultBackend(kv_version=2)
            result = backend.get_kv("slopbox/es/prod-metrics")

        mock_client.secrets.kv.v2.read_secret.assert_called_once_with(
            path="slopbox/es/prod-metrics", mount_point="secret"
        )
        assert result == {"api_key": "id:key=="}

    @patch("slopbox.vault.hvac.Client")
    def test_kv_v1_extracts_flat_data(self, mock_client_cls: MagicMock) -> None:
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client.secrets.kv.v1.read_secret.return_value = {
            "data": {"api_key": "id:key=="}
        }
        mock_client_cls.return_value = mock_client

        with patch.dict(
            os.environ,
            {"VAULT_ADDR": self._VAULT_URL, "VAULT_TOKEN": "s.tok"},
            clear=False,
        ):
            backend = VaultBackend(kv_version=1)
            result = backend.get_kv("slopbox/es/prod-metrics")

        mock_client.secrets.kv.v1.read_secret.assert_called_once_with(
            path="slopbox/es/prod-metrics", mount_point="secret"
        )
        assert result == {"api_key": "id:key=="}

    @patch("slopbox.vault.hvac.Client")
    def test_kv_version_from_env_var(self, mock_client_cls: MagicMock) -> None:
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client.secrets.kv.v1.read_secret.return_value = {
            "data": {"api_key": "id:key=="}
        }
        mock_client_cls.return_value = mock_client

        with patch.dict(
            os.environ,
            {
                "VAULT_ADDR": self._VAULT_URL,
                "VAULT_TOKEN": "s.tok",
                "VAULT_KV_VERSION": "1",
            },
            clear=False,
        ):
            backend = VaultBackend()
            backend.get_kv("slopbox/es/prod-metrics")

        # v1 path should have been called, not v2.
        mock_client.secrets.kv.v1.read_secret.assert_called_once()
        mock_client.secrets.kv.v2.read_secret.assert_not_called()

    @patch("slopbox.vault.hvac.Client")
    def test_custom_kv_mount(self, mock_client_cls: MagicMock) -> None:
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client.secrets.kv.v2.read_secret.return_value = {
            "data": {"data": {"api_key": "id:key=="}}
        }
        mock_client_cls.return_value = mock_client

        with patch.dict(
            os.environ,
            {"VAULT_ADDR": self._VAULT_URL, "VAULT_TOKEN": "s.tok"},
            clear=False,
        ):
            backend = VaultBackend(kv_mount="kvv2")
            backend.get_kv("slopbox/es/prod-metrics")

        mock_client.secrets.kv.v2.read_secret.assert_called_once_with(
            path="slopbox/es/prod-metrics", mount_point="kvv2"
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _env_without_token(key: str, default: Any = None) -> Any:
    """os.getenv substitute that returns None for VAULT_TOKEN."""
    if key == "VAULT_TOKEN":
        return None
    return os.environ.get(key, default)
