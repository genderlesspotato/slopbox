"""Vault-based secrets resolution for slopbox tools.

Provides a ``SecretsBackend`` Protocol and a ``VaultBackend`` implementation
that resolves cluster credentials from HashiCorp Vault KV secrets (v1 and v2).

Auth method detection (in priority order):
1. ``VAULT_TOKEN`` env var — used on developer laptops after ``vault login``
   and in CI pipelines after a Vault OIDC authentication step.
2. Kubernetes Service Account JWT at the well-known path — used by Temporal
   workflow workers running inside a k8s cluster.

Environment variables:

    VAULT_ADDR        required — Vault server URL
    VAULT_TOKEN       token auth (laptop / CI)
    VAULT_ROLE        k8s auth role name (default: slopbox)
    VAULT_KV_MOUNT    KV mount name (default: secret)
    VAULT_KV_VERSION  KV version, 1 or 2 (default: 2)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Protocol, runtime_checkable

import hvac

# Path where k8s injects the pod's Service Account JWT.
_K8S_SA_TOKEN_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------

@runtime_checkable
class SecretsBackend(Protocol):
    """Minimal interface for resolving KV secrets."""

    def get_kv(self, path: str) -> dict[str, str]:
        """Return all key-value pairs stored at the given KV path.

        ``path`` is relative to the KV mount (e.g. ``"slopbox/es/prod-metrics"``).
        The returned dict contains the secret's data fields.
        """
        ...


# ---------------------------------------------------------------------------
# VaultBackend
# ---------------------------------------------------------------------------

class VaultBackend:
    """Resolves KV secrets from HashiCorp Vault using the hvac SDK.

    Args:
        vault_url:  Vault server URL.  Defaults to ``VAULT_ADDR`` env var.
        kv_mount:   KV secrets engine mount name.  Defaults to ``VAULT_KV_MOUNT``
                    env var, or ``"secret"`` if unset.
        kv_version: KV engine version — 1 or 2.  Defaults to ``VAULT_KV_VERSION``
                    env var (parsed as int), or ``2`` if unset.
        k8s_role:   Vault role name used for k8s Service Account authentication.
                    Defaults to ``VAULT_ROLE`` env var, or ``"slopbox"`` if unset.
    """

    def __init__(
        self,
        vault_url: str | None = None,
        kv_mount: str | None = None,
        kv_version: int | None = None,
        k8s_role: str | None = None,
    ) -> None:
        self._url: str = vault_url or os.environ["VAULT_ADDR"]
        self._mount: str = kv_mount or os.getenv("VAULT_KV_MOUNT", "secret")
        self._version: int = (
            kv_version
            if kv_version is not None
            else int(os.getenv("VAULT_KV_VERSION", "2"))
        )
        self._role: str = k8s_role or os.getenv("VAULT_ROLE", "slopbox")
        self._client: hvac.Client | None = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def get_kv(self, path: str) -> dict[str, str]:
        """Return all key-value pairs stored at *path* in the KV engine.

        Authenticates lazily on the first call; reuses the client thereafter.
        """
        client = self._ensure_authenticated()
        return self._read_kv(client, path)

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _ensure_authenticated(self) -> hvac.Client:
        if self._client is not None and self._client.is_authenticated():
            return self._client

        client = hvac.Client(url=self._url)

        token = os.getenv("VAULT_TOKEN")
        if token:
            client.token = token
        elif _K8S_SA_TOKEN_PATH.exists():
            jwt = _K8S_SA_TOKEN_PATH.read_text()
            client.auth.kubernetes.login(role=self._role, jwt=jwt)
        else:
            raise RuntimeError(
                "no Vault authentication available: set VAULT_TOKEN, or run"
                " inside a Kubernetes pod with a mounted Service Account token"
            )

        if not client.is_authenticated():
            raise RuntimeError(
                f"Vault authentication failed (url={self._url!r},"
                f" role={self._role!r})"
            )

        self._client = client
        return client

    # ------------------------------------------------------------------
    # KV read — v1 and v2
    # ------------------------------------------------------------------

    def _read_kv(self, client: hvac.Client, path: str) -> dict[str, str]:
        if self._version == 1:
            response = client.secrets.kv.v1.read_secret(
                path=path, mount_point=self._mount
            )
            return response["data"]
        else:
            response = client.secrets.kv.v2.read_secret(
                path=path, mount_point=self._mount
            )
            return response["data"]["data"]
