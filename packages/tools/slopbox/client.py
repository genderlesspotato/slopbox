"""Elasticsearch client construction.

Two construction paths:

* ``build_client()`` — single-cluster, env var driven.  Used by tools that
  target one cluster via ``ES_HOST`` / ``ES_API_KEY`` etc.

* ``build_client_for(cluster, secrets)`` — multi-cluster, registry driven.
  Resolves credentials from a ``SecretsBackend`` (Vault) for the named
  cluster config.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import TYPE_CHECKING

from elasticsearch import Elasticsearch

if TYPE_CHECKING:
    from slopbox_domain.es.cluster import ElasticsearchClusterConfig
    from slopbox.vault import SecretsBackend

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Single-cluster construction (env vars)
# ---------------------------------------------------------------------------

def build_client() -> Elasticsearch:
    host = os.environ.get("ES_HOST")
    cloud_id = os.environ.get("ES_CLOUD_ID")
    api_key = os.environ.get("ES_API_KEY")
    username = os.environ.get("ES_USERNAME")
    password = os.environ.get("ES_PASSWORD")

    if not host and not cloud_id:
        logger.error("ES_HOST or ES_CLOUD_ID must be set")
        sys.exit(1)
    if not api_key and not (username and password):
        logger.error("ES_API_KEY or both ES_USERNAME and ES_PASSWORD must be set")
        sys.exit(1)

    kwargs: dict = {}

    if cloud_id:
        kwargs["cloud_id"] = cloud_id
    else:
        kwargs["hosts"] = [host]

    if api_key:
        kwargs["api_key"] = api_key
    else:
        kwargs["basic_auth"] = (username, password)

    return Elasticsearch(**kwargs)


# ---------------------------------------------------------------------------
# Multi-cluster construction (registry + secrets backend)
# ---------------------------------------------------------------------------

def build_client_for(
    cluster: "ElasticsearchClusterConfig",
    secrets: "SecretsBackend",
) -> Elasticsearch:
    """Construct an Elasticsearch client for a known cluster.

    Resolves credentials from *secrets* using ``cluster.vault_path``.  The
    secret must contain ``api_key`` (preferred) or both ``username`` and
    ``password``.

    Args:
        cluster: An ``ElasticsearchClusterConfig`` from the cluster registry.
        secrets: A ``SecretsBackend`` instance (e.g. ``VaultBackend()``).

    Returns:
        An authenticated ``Elasticsearch`` client.

    Raises:
        ValueError: If the Vault secret lacks usable credentials.
    """
    data = secrets.get_kv(cluster.vault_path)
    api_key = data.get("api_key")
    username = data.get("username")
    password = data.get("password")

    if not api_key and not (username and password):
        raise ValueError(
            f"Vault secret at '{cluster.vault_path}' must contain 'api_key'"
            " or both 'username' and 'password'"
        )

    kwargs: dict = {}
    if cluster.cloud_id:
        kwargs["cloud_id"] = cluster.cloud_id
    else:
        kwargs["hosts"] = [cluster.host]

    if api_key:
        kwargs["api_key"] = api_key
    else:
        kwargs["basic_auth"] = (username, password)

    return Elasticsearch(**kwargs)
