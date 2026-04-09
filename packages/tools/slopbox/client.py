"""Elasticsearch client construction from environment variables."""

import logging
import os
import sys
from dataclasses import dataclass

from elasticsearch import Elasticsearch

from slopbox_domain.es.version import ClusterVersion

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Client construction
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
# Connected cluster — client + detected version
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ConnectedCluster:
    """An Elasticsearch client paired with the cluster's detected version.

    Use ``build_connected_cluster()`` to construct this. Tools that need
    version-aware behaviour should accept or hold a ``ConnectedCluster``
    rather than a bare ``Elasticsearch`` instance.
    """

    client: Elasticsearch
    version: ClusterVersion


def build_connected_cluster() -> ConnectedCluster:
    """Build an ES client, detect the cluster version, and return both.

    Calls ``client.info()`` once after construction to resolve the running
    version. Exits with code 1 if the cluster cannot be reached or the
    version string cannot be parsed — version detection is a hard requirement
    for version-aware tools.
    """
    client = build_client()
    try:
        info = client.info()
        version = ClusterVersion.from_info(info.body)
    except Exception as exc:
        logger.error("failed to detect cluster version: %s", exc)
        sys.exit(1)
    logger.info("connected to Elasticsearch %s", version)
    return ConnectedCluster(client=client, version=version)
