"""Kubernetes client construction from a KubernetesClusterConfig.

Uses new_client_from_config(), which allocates a fresh Configuration per
call internally — safe to call concurrently from multiple threads (e.g. a
ThreadPoolExecutor fanning out across a fleet of clusters).
"""

import logging
import sys

import kubernetes.client
import kubernetes.config
from kubernetes.config import ConfigException

from slopbox_domain.k8s.cluster import KubernetesClusterConfig

logger = logging.getLogger("slopbox.k8s_client")


# ---------------------------------------------------------------------------
# Client construction
# ---------------------------------------------------------------------------

def build_client(config: KubernetesClusterConfig) -> kubernetes.client.CoreV1Api:
    """Return a CoreV1Api bound to *config*.

    Thread-safe: new_client_from_config() allocates its own Configuration
    so concurrent calls for different clusters cannot interfere.

    Reads KUBECONFIG from the environment (or defaults to ~/.kube/config)
    and selects the context named by config.context.
    """
    try:
        api_client = kubernetes.config.new_client_from_config(
            context=config.context,
        )
    except ConfigException as exc:
        logger.error(
            "failed to load kubeconfig for cluster %r (context %r): %s",
            config.name,
            config.context,
            exc,
        )
        sys.exit(1)

    return kubernetes.client.CoreV1Api(api_client=api_client)
