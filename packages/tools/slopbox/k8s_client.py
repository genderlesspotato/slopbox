"""Kubernetes client construction from a KubernetesClusterConfig.

Uses new_client_from_config(), which allocates a fresh Configuration per
call internally — safe to call concurrently from multiple threads (e.g. a
ThreadPoolExecutor fanning out across a fleet of clusters).

Two entry points:
- build_client()     → CoreV1Api (backward-compat, for tools that only need pods/nodes)
- build_api_bundle() → KubernetesApiBundle (for tools that also need Deployments, CRDs, or version)
"""

import logging
import sys
from dataclasses import dataclass

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


# ---------------------------------------------------------------------------
# Multi-API bundle (for tools that need more than CoreV1Api)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class KubernetesApiBundle:
    """All four API wrappers sharing a single ApiClient.

    Use build_api_bundle() to construct; all fields share the same underlying
    connection so new_client_from_config() is called exactly once per cluster.
    """
    core: kubernetes.client.CoreV1Api
    apps: kubernetes.client.AppsV1Api
    custom: kubernetes.client.CustomObjectsApi
    version: kubernetes.client.VersionApi


def build_api_bundle(config: KubernetesClusterConfig) -> KubernetesApiBundle:
    """Allocate one ApiClient and return all four API wrappers.

    Thread-safe: new_client_from_config() allocates its own Configuration so
    concurrent calls for different clusters cannot interfere. All four API
    objects share the same api_client, so only one connection is opened per
    cluster.
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
    return KubernetesApiBundle(
        core=kubernetes.client.CoreV1Api(api_client=api_client),
        apps=kubernetes.client.AppsV1Api(api_client=api_client),
        custom=kubernetes.client.CustomObjectsApi(api_client=api_client),
        version=kubernetes.client.VersionApi(api_client=api_client),
    )
