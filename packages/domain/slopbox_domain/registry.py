"""Cluster registry — loads and filters the ops team's cluster fleet.

The registry is backed by clusters.yaml at the repo root, which is the
version-controlled (GitOps) source of truth for cluster identity and
connectivity metadata. No credentials live here.

Usage:

    from pathlib import Path
    from slopbox_domain.registry import ClusterRegistry

    registry = ClusterRegistry.from_yaml(Path("clusters.yaml"))

    # Filter by direct field:
    prod_es = registry.es(environment="prod")
    metrics_clusters = registry.es(environment="prod", workload="metrics")

    # Filter by tag:
    gold_es = registry.es(tier="gold")

    # k8s clusters:
    prod_k8s = registry.k8s(environment="prod")
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field

from slopbox_domain.es.cluster import ElasticsearchClusterConfig
from slopbox_domain.k8s.cluster import KubernetesClusterConfig


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class ClusterRegistry(BaseModel):
    model_config = ConfigDict(frozen=True)

    elasticsearch: list[ElasticsearchClusterConfig] = Field(default_factory=list)
    kubernetes: list[KubernetesClusterConfig] = Field(default_factory=list)

    @classmethod
    def from_yaml(cls, path: Path) -> ClusterRegistry:
        """Load and validate a cluster registry from a YAML file."""
        with path.open() as f:
            return cls.model_validate(yaml.safe_load(f) or {})

    def es(self, **filters: str) -> list[ElasticsearchClusterConfig]:
        """Return ES clusters matching all given filters.

        Filters are matched against direct model fields first, then against
        the ``tags`` dict.  e.g.::

            registry.es(environment="prod", workload="metrics")
            registry.es(tier="gold")    # matches tags
        """
        return [c for c in self.elasticsearch if _matches(c, filters)]

    def k8s(self, **filters: str) -> list[KubernetesClusterConfig]:
        """Return k8s clusters matching all given filters.

        Filters are matched against direct model fields first, then against
        the ``tags`` dict.  e.g.::

            registry.k8s(environment="prod")
            registry.k8s(tier="gold")   # matches tags
        """
        return [c for c in self.kubernetes if _matches(c, filters)]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _matches(cluster: BaseModel, filters: dict[str, Any]) -> bool:
    """Return True if the cluster satisfies every key=value in filters."""
    tags: dict[str, str] = getattr(cluster, "tags", {})
    return all(
        getattr(cluster, key, None) == value or tags.get(key) == value
        for key, value in filters.items()
    )
