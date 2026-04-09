"""Kubernetes domain objects.

Unlike ES _cat APIs, the kubernetes Python client returns properly typed
objects (V1Pod, V1Deployment, etc.) — no string coercion boundary layer is
needed. Domain models are constructed directly from client objects via
classmethods.

Add `kubernetes>=29.0.0` to [project.dependencies] when the first k8s tool
is added.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from slopbox_domain.k8s.cluster import KubernetesClusterConfig


class PodProfile(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    namespace: str
    node: str | None
    phase: str
    restart_count: int
    cpu_request: str | None
    memory_request: str | None

    @classmethod
    def from_v1pod(cls, pod: object) -> PodProfile:
        """Construct from a kubernetes.client.V1Pod object."""
        containers = pod.spec.containers or []
        restart_count = sum(
            (cs.restart_count or 0)
            for cs in (pod.status.container_statuses or [])
        )
        cpu_request = next(
            (
                c.resources.requests.get("cpu")
                for c in containers
                if c.resources and c.resources.requests
            ),
            None,
        )
        memory_request = next(
            (
                c.resources.requests.get("memory")
                for c in containers
                if c.resources and c.resources.requests
            ),
            None,
        )
        return cls(
            name=pod.metadata.name,
            namespace=pod.metadata.namespace,
            node=pod.spec.node_name,
            phase=pod.status.phase or "Unknown",
            restart_count=restart_count,
            cpu_request=cpu_request,
            memory_request=memory_request,
        )


# ---------------------------------------------------------------------------
# Cluster inventory domain models
# ---------------------------------------------------------------------------

class NodeProfile(BaseModel):
    """A Kubernetes node with role and cloud topology metadata."""
    model_config = ConfigDict(frozen=True)

    name: str
    role: str                    # "control-plane" | "worker"
    instance_type: str | None
    region: str | None

    @classmethod
    def from_v1node(cls, node: object) -> NodeProfile:
        """Construct from a kubernetes.client.V1Node object."""
        labels: dict[str, str] = node.metadata.labels or {}
        role = "control-plane" if "node-role.kubernetes.io/control-plane" in labels else "worker"
        return cls(
            name=node.metadata.name,
            role=role,
            instance_type=labels.get("node.kubernetes.io/instance-type"),
            region=labels.get("topology.kubernetes.io/region"),
        )


class ResourceItem(BaseModel):
    """A single discovered k8s resource, shaped for inventory display.

    Replaces per-kind profile classes (EckElasticsearchProfile, etc.).
    Attribute keys and values are kind-specific strings suitable for
    rendering directly into a markdown table.
    """
    model_config = ConfigDict(frozen=True)

    kind: str        # "Elasticsearch", "VMCluster", "Deployment", etc.
    name: str
    namespace: str
    attributes: dict[str, str]  # ordered display fields, e.g. {"version": "8.12.0", "health": "green"}


class PillarSnapshot(BaseModel):
    """Inventory of resources for one observability pillar (logs/traces/metrics)."""
    model_config = ConfigDict(frozen=True)

    name: str               # "logs" | "traces" | "metrics"
    detected: bool          # False only when all CRD calls returned 404/405 and no Deployments matched
    resources: list[ResourceItem] = Field(default_factory=list)


class ClusterSnapshot(BaseModel):
    """Full inventory for a single k8s cluster."""
    model_config = ConfigDict(frozen=True)

    config: KubernetesClusterConfig
    k8s_version: str
    node_profiles: list[NodeProfile] = Field(default_factory=list)
    pillars: list[PillarSnapshot] = Field(default_factory=list)
