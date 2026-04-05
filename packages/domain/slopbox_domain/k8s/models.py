"""Kubernetes domain objects.

Unlike ES _cat APIs, the kubernetes Python client returns properly typed
objects (V1Pod, V1Deployment, etc.) — no string coercion boundary layer is
needed. Domain models are constructed directly from client objects via
classmethods.

Add `kubernetes>=29.0.0` to [project.dependencies] when the first k8s tool
is added.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


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
