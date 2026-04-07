"""Kubernetes cluster configuration model.

Represents a known Kubernetes cluster in the ops team's fleet. This is static
identity and connectivity metadata — not runtime state. Authentication is
handled by kubeconfig (the `context` field names the kubectl context), so no
separate credential field is needed.

Use slopbox.k8s_client.build_client() to obtain a CoreV1Api bound to a cluster.

Loaded via ClusterRegistry.from_yaml() from clusters.yaml.
"""

from pydantic import BaseModel, ConfigDict, Field


class KubernetesClusterConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    context: str                            # kubectl context name
    environment: str                        # prod, staging, dev
    region: str | None = None
    default_namespace: str = "default"
    tags: dict[str, str] = Field(default_factory=dict)
