"""VictoriaMetrics cluster configuration model.

Represents a known VictoriaMetrics cluster in the ops team's fleet. Static
identity and connectivity metadata only — no credentials. Internal clusters
require no authentication; the host field is a plain HTTP URL.

Loaded via ClusterRegistry.from_yaml() from clusters.yaml.
"""

from pydantic import BaseModel, ConfigDict, Field


class VictoriaMetricsClusterConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    host: str                           # http(s)://host:port, no trailing slash
    environment: str                    # prod, staging, dev
    role: str | None = None             # primary, replica, etc.
    region: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)
