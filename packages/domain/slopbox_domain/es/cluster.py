"""Elasticsearch cluster configuration model.

Represents a known Elasticsearch cluster in the ops team's fleet. This is
static identity and connectivity metadata — not runtime state. Credentials
are resolved at runtime via Vault: ``vault_path`` points to the KV secret
holding the cluster's ``api_key`` (or ``username`` + ``password``).

Use ``build_client_for(cluster, secrets)`` from ``slopbox.client`` to
construct an authenticated Elasticsearch client.

Loaded via ClusterRegistry.from_yaml() from clusters.yaml.
"""

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ElasticsearchClusterConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    environment: str                        # prod, staging, dev
    workload: str                           # metrics, logs, traces, apm
    region: str | None = None
    host: str | None = None                 # mutually exclusive with cloud_id
    cloud_id: str | None = None
    vault_path: str                         # KV path relative to mount, e.g. "slopbox/es/prod-metrics"
    tags: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _require_one_of_host_cloud_id(self) -> "ElasticsearchClusterConfig":
        if not self.host and not self.cloud_id:
            raise ValueError("one of 'host' or 'cloud_id' must be set")
        return self
