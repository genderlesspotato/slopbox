"""Tests for ClusterRegistry, ElasticsearchClusterConfig, KubernetesClusterConfig."""

import textwrap
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from slopbox_domain.es.cluster import ElasticsearchClusterConfig
from slopbox_domain.k8s.cluster import KubernetesClusterConfig
from slopbox_domain.metrics.cluster import VictoriaMetricsClusterConfig
from slopbox_domain.registry import ClusterRegistry


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MINIMAL_ES = {
    "name": "prod-metrics",
    "environment": "prod",
    "workload": "metrics",
    "host": "https://prod-metrics.example.com:9200",
}

MINIMAL_K8S = {
    "name": "prod-eu",
    "context": "arn:aws:eks:eu-west-1:123456789012:cluster/prod-eu",
    "environment": "prod",
}

MINIMAL_VM = {
    "name": "prod-metrics-a",
    "host": "http://vm-a.internal:8428",
    "environment": "prod",
}

REGISTRY_DICT = {
    "elasticsearch": [
        {**MINIMAL_ES, "tags": {"tier": "gold"}},
        {
            "name": "prod-logs",
            "environment": "prod",
            "workload": "logs",
            "region": "aws-us-east-1",
            "host": "https://prod-logs.example.com:9200",
            "tags": {"tier": "gold"},
        },
        {
            "name": "staging-logs",
            "environment": "staging",
            "workload": "logs",
            "host": "https://staging-logs.example.com:9200",
        },
    ],
    "kubernetes": [
        {**MINIMAL_K8S, "tags": {"tier": "gold"}},
        {
            "name": "staging",
            "context": "arn:aws:eks:us-east-1:123456789012:cluster/staging",
            "environment": "staging",
        },
    ],
    "victoriametrics": [
        {**MINIMAL_VM, "role": "primary", "tags": {"tier": "gold"}},
        {
            "name": "prod-metrics-b",
            "host": "http://vm-b.internal:8428",
            "environment": "prod",
            "role": "replica",
            "tags": {"tier": "gold"},
        },
        {
            "name": "staging-metrics-a",
            "host": "http://vm-staging.internal:8428",
            "environment": "staging",
            "role": "primary",
        },
    ],
}


# ---------------------------------------------------------------------------
# ElasticsearchClusterConfig
# ---------------------------------------------------------------------------

class TestElasticsearchClusterConfig:
    def test_valid_with_host(self) -> None:
        c = ElasticsearchClusterConfig(**MINIMAL_ES)
        assert c.name == "prod-metrics"
        assert c.host == "https://prod-metrics.example.com:9200"
        assert c.cloud_id is None
        assert c.tags == {}

    def test_valid_with_cloud_id(self) -> None:
        c = ElasticsearchClusterConfig(
            name="prod-logs",
            environment="prod",
            workload="logs",
            cloud_id="prod-logs:dXMtZWFzdC0x",
        )
        assert c.cloud_id == "prod-logs:dXMtZWFzdC0x"
        assert c.host is None

    def test_requires_host_or_cloud_id(self) -> None:
        with pytest.raises(ValidationError, match="one of 'host' or 'cloud_id' must be set"):
            ElasticsearchClusterConfig(
                name="bad",
                environment="prod",
                workload="metrics",
            )

    def test_forbids_extra_fields(self) -> None:
        with pytest.raises(ValidationError):
            ElasticsearchClusterConfig(**MINIMAL_ES, unknown_field="value")

    def test_frozen(self) -> None:
        c = ElasticsearchClusterConfig(**MINIMAL_ES)
        with pytest.raises(Exception):
            c.name = "changed"  # type: ignore[misc]

    def test_tags_default_empty(self) -> None:
        c = ElasticsearchClusterConfig(**MINIMAL_ES)
        assert c.tags == {}

    def test_optional_region(self) -> None:
        c = ElasticsearchClusterConfig(**MINIMAL_ES, region="aws-us-east-1")
        assert c.region == "aws-us-east-1"


# ---------------------------------------------------------------------------
# KubernetesClusterConfig
# ---------------------------------------------------------------------------

class TestKubernetesClusterConfig:
    def test_valid(self) -> None:
        c = KubernetesClusterConfig(**MINIMAL_K8S)
        assert c.name == "prod-eu"
        assert c.context == "arn:aws:eks:eu-west-1:123456789012:cluster/prod-eu"
        assert c.default_namespace == "default"
        assert c.tags == {}

    def test_default_namespace_override(self) -> None:
        c = KubernetesClusterConfig(**MINIMAL_K8S, default_namespace="monitoring")
        assert c.default_namespace == "monitoring"

    def test_forbids_extra_fields(self) -> None:
        with pytest.raises(ValidationError):
            KubernetesClusterConfig(**MINIMAL_K8S, unknown_field="value")

    def test_frozen(self) -> None:
        c = KubernetesClusterConfig(**MINIMAL_K8S)
        with pytest.raises(Exception):
            c.name = "changed"  # type: ignore[misc]

    def test_optional_region(self) -> None:
        c = KubernetesClusterConfig(**MINIMAL_K8S, region="aws-eu-west-1")
        assert c.region == "aws-eu-west-1"


# ---------------------------------------------------------------------------
# VictoriaMetricsClusterConfig
# ---------------------------------------------------------------------------

class TestVictoriaMetricsClusterConfig:
    def test_valid_minimal(self) -> None:
        c = VictoriaMetricsClusterConfig(**MINIMAL_VM)
        assert c.name == "prod-metrics-a"
        assert c.host == "http://vm-a.internal:8428"
        assert c.environment == "prod"
        assert c.role is None
        assert c.region is None
        assert c.tags == {}

    def test_optional_role(self) -> None:
        c = VictoriaMetricsClusterConfig(**MINIMAL_VM, role="primary")
        assert c.role == "primary"

    def test_optional_region(self) -> None:
        c = VictoriaMetricsClusterConfig(**MINIMAL_VM, region="aws-us-east-1")
        assert c.region == "aws-us-east-1"

    def test_tags_default_empty(self) -> None:
        c = VictoriaMetricsClusterConfig(**MINIMAL_VM)
        assert c.tags == {}

    def test_tags_populated(self) -> None:
        c = VictoriaMetricsClusterConfig(**MINIMAL_VM, tags={"tier": "gold"})
        assert c.tags == {"tier": "gold"}

    def test_forbids_extra_fields(self) -> None:
        with pytest.raises(ValidationError):
            VictoriaMetricsClusterConfig(**MINIMAL_VM, unknown_field="value")

    def test_frozen(self) -> None:
        c = VictoriaMetricsClusterConfig(**MINIMAL_VM)
        with pytest.raises(Exception):
            c.name = "changed"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# ClusterRegistry — construction
# ---------------------------------------------------------------------------

class TestClusterRegistryConstruction:
    def test_empty_registry(self) -> None:
        r = ClusterRegistry()
        assert r.elasticsearch == []
        assert r.kubernetes == []

    def test_from_dict(self) -> None:
        r = ClusterRegistry.model_validate(REGISTRY_DICT)
        assert len(r.elasticsearch) == 3
        assert len(r.kubernetes) == 2
        assert r.elasticsearch[0].name == "prod-metrics"
        assert r.kubernetes[0].name == "prod-eu"

    def test_from_yaml_file(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "clusters.yaml"
        yaml_file.write_text(yaml.dump(REGISTRY_DICT))
        r = ClusterRegistry.from_yaml(yaml_file)
        assert {c.name for c in r.elasticsearch} == {"prod-metrics", "prod-logs", "staging-logs"}
        assert {c.name for c in r.kubernetes} == {"prod-eu", "staging"}

    def test_from_yaml_empty_file(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "clusters.yaml"
        yaml_file.write_text("")
        r = ClusterRegistry.from_yaml(yaml_file)
        assert r.elasticsearch == []
        assert r.kubernetes == []

    def test_from_yaml_only_es(self, tmp_path: Path) -> None:
        data = {"elasticsearch": [MINIMAL_ES]}
        yaml_file = tmp_path / "clusters.yaml"
        yaml_file.write_text(yaml.dump(data))
        r = ClusterRegistry.from_yaml(yaml_file)
        assert len(r.elasticsearch) == 1
        assert r.kubernetes == []

    def test_from_yaml_raises_on_missing_host_and_cloud_id(self, tmp_path: Path) -> None:
        bad = {
            "elasticsearch": [
                {"name": "bad", "environment": "prod", "workload": "metrics"}
            ]
        }
        yaml_file = tmp_path / "clusters.yaml"
        yaml_file.write_text(yaml.dump(bad))
        with pytest.raises(ValidationError, match="one of 'host' or 'cloud_id' must be set"):
            ClusterRegistry.from_yaml(yaml_file)


# ---------------------------------------------------------------------------
# ClusterRegistry — ES filtering
# ---------------------------------------------------------------------------

class TestClusterRegistryEsFilter:
    @pytest.fixture
    def registry(self) -> ClusterRegistry:
        return ClusterRegistry.model_validate(REGISTRY_DICT)

    def test_filter_by_environment_prod(self, registry: ClusterRegistry) -> None:
        result = registry.es(environment="prod")
        assert len(result) == 2
        assert all(c.environment == "prod" for c in result)

    def test_filter_by_environment_staging(self, registry: ClusterRegistry) -> None:
        result = registry.es(environment="staging")
        assert len(result) == 1
        assert result[0].name == "staging-logs"

    def test_filter_by_workload(self, registry: ClusterRegistry) -> None:
        result = registry.es(workload="metrics")
        assert len(result) == 1
        assert result[0].name == "prod-metrics"

    def test_filter_by_multiple_fields(self, registry: ClusterRegistry) -> None:
        result = registry.es(environment="prod", workload="logs")
        assert len(result) == 1
        assert result[0].name == "prod-logs"

    def test_filter_by_tag(self, registry: ClusterRegistry) -> None:
        result = registry.es(tier="gold")
        assert len(result) == 2
        assert all(c.tags.get("tier") == "gold" for c in result)

    def test_filter_by_field_and_tag(self, registry: ClusterRegistry) -> None:
        result = registry.es(environment="prod", tier="gold")
        assert len(result) == 2

    def test_no_match_returns_empty_list(self, registry: ClusterRegistry) -> None:
        result = registry.es(environment="dev")
        assert result == []

    def test_no_filters_returns_all(self, registry: ClusterRegistry) -> None:
        result = registry.es()
        assert len(result) == 3

    def test_unknown_tag_no_match(self, registry: ClusterRegistry) -> None:
        result = registry.es(nonexistent_tag="value")
        assert result == []


# ---------------------------------------------------------------------------
# ClusterRegistry — k8s filtering
# ---------------------------------------------------------------------------

class TestClusterRegistryK8sFilter:
    @pytest.fixture
    def registry(self) -> ClusterRegistry:
        return ClusterRegistry.model_validate(REGISTRY_DICT)

    def test_filter_by_environment_prod(self, registry: ClusterRegistry) -> None:
        result = registry.k8s(environment="prod")
        assert len(result) == 1
        assert result[0].name == "prod-eu"

    def test_filter_by_environment_staging(self, registry: ClusterRegistry) -> None:
        result = registry.k8s(environment="staging")
        assert len(result) == 1
        assert result[0].name == "staging"

    def test_filter_by_tag(self, registry: ClusterRegistry) -> None:
        result = registry.k8s(tier="gold")
        assert len(result) == 1
        assert result[0].name == "prod-eu"

    def test_no_match_returns_empty_list(self, registry: ClusterRegistry) -> None:
        result = registry.k8s(environment="dev")
        assert result == []

    def test_no_filters_returns_all(self, registry: ClusterRegistry) -> None:
        result = registry.k8s()
        assert len(result) == 2


# ---------------------------------------------------------------------------
# ClusterRegistry — VM filtering
# ---------------------------------------------------------------------------

class TestClusterRegistryVmFilter:
    @pytest.fixture
    def registry(self) -> ClusterRegistry:
        return ClusterRegistry.model_validate(REGISTRY_DICT)

    def test_filter_by_environment_prod(self, registry: ClusterRegistry) -> None:
        result = registry.vm(environment="prod")
        assert len(result) == 2
        assert all(c.environment == "prod" for c in result)

    def test_filter_by_environment_staging(self, registry: ClusterRegistry) -> None:
        result = registry.vm(environment="staging")
        assert len(result) == 1
        assert result[0].name == "staging-metrics-a"

    def test_filter_by_role(self, registry: ClusterRegistry) -> None:
        result = registry.vm(role="primary")
        assert len(result) == 2
        assert all(c.role == "primary" for c in result)

    def test_filter_by_name(self, registry: ClusterRegistry) -> None:
        result = registry.vm(name="prod-metrics-b")
        assert len(result) == 1
        assert result[0].host == "http://vm-b.internal:8428"

    def test_filter_by_tag(self, registry: ClusterRegistry) -> None:
        result = registry.vm(tier="gold")
        assert len(result) == 2
        assert all(c.tags.get("tier") == "gold" for c in result)

    def test_filter_by_field_and_tag(self, registry: ClusterRegistry) -> None:
        result = registry.vm(environment="prod", tier="gold")
        assert len(result) == 2

    def test_no_match_returns_empty_list(self, registry: ClusterRegistry) -> None:
        result = registry.vm(environment="dev")
        assert result == []

    def test_no_filters_returns_all(self, registry: ClusterRegistry) -> None:
        result = registry.vm()
        assert len(result) == 3

    def test_unknown_tag_no_match(self, registry: ClusterRegistry) -> None:
        result = registry.vm(nonexistent_tag="value")
        assert result == []


# ---------------------------------------------------------------------------
# Smoke test against the real clusters.yaml
# ---------------------------------------------------------------------------

class TestRealClustersYaml:
    """Validates that clusters.yaml in the repo root is well-formed."""

    def test_loads_successfully(self) -> None:
        clusters_path = Path(__file__).parent.parent.parent.parent / "clusters.yaml"
        assert clusters_path.exists(), "clusters.yaml not found at repo root"
        r = ClusterRegistry.from_yaml(clusters_path)
        assert len(r.elasticsearch) > 0
        assert len(r.kubernetes) > 0
        assert len(r.victoriametrics) > 0

    def test_all_es_clusters_have_required_fields(self) -> None:
        clusters_path = Path(__file__).parent.parent.parent.parent / "clusters.yaml"
        r = ClusterRegistry.from_yaml(clusters_path)
        for c in r.elasticsearch:
            assert c.name
            assert c.environment
            assert c.workload
            assert c.host or c.cloud_id

    def test_all_k8s_clusters_have_required_fields(self) -> None:
        clusters_path = Path(__file__).parent.parent.parent.parent / "clusters.yaml"
        r = ClusterRegistry.from_yaml(clusters_path)
        for c in r.kubernetes:
            assert c.name
            assert c.context
            assert c.environment

    def test_all_vm_clusters_have_required_fields(self) -> None:
        clusters_path = Path(__file__).parent.parent.parent.parent / "clusters.yaml"
        r = ClusterRegistry.from_yaml(clusters_path)
        for c in r.victoriametrics:
            assert c.name
            assert c.host
            assert c.environment
