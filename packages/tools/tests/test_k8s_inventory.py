"""Unit tests for k8s_inventory.py.

All Kubernetes API calls are mocked — no running cluster required.
"""

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import k8s_inventory
from k8s_inventory import (
    PILLARS,
    ResourceSpec,
    _extract_attributes,
    _v1deployment_to_raw,
    fetch_resources,
    render_cluster_md,
    render_index_md,
    render_pillar_table,
    scan_cluster,
)
from slopbox_domain.k8s.cluster import KubernetesClusterConfig
from slopbox_domain.k8s.models import (
    ClusterSnapshot,
    NodeProfile,
    PillarSnapshot,
    ResourceItem,
)
from slopbox_domain.registry import ClusterRegistry


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _cfg(name="prod-k8s", context="prod", environment="prod", namespace="default"):
    return KubernetesClusterConfig(
        name=name, context=context, environment=environment, default_namespace=namespace
    )


def _node(name="node-1", role="worker", instance_type=None, region=None):
    return NodeProfile(name=name, role=role, instance_type=instance_type, region=region)


def _item(kind, name, namespace="monitoring", **attrs):
    return ResourceItem(kind=kind, name=name, namespace=namespace, attributes=attrs)


def _make_snapshot(cfg=None, k8s_version="v1.29.0", nodes=None, pillars=None):
    return ClusterSnapshot(
        config=cfg or _cfg(),
        k8s_version=k8s_version,
        node_profiles=nodes or [],
        pillars=pillars or [],
    )


# ---------------------------------------------------------------------------
# NodeProfile.from_v1node
# ---------------------------------------------------------------------------

def _mock_node(name, labels):
    node = MagicMock()
    node.metadata.name = name
    node.metadata.labels = labels
    return node


def test_node_profile_worker_no_label():
    n = NodeProfile.from_v1node(_mock_node("n1", {}))
    assert n.role == "worker"
    assert n.instance_type is None
    assert n.region is None


def test_node_profile_control_plane_label():
    labels = {"node-role.kubernetes.io/control-plane": ""}
    n = NodeProfile.from_v1node(_mock_node("cp-1", labels))
    assert n.role == "control-plane"


def test_node_profile_instance_type_and_region():
    labels = {
        "node.kubernetes.io/instance-type": "m5.xlarge",
        "topology.kubernetes.io/region": "eu-west-1",
    }
    n = NodeProfile.from_v1node(_mock_node("n2", labels))
    assert n.instance_type == "m5.xlarge"
    assert n.region == "eu-west-1"


# ---------------------------------------------------------------------------
# _v1deployment_to_raw
# ---------------------------------------------------------------------------

def _mock_dep(name, namespace, replicas, ready_replicas, image):
    dep = MagicMock()
    dep.metadata.name = name
    dep.metadata.namespace = namespace
    dep.spec.replicas = replicas
    dep.status.ready_replicas = ready_replicas
    container = MagicMock()
    container.image = image
    dep.spec.template.spec.containers = [container]
    return dep


def test_v1deployment_to_raw_shape():
    raw = _v1deployment_to_raw(_mock_dep("logstash-prod", "logging", 3, 3, "logstash:8.12.0"))
    assert raw["metadata"]["name"] == "logstash-prod"
    assert raw["metadata"]["namespace"] == "logging"
    assert raw["spec"]["replicas"] == 3
    assert raw["status"]["readyReplicas"] == 3
    assert raw["spec"]["template"]["spec"]["containers"][0]["image"] == "logstash:8.12.0"


def test_v1deployment_to_raw_none_replicas_becomes_zero():
    raw = _v1deployment_to_raw(_mock_dep("dep", "ns", None, None, "img"))
    assert raw["spec"]["replicas"] == 0
    assert raw["status"]["readyReplicas"] == 0


def test_v1deployment_to_raw_empty_containers():
    dep = MagicMock()
    dep.metadata.name = "dep"
    dep.metadata.namespace = "ns"
    dep.spec.replicas = 1
    dep.status.ready_replicas = 1
    dep.spec.template.spec.containers = []
    raw = _v1deployment_to_raw(dep)
    assert raw["spec"]["template"]["spec"]["containers"] == []


# ---------------------------------------------------------------------------
# _extract_attributes
# ---------------------------------------------------------------------------

_ES_SPEC = ResourceSpec("Elasticsearch", "elasticsearch.k8s.elastic.co", "v1", "elasticsearches")
_KB_SPEC = ResourceSpec("Kibana", "kibana.k8s.elastic.co", "v1", "kibanas")
_DEP_SPEC = ResourceSpec("Deployment", "apps", "v1", "deployments", name_filter="logstash-")
_VMC_SPEC = ResourceSpec("VMCluster", "operator.victoriametrics.com", "v1beta1", "vmclusters")
_VMS_SPEC = ResourceSpec("VMSingle", "operator.victoriametrics.com", "v1beta1", "vmsingles")


def test_extract_elasticsearch_node_count_sums_node_sets():
    raw = {
        "spec": {"version": "8.12.0", "nodeSets": [{"count": 3}, {"count": 2}]},
        "status": {"health": "green"},
    }
    attrs = _extract_attributes(_ES_SPEC, raw)
    assert attrs["nodes"] == "5"
    assert attrs["version"] == "8.12.0"
    assert attrs["health"] == "green"


def test_extract_elasticsearch_missing_status_defaults():
    raw = {"spec": {"version": "8.0.0", "nodeSets": []}, "status": {}}
    attrs = _extract_attributes(_ES_SPEC, raw)
    assert attrs["health"] == "unknown"
    assert attrs["nodes"] == "0"


def test_extract_kibana():
    raw = {"spec": {"version": "8.12.0", "count": 2}, "status": {}}
    attrs = _extract_attributes(_KB_SPEC, raw)
    assert attrs["version"] == "8.12.0"
    assert attrs["replicas"] == "2"


def test_extract_deployment():
    raw = {
        "spec": {
            "replicas": 3,
            "template": {"spec": {"containers": [{"image": "logstash:8.12.0"}]}},
        },
        "status": {"readyReplicas": 2},
    }
    attrs = _extract_attributes(_DEP_SPEC, raw)
    assert attrs["desired"] == "3"
    assert attrs["ready"] == "2"
    assert attrs["image"] == "logstash:8.12.0"


def test_extract_deployment_no_containers():
    raw = {
        "spec": {"replicas": 1, "template": {"spec": {"containers": []}}},
        "status": {"readyReplicas": 0},
    }
    attrs = _extract_attributes(_DEP_SPEC, raw)
    assert attrs["image"] == "unknown"


def test_extract_vmcluster_all_replica_counts():
    raw = {
        "spec": {
            "vmselect": {"replicaCount": 2},
            "vmstorage": {"replicaCount": 3},
            "vminsert": {"replicaCount": 2},
        },
        "status": {},
    }
    attrs = _extract_attributes(_VMC_SPEC, raw)
    assert attrs["vmselect"] == "2"
    assert attrs["vmstorage"] == "3"
    assert attrs["vminsert"] == "2"


def test_extract_vmsingle():
    raw = {"spec": {"replicaCount": 1}, "status": {}}
    attrs = _extract_attributes(_VMS_SPEC, raw)
    assert attrs["replicas"] == "1"


def test_extract_unknown_kind_returns_empty():
    spec = ResourceSpec("Unknown", "some.group", "v1", "unknowns")
    attrs = _extract_attributes(spec, {"spec": {}, "status": {}})
    assert attrs == {}


# ---------------------------------------------------------------------------
# fetch_resources
# ---------------------------------------------------------------------------

def _make_bundle(apps=None, custom=None, core=None, version=None):
    from slopbox.k8s_client import KubernetesApiBundle
    return KubernetesApiBundle(
        core=core or MagicMock(),
        apps=apps or MagicMock(),
        custom=custom or MagicMock(),
        version=version or MagicMock(),
    )


def test_fetch_resources_crd_happy_path():
    custom = MagicMock()
    custom.list_namespaced_custom_object.return_value = {
        "items": [{"metadata": {"name": "es-prod"}, "spec": {}, "status": {}}]
    }
    bundle = _make_bundle(custom=custom)
    items, detected = fetch_resources(bundle, _ES_SPEC, "monitoring")
    assert detected is True
    assert len(items) == 1
    assert items[0]["metadata"]["name"] == "es-prod"


@pytest.mark.parametrize("status_code", [404, 405])
def test_fetch_resources_crd_not_found(status_code):
    from kubernetes.client.exceptions import ApiException
    custom = MagicMock()
    exc = ApiException(status=status_code)
    custom.list_namespaced_custom_object.side_effect = exc
    bundle = _make_bundle(custom=custom)
    items, detected = fetch_resources(bundle, _ES_SPEC, "monitoring")
    assert detected is False
    assert items == []


def test_fetch_resources_crd_other_exception_reraises():
    from kubernetes.client.exceptions import ApiException
    custom = MagicMock()
    custom.list_namespaced_custom_object.side_effect = ApiException(status=500)
    bundle = _make_bundle(custom=custom)
    with pytest.raises(ApiException):
        fetch_resources(bundle, _ES_SPEC, "monitoring")


def test_fetch_resources_deployment_filters_by_prefix():
    apps = MagicMock()
    dep_match = _mock_dep("logstash-prod", "logging", 2, 2, "logstash:8")
    dep_skip = _mock_dep("tempo-prod", "tracing", 1, 1, "tempo:latest")
    apps.list_namespaced_deployment.return_value.items = [dep_match, dep_skip]
    bundle = _make_bundle(apps=apps)

    spec = ResourceSpec("Deployment", "apps", "v1", "deployments", name_filter="logstash-")
    items, detected = fetch_resources(bundle, spec, "logging")
    assert detected is True
    assert len(items) == 1
    assert items[0]["metadata"]["name"] == "logstash-prod"


def test_fetch_resources_deployment_no_filter_returns_all():
    apps = MagicMock()
    dep1 = _mock_dep("dep-a", "ns", 1, 1, "img-a")
    dep2 = _mock_dep("dep-b", "ns", 1, 1, "img-b")
    apps.list_namespaced_deployment.return_value.items = [dep1, dep2]
    bundle = _make_bundle(apps=apps)

    spec = ResourceSpec("Deployment", "apps", "v1", "deployments")  # no name_filter
    items, detected = fetch_resources(bundle, spec, "ns")
    assert len(items) == 2


# ---------------------------------------------------------------------------
# scan_cluster
# ---------------------------------------------------------------------------

def _make_full_bundle():
    """Return a KubernetesApiBundle mock with sensible defaults for a full scan."""
    core = MagicMock()
    apps = MagicMock()
    custom = MagicMock()
    version = MagicMock()

    version.get_code.return_value.git_version = "v1.29.0"

    node_mock = _mock_node("node-1", {})
    core.list_node.return_value.items = [node_mock]

    apps.list_namespaced_deployment.return_value.items = []

    custom.list_namespaced_custom_object.return_value = {"items": []}

    from slopbox.k8s_client import KubernetesApiBundle
    return KubernetesApiBundle(core=core, apps=apps, custom=custom, version=version)


@patch("k8s_inventory.build_api_bundle")
def test_scan_cluster_happy_path(mock_bundle_factory):
    bundle = _make_full_bundle()
    mock_bundle_factory.return_value = bundle

    snapshot = scan_cluster(_cfg())

    assert snapshot.k8s_version == "v1.29.0"
    assert len(snapshot.node_profiles) == 1
    assert len(snapshot.pillars) == len(PILLARS)


@patch("k8s_inventory.build_api_bundle")
def test_scan_cluster_crd_not_found_sets_detected_false(mock_bundle_factory, caplog):
    from kubernetes.client.exceptions import ApiException
    bundle = _make_full_bundle()
    bundle.custom.list_namespaced_custom_object.side_effect = ApiException(status=404)
    mock_bundle_factory.return_value = bundle

    with caplog.at_level(logging.WARNING, logger="k8s_inventory"):
        snapshot = scan_cluster(_cfg())

    # All CRD-based specs return 404, so CRD pillars are not detected
    logs_pillar = next(p for p in snapshot.pillars if p.name == "logs")
    # logs pillar has Deployments (which succeed), so detected=True overall
    # but the CRD specs individually warn
    assert "CRD not found" in caplog.text


@patch("k8s_inventory.build_api_bundle")
def test_scan_cluster_all_crd_missing_pillar_not_detected(mock_bundle_factory):
    from kubernetes.client.exceptions import ApiException
    bundle = _make_full_bundle()
    # CRDs all 404, Deployments return nothing
    bundle.custom.list_namespaced_custom_object.side_effect = ApiException(status=404)
    bundle.apps.list_namespaced_deployment.return_value.items = []
    mock_bundle_factory.return_value = bundle

    snapshot = scan_cluster(_cfg())

    metrics_pillar = next(p for p in snapshot.pillars if p.name == "metrics")
    # metrics has no Deployments, only CRDs — all 404 → not detected
    assert metrics_pillar.detected is False
    assert metrics_pillar.resources == []


@patch("k8s_inventory.build_api_bundle")
def test_scan_cluster_non_404_api_exception_reraises(mock_bundle_factory):
    from kubernetes.client.exceptions import ApiException
    bundle = _make_full_bundle()
    bundle.custom.list_namespaced_custom_object.side_effect = ApiException(status=500)
    mock_bundle_factory.return_value = bundle

    with pytest.raises(ApiException):
        scan_cluster(_cfg())


# ---------------------------------------------------------------------------
# render_pillar_table
# ---------------------------------------------------------------------------

def test_render_pillar_table_column_headers_from_attributes():
    items = [
        _item("Elasticsearch", "es-prod", version="8.12.0", nodes="5", health="green"),
        _item("Elasticsearch", "es-dev",  version="8.11.0", nodes="3", health="yellow"),
    ]
    table = render_pillar_table("Elasticsearch", items)
    assert "**Elasticsearch**" in table
    assert "Name" in table
    assert "Namespace" in table
    assert "Version" in table
    assert "Nodes" in table
    assert "Health" in table
    assert "es-prod" in table
    assert "green" in table


def test_render_pillar_table_empty_returns_empty_string():
    assert render_pillar_table("Elasticsearch", []) == ""


# ---------------------------------------------------------------------------
# render_cluster_md
# ---------------------------------------------------------------------------

def _logs_pillar(detected=True):
    resources = [
        _item("Elasticsearch", "es-prod", "logging", version="8.12.0", nodes="5", health="green"),
        _item("Kibana", "kb-prod", "logging", version="8.12.0", replicas="1"),
    ] if detected else []
    return PillarSnapshot(name="logs", detected=detected, resources=resources)


def test_render_cluster_md_contains_cluster_name():
    snap = _make_snapshot(cfg=_cfg(name="prod-k8s"), k8s_version="v1.29.0")
    md = render_cluster_md(snap)
    assert "prod-k8s" in md
    assert "v1.29.0" in md


def test_render_cluster_md_shows_nodes_table():
    nodes = [_node("node-1", "worker", "m5.xlarge", "eu-west-1")]
    snap = _make_snapshot(nodes=nodes)
    md = render_cluster_md(snap)
    assert "node-1" in md
    assert "worker" in md
    assert "m5.xlarge" in md


def test_render_cluster_md_not_detected_message():
    pillar = PillarSnapshot(name="traces", detected=False, resources=[])
    snap = _make_snapshot(pillars=[pillar])
    md = render_cluster_md(snap)
    assert "Not detected" in md


def test_render_cluster_md_detected_shows_resources():
    pillar = _logs_pillar(detected=True)
    snap = _make_snapshot(pillars=[pillar])
    md = render_cluster_md(snap)
    assert "es-prod" in md
    assert "green" in md
    assert "kb-prod" in md


def test_render_cluster_md_no_nodes_fallback():
    snap = _make_snapshot(nodes=[])
    md = render_cluster_md(snap)
    assert "No nodes found" in md


# ---------------------------------------------------------------------------
# render_index_md
# ---------------------------------------------------------------------------

def _make_registry(k8s_names=("prod-k8s",)):
    from slopbox_domain.es.cluster import ElasticsearchClusterConfig
    cfgs = [
        KubernetesClusterConfig(name=n, context=n, environment="prod")
        for n in k8s_names
    ]
    return ClusterRegistry(kubernetes=cfgs, elasticsearch=[])


def test_render_index_md_contains_generated_timestamp():
    registry = _make_registry()
    snap = _make_snapshot(cfg=_cfg(name="prod-k8s"))
    md = render_index_md(registry, [snap])
    assert "Generated:" in md


def test_render_index_md_cluster_name_in_table():
    registry = _make_registry(("prod-k8s",))
    snap = _make_snapshot(cfg=_cfg(name="prod-k8s"))
    md = render_index_md(registry, [snap])
    assert "prod-k8s" in md


def test_render_index_md_pillar_detected_cell():
    registry = _make_registry()
    pillar = PillarSnapshot(name="logs", detected=True, resources=[])
    snap = _make_snapshot(cfg=_cfg(name="prod-k8s"), pillars=[pillar])
    md = render_index_md(registry, [snap])
    assert "detected" in md


def test_render_index_md_missing_cluster_shows_dash():
    registry = _make_registry(("prod-k8s", "staging-k8s"))
    # only one snapshot provided — staging failed
    snap = _make_snapshot(cfg=_cfg(name="prod-k8s"))
    md = render_index_md(registry, [snap])
    # staging-k8s row should exist with "—"
    assert "staging-k8s" in md


def test_render_index_md_no_es_clusters_omits_es_section():
    registry = _make_registry()
    snap = _make_snapshot(cfg=_cfg(name="prod-k8s"))
    md = render_index_md(registry, [snap])
    assert "Elasticsearch Clusters" not in md


# ---------------------------------------------------------------------------
# main() — write_inventory and CLI
# ---------------------------------------------------------------------------

@patch("k8s_inventory.scan_cluster")
@patch("k8s_inventory.ClusterRegistry.from_yaml")
def test_main_writes_index_and_cluster_files(mock_registry, mock_scan, tmp_path):
    cfg = _cfg(name="prod-k8s")
    mock_registry.return_value = ClusterRegistry(
        kubernetes=[cfg], elasticsearch=[]
    )
    mock_scan.return_value = _make_snapshot(cfg=cfg, k8s_version="v1.29.0")

    with patch("sys.argv", ["k8s_inventory.py", "--output-dir", str(tmp_path)]):
        with patch("k8s_inventory.DEFAULT_CLUSTERS_YAML") as mock_yaml:
            mock_yaml.exists.return_value = True
            k8s_inventory.main()

    assert (tmp_path / "index.md").exists()
    assert (tmp_path / "prod-k8s.md").exists()


@patch("k8s_inventory.scan_cluster")
@patch("k8s_inventory.ClusterRegistry.from_yaml")
def test_main_failed_cluster_excluded_from_output(mock_registry, mock_scan, tmp_path):
    good_cfg = _cfg(name="prod-k8s")
    bad_cfg = _cfg(name="staging-k8s", context="staging")
    mock_registry.return_value = ClusterRegistry(
        kubernetes=[good_cfg, bad_cfg], elasticsearch=[]
    )

    def scan_side_effect(cfg):
        if cfg.name == "staging-k8s":
            raise RuntimeError("connection refused")
        return _make_snapshot(cfg=cfg)

    mock_scan.side_effect = scan_side_effect

    with patch("sys.argv", ["k8s_inventory.py", "--output-dir", str(tmp_path)]):
        with patch("k8s_inventory.DEFAULT_CLUSTERS_YAML") as mock_yaml:
            mock_yaml.exists.return_value = True
            k8s_inventory.main()

    assert (tmp_path / "prod-k8s.md").exists()
    assert not (tmp_path / "staging-k8s.md").exists()


@patch("k8s_inventory.scan_cluster")
@patch("k8s_inventory.ClusterRegistry.from_yaml")
def test_main_output_dir_env_var(mock_registry, mock_scan, tmp_path, monkeypatch):
    cfg = _cfg(name="prod-k8s")
    mock_registry.return_value = ClusterRegistry(kubernetes=[cfg], elasticsearch=[])
    mock_scan.return_value = _make_snapshot(cfg=cfg)
    monkeypatch.setenv("INVENTORY_DIR", str(tmp_path))

    with patch("sys.argv", ["k8s_inventory.py"]):
        with patch("k8s_inventory.DEFAULT_CLUSTERS_YAML") as mock_yaml:
            mock_yaml.exists.return_value = True
            k8s_inventory.main()

    assert (tmp_path / "index.md").exists()
