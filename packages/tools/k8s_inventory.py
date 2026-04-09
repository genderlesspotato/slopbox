"""k8s cluster inventory tool.

Connects to every Kubernetes cluster in clusters.yaml and produces a markdown
inventory of observability workloads across three pillars:

  logs    — ECK Elasticsearch, ECK Kibana, Logstash Deployments
  traces  — Tempo Deployments
  metrics — VictoriaMetrics VMCluster / VMSingle CRDs

Output is written to INVENTORY_DIR (default: ./inventory) as:
  index.md              — fleet overview table
  {cluster-name}.md     — per-cluster detail

Each file is also printed to stdout.

Configuration via environment variables:
  INVENTORY_DIR   Output directory (default: ./inventory)
  KUBECONFIG      Path to kubeconfig (default: ~/.kube/config)
  K8S_CONTEXT     Override context (default: current context)
  LOG_FORMAT      human | json  (default: human)
"""

import argparse
import logging
import os
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from kubernetes.client.exceptions import ApiException

from slopbox.k8s_client import KubernetesApiBundle, build_api_bundle
from slopbox.logging import configure_logging
from slopbox_domain.k8s.cluster import KubernetesClusterConfig
from slopbox_domain.k8s.models import (
    ClusterSnapshot,
    NodeProfile,
    PillarSnapshot,
    ResourceItem,
)
from slopbox_domain.registry import ClusterRegistry

logger = logging.getLogger("k8s_inventory")

DEFAULT_OUTPUT_DIR = Path("./inventory")
DEFAULT_CLUSTERS_YAML = Path("clusters.yaml")


# ---------------------------------------------------------------------------
# ResourceSpec — declarative resource descriptor (tool-internal config)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ResourceSpec:
    """Describes a single k8s resource type to fetch within a pillar.

    api_group="" means a core k8s resource; "apps" means the apps/v1 API.
    All other api_group values are treated as CRD groups fetched via
    CustomObjectsApi.
    """
    kind: str           # e.g. "Elasticsearch", "Deployment"
    api_group: str      # e.g. "elasticsearch.k8s.elastic.co"; "apps" for Deployments
    version: str        # e.g. "v1"
    plural: str         # e.g. "elasticsearches", "deployments"
    name_filter: str | None = None    # prefix filter applied to Deployment names
    label_selector: str | None = None


# ---------------------------------------------------------------------------
# PILLARS — declarative pillar configuration
# ---------------------------------------------------------------------------

PILLARS: dict[str, list[ResourceSpec]] = {
    "logs": [
        ResourceSpec("Elasticsearch", "elasticsearch.k8s.elastic.co", "v1", "elasticsearches"),
        ResourceSpec("Kibana",        "kibana.k8s.elastic.co",        "v1", "kibanas"),
        ResourceSpec("Deployment",    "apps",                          "v1", "deployments", name_filter="logstash-"),
    ],
    "traces": [
        ResourceSpec("Deployment", "apps", "v1", "deployments", name_filter="tempo-"),
    ],
    "metrics": [
        ResourceSpec("VMCluster", "operator.victoriametrics.com", "v1beta1", "vmclusters"),
        ResourceSpec("VMSingle",  "operator.victoriametrics.com", "v1beta1", "vmsingles"),
    ],
}


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def _v1deployment_to_raw(dep: object) -> dict:
    """Normalise a V1Deployment object to a plain dict matching CRD item shape."""
    containers = dep.spec.template.spec.containers or []
    return {
        "metadata": {"name": dep.metadata.name, "namespace": dep.metadata.namespace},
        "spec": {
            "replicas": dep.spec.replicas or 0,
            "template": {"spec": {"containers": [{"image": c.image} for c in containers]}},
        },
        "status": {"readyReplicas": dep.status.ready_replicas or 0},
    }


def fetch_resources(
    bundle: KubernetesApiBundle,
    spec: ResourceSpec,
    namespace: str,
) -> tuple[list[dict], bool]:
    """Fetch resources matching *spec* from *namespace*.

    Returns ``(items, detected)`` where ``detected=False`` only when a CRD
    returned 404/405 (operator not installed). Deployment fetches always
    succeed (detected=True).

    Non-404/405 ApiExceptions are re-raised to bubble up through scan_cluster.
    """
    if spec.api_group == "apps":
        resp = bundle.apps.list_namespaced_deployment(namespace=namespace)
        items = [
            _v1deployment_to_raw(dep)
            for dep in resp.items
            if spec.name_filter is None or dep.metadata.name.startswith(spec.name_filter)
        ]
        return items, True

    # CRD via CustomObjectsApi
    try:
        resp = bundle.custom.list_namespaced_custom_object(
            group=spec.api_group,
            version=spec.version,
            namespace=namespace,
            plural=spec.plural,
        )
        return resp.get("items", []), True
    except ApiException as exc:
        if exc.status in (404, 405):
            return [], False
        raise


# ---------------------------------------------------------------------------
# Attribute extraction — per-kind mapping from raw dict → display strings
# ---------------------------------------------------------------------------

def _extract_attributes(spec: ResourceSpec, raw: dict) -> dict[str, str]:
    """Extract display attributes from a normalised raw resource dict."""
    s = raw.get("spec", {})
    status = raw.get("status", {})

    match spec.kind:
        case "Elasticsearch":
            node_count = sum(ns.get("count", 0) for ns in s.get("nodeSets", []))
            return {
                "version": str(s.get("version", "unknown")),
                "nodes": str(node_count),
                "health": str(status.get("health", "unknown")),
            }
        case "Kibana":
            return {
                "version": str(s.get("version", "unknown")),
                "replicas": str(s.get("count", 0)),
            }
        case "Deployment":
            containers = s.get("template", {}).get("spec", {}).get("containers", [])
            image = containers[0].get("image", "unknown") if containers else "unknown"
            return {
                "desired": str(s.get("replicas", 0)),
                "ready": str(status.get("readyReplicas", 0)),
                "image": image,
            }
        case "VMCluster":
            return {
                "vmselect": str(s.get("vmselect", {}).get("replicaCount", 0)),
                "vmstorage": str(s.get("vmstorage", {}).get("replicaCount", 0)),
                "vminsert": str(s.get("vminsert", {}).get("replicaCount", 0)),
            }
        case "VMSingle":
            return {
                "replicas": str(s.get("replicaCount", 0)),
            }
        case _:
            return {}


# ---------------------------------------------------------------------------
# Cluster scan
# ---------------------------------------------------------------------------

def scan_cluster(config: KubernetesClusterConfig) -> ClusterSnapshot:
    """Connect to one cluster and return a full inventory snapshot.

    Raises ApiException for non-404/405 errors so the caller can handle
    per-cluster failures without aborting the whole fleet scan.
    """
    logger.info("scanning cluster %s (context: %s)", config.name, config.context)
    bundle = build_api_bundle(config)

    k8s_version = bundle.version.get_code().git_version
    node_profiles = [NodeProfile.from_v1node(n) for n in bundle.core.list_node().items]
    logger.info(
        "cluster %s: k8s %s, %d nodes",
        config.name, k8s_version, len(node_profiles),
    )

    pillars: list[PillarSnapshot] = []
    for pillar_name, specs in PILLARS.items():
        items: list[ResourceItem] = []
        any_detected = False

        for spec in specs:
            raws, present = fetch_resources(bundle, spec, config.default_namespace)
            if present:
                any_detected = True
            else:
                logger.warning(
                    "cluster %s: %s CRD not found (operator not installed?)",
                    config.name, spec.kind,
                )
            for raw in raws:
                meta = raw.get("metadata", {})
                items.append(ResourceItem(
                    kind=spec.kind,
                    name=meta.get("name", ""),
                    namespace=meta.get("namespace", ""),
                    attributes=_extract_attributes(spec, raw),
                ))

        logger.info(
            "cluster %s: pillar=%s detected=%s resources=%d",
            config.name, pillar_name, any_detected, len(items),
        )
        pillars.append(PillarSnapshot(name=pillar_name, detected=any_detected, resources=items))

    return ClusterSnapshot(
        config=config,
        k8s_version=k8s_version,
        node_profiles=node_profiles,
        pillars=pillars,
    )


# ---------------------------------------------------------------------------
# Markdown rendering helpers
# ---------------------------------------------------------------------------

def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    """Build a simple GitHub-flavoured markdown table."""
    sep = ["---"] * len(headers)
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(sep) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join(lines)


def render_pillar_table(kind: str, items: list[ResourceItem]) -> str:
    """Build a markdown table for all items of the same kind.

    Column headers: Name | Namespace | <attribute keys from first item>.
    """
    if not items:
        return ""
    attr_keys = list(items[0].attributes.keys())
    headers = ["Name", "Namespace"] + [k.title() for k in attr_keys]
    rows = [
        [item.name, item.namespace] + [item.attributes.get(k, "") for k in attr_keys]
        for item in items
    ]
    return f"**{kind}**\n\n" + _md_table(headers, rows)


def render_cluster_md(snapshot: ClusterSnapshot) -> str:
    """Render a per-cluster markdown page."""
    cfg = snapshot.config
    lines: list[str] = [
        f"# {cfg.name}",
        "",
        f"**Environment:** {cfg.environment}  ",
        f"**Region:** {cfg.region or '—'}  ",
        f"**Kubernetes version:** {snapshot.k8s_version}",
        "",
    ]

    # Nodes
    lines.append("## Nodes")
    lines.append("")
    if snapshot.node_profiles:
        node_rows = [
            [n.name, n.role, n.instance_type or "—", n.region or "—"]
            for n in sorted(snapshot.node_profiles, key=lambda n: (n.role, n.name))
        ]
        lines.append(_md_table(["Name", "Role", "Instance Type", "Region"], node_rows))
    else:
        lines.append("_No nodes found._")
    lines.append("")

    # Pillars
    for pillar in snapshot.pillars:
        lines.append(f"## {pillar.name.title()} Pillar")
        lines.append("")
        if not pillar.detected:
            lines.append("_Not detected._")
            lines.append("")
            continue

        # Group resources by kind, preserving PILLARS order
        by_kind: dict[str, list[ResourceItem]] = defaultdict(list)
        for item in pillar.resources:
            by_kind[item.kind].append(item)

        if not by_kind:
            lines.append("_No resources found._")
        else:
            for kind, kind_items in by_kind.items():
                lines.append(render_pillar_table(kind, kind_items))
                lines.append("")

    return "\n".join(lines)


def render_index_md(registry: ClusterRegistry, snapshots: list[ClusterSnapshot]) -> str:
    """Render the fleet index page."""
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    pillar_names = list(PILLARS.keys())

    lines: list[str] = [
        "# Cluster Inventory",
        "",
        f"_Generated: {generated}_",
        "",
        "## Kubernetes Clusters",
        "",
    ]

    # Build snapshot lookup by cluster name
    snap_by_name = {s.config.name: s for s in snapshots}

    headers = ["Cluster", "Env", "Region", "K8s Version", "Nodes"] + [
        p.title() for p in pillar_names
    ]
    rows: list[list[str]] = []
    for cfg in registry.kubernetes:
        snap = snap_by_name.get(cfg.name)
        if snap is None:
            # Cluster failed to scan
            rows.append([cfg.name, cfg.environment, cfg.region or "—", "—", "—"] + ["—"] * len(pillar_names))
            continue
        pillar_cells = []
        snap_pillars = {p.name: p for p in snap.pillars}
        for pname in pillar_names:
            p = snap_pillars.get(pname)
            pillar_cells.append("detected" if (p and p.detected) else "not detected")
        rows.append([
            cfg.name,
            cfg.environment,
            cfg.region or "—",
            snap.k8s_version,
            str(len(snap.node_profiles)),
        ] + pillar_cells)
    lines.append(_md_table(headers, rows))
    lines.append("")

    # ES clusters
    if registry.elasticsearch:
        lines.append("## Elasticsearch Clusters")
        lines.append("")
        es_rows = [
            [c.name, c.environment, getattr(c, "workload", "—"), c.region or "—", c.host]
            for c in registry.elasticsearch
        ]
        lines.append(_md_table(["Name", "Env", "Workload", "Region", "Host"], es_rows))
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_inventory(output_dir: Path, pages: dict[str, str]) -> None:
    """Write pages to output_dir and print each to stdout."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for filename, content in pages.items():
        (output_dir / filename).write_text(content, encoding="utf-8")
        print(content)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    log_format = configure_logging()

    parser = argparse.ArgumentParser(description="k8s cluster inventory tool")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory to write markdown pages (default: $INVENTORY_DIR or ./inventory)",
    )
    args = parser.parse_args()

    output_dir = Path(
        args.output_dir
        or os.getenv("INVENTORY_DIR")
        or DEFAULT_OUTPUT_DIR
    )

    clusters_yaml = DEFAULT_CLUSTERS_YAML
    if not clusters_yaml.exists():
        logger.error("clusters.yaml not found at %s", clusters_yaml.resolve())
        sys.exit(1)

    registry = ClusterRegistry.from_yaml(clusters_yaml)
    k8s_clusters = registry.kubernetes

    if not k8s_clusters:
        logger.error("no kubernetes clusters found in %s", clusters_yaml)
        sys.exit(1)

    logger.info(
        "scanning %d cluster(s): %s",
        len(k8s_clusters),
        ", ".join(c.name for c in k8s_clusters),
    )

    snapshots: list[ClusterSnapshot] = []
    errors: list[str] = []

    with ThreadPoolExecutor(max_workers=len(k8s_clusters)) as pool:
        futures = {pool.submit(scan_cluster, cfg): cfg for cfg in k8s_clusters}
        for future in as_completed(futures):
            cfg = futures[future]
            try:
                snapshots.append(future.result())
            except Exception as exc:
                logger.error("failed to scan cluster %s: %s", cfg.name, exc)
                errors.append(cfg.name)

    snapshots.sort(key=lambda s: s.config.name)

    pages: dict[str, str] = {
        "index.md": render_index_md(registry, snapshots),
    }
    for snap in snapshots:
        pages[f"{snap.config.name}.md"] = render_cluster_md(snap)

    write_inventory(output_dir, pages)
    logger.info(
        "inventory complete: %d page(s) written to %s",
        len(pages), output_dir,
    )

    if errors:
        logger.warning("failed clusters (excluded from inventory): %s", ", ".join(errors))


if __name__ == "__main__":
    main()
