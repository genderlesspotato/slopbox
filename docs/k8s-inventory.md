# `k8s_inventory.py` — Kubernetes Cluster Inventory

Connects to every Kubernetes cluster listed in `clusters.yaml` in parallel and
produces a Markdown inventory of observability workloads — ECK (Elasticsearch,
Kibana), VictoriaMetrics, Logstash, and Tempo — grouped by _pillar_.

---

## API calls per cluster

| Call | Purpose |
|------|---------|
| `VersionApi.get_code()` | Kubernetes server version |
| `CoreV1Api.list_node()` | Node list (roles, versions, capacity) |
| `CustomObjectsApi.list_namespaced_custom_object()` | Once per CRD spec: ECK Elasticsearch, ECK Kibana, VMCluster, VMSingle |
| `AppsV1Api.list_namespaced_deployment()` | Once per Deployment spec: Logstash, Tempo |

All clusters are scanned concurrently via `ThreadPoolExecutor`.

---

## Running

```bash
# With direnv active:
python k8s_inventory.py

# Specify output directory:
INVENTORY_DIR=./out python k8s_inventory.py

# JSON mode:
LOG_FORMAT=json python k8s_inventory.py > report.json 2> ops.jsonl
```

---

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_FORMAT` | `human` | `human` for Rich terminal output; `json` for machine-readable output |
| `KUBECONFIG` | `~/.kube/config` | Path to kubeconfig file |
| `K8S_CONTEXT` | current context | kubectl context to use |
| `K8S_NAMESPACE` | tool default | Namespace to target |
| `INVENTORY_DIR` | `./inventory` | Output directory for Markdown pages |

---

## Cluster configuration

Clusters are declared in `clusters.yaml` at the repo root:

```yaml
clusters:
  - name: prod-eu-west-1
    context: eks-prod-eu
    namespace: monitoring
  - name: staging-us-east-1
    context: eks-staging-us
    namespace: monitoring
```

---

## Output

The tool writes two types of Markdown pages to `INVENTORY_DIR`:

| File | Contents |
|------|---------|
| `index.md` | Fleet overview: cluster table + ES registry table across all clusters |
| `{cluster-name}.md` | Per-cluster detail: node table + one sub-table per resource kind per pillar |

All pages are also printed to stdout.

---

## Pillars

| Pillar | Resources detected |
|--------|--------------------|
| ECK | `Elasticsearch`, `Kibana` CRDs |
| VictoriaMetrics | `VMCluster`, `VMSingle` CRDs |
| Logstash | `logstash` Deployments |
| Tempo | `tempo` Deployments |

404 and 405 responses (CRD not installed, API not available) are silently
treated as "not detected" rather than errors.
