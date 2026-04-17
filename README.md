# slopbox

A collection of standalone Elasticsearch and Kubernetes ops tooling, plus
Temporal workflow workers. Each tool is a self-contained Python script; workflows
live in a separate package. All share a common `uv`-managed workspace.

---

## Tools

| Tool | Package | Description |
|------|---------|-------------|
| [`ilm_review.py`](docs/ilm-review.md) | `slopbox-tools` | Inventory ILM-managed indices; emit per-data-stream recommendations to tune shard size and rotation cadence |
| [`dangling_index_scanner.py`](docs/dangling-index-scanner.md) | `slopbox-tools` | Scan an ES data node for orphaned index directories; quarantine and reap them with layered safety checks |
| [`k8s_inventory.py`](docs/k8s-inventory.md) | `slopbox-tools` | Inventory k8s clusters; report observability workloads (ECK, VictoriaMetrics, Logstash, Tempo) as Markdown |

## Workflows

| Workflow | Theme | Package | Description |
|----------|-------|---------|-------------|
| [`kibana_export`](docs/kibana-log-export.md) | `log-ops` | `slopbox-temporal` | ES PIT pagination → gzip NDJSON chunks → S3, replacing manual Kibana CSV exports |
| `log_scrub` | `log-ops` | `slopbox-temporal` | Delete-by-query scrub of sensitive documents across log indices, with index-by-index progress tracking |
| [`node_drain_reboot`](docs/maintenance-node-drain-reboot.md) | `maintenance` | `slopbox-temporal` | Drain and reboot a single Kubernetes node via Ansible playbooks (cordon → drain → reboot → wait-ready → uncordon) |

### Workers

Workflows are grouped into **themes**; each theme has its own Temporal task
queue and its own worker process. Worker processes are intentionally scoped
to the credentials and tools their theme needs — `log-ops` needs ES + S3
access; `maintenance` needs an SSH key and a playbook repo.

| Theme | Task queue | Entrypoint | Workflows |
|-------|-----------|------------|-----------|
| `log-ops` | `log-ops` | `python -m slopbox_temporal.workers.log_ops` | `kibana_export`, `log_scrub` |
| `maintenance` | `maintenance` | `python -m slopbox_temporal.workers.maintenance` | `node_drain_reboot` |

Both workers read `TEMPORAL_ADDRESS` (default `localhost:7233`) and
`TEMPORAL_NAMESPACE` (default `default`); each theme documents the
additional variables its activities need.

---

## Repo layout

```
packages/
├── domain/            # slopbox-domain — shared Pydantic models and ES/k8s boundary types
├── tools/             # slopbox-tools  — CLI scripts + slopbox.* shared utilities
└── temporal-workflows/ # slopbox-temporal — Temporal workflow workers
```

---

## Setup

Prerequisites: [`uv`](https://docs.astral.sh/uv/getting-started/installation/) and [`direnv`](https://direnv.net/docs/installation.html).

```bash
# Clone and enter the repo
git clone <repo-url> slopbox
cd slopbox

# Copy credentials template and fill in values
cp .env.example .env
$EDITOR .env

# Allow direnv — runs uv sync and activates .venv automatically
direnv allow
```

After `direnv allow`, re-entering the directory in any future shell activates
the venv and exports all credentials automatically.

### Without direnv

```bash
uv sync
export ES_HOST=https://my-cluster:9200
export ES_API_KEY=<base64-id:key>
uv run python ilm_review.py
```

---

## Running tests

```bash
# With the venv active (direnv):
pytest

# Explicit:
uv run --group dev pytest
```

Tests use static mock data shaped after real Elasticsearch 7.x / 8.x API
responses. No running cluster is required.

---

## Dependency pinning

`uv.lock` is committed intentionally. This is an ops application, not a library —
pinning all transitive dependencies ensures reproducibility, security auditability,
and offline use in restricted environments.

When bumping a dependency, always commit `pyproject.toml` and `uv.lock` together:

```bash
uv lock
git add pyproject.toml uv.lock
git commit -m "chore: bump <package> to x.y.z"
```

---

## Adding a new tool

1. Drop the script at `packages/tools/`: `packages/tools/my_tool.py`
2. Call `configure_logging()` at the top of `main()` and use a named logger
3. Add dependencies to `packages/tools/pyproject.toml`, then `uv lock && uv sync`
4. Add tests under `packages/tools/tests/`
5. Add an entry to the tools table above and create `docs/my-tool.md`
6. If the tool performs any mutation, implement dry-run mode (`DRY_RUN` env var, default `true`)
