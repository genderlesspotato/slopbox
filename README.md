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

| Workflow | Package | Description |
|----------|---------|-------------|
| [`kibana_export`](docs/kibana-log-export.md) | `slopbox-temporal` | Temporal workflow: ES PIT pagination → gzip NDJSON chunks → S3, replacing manual Kibana CSV exports |

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
