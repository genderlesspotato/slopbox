# CLAUDE.md — AI Assistant Guide for slopbox

## Project Overview

**slopbox** is a collection of standalone Elasticsearch and Kubernetes ops tooling. Each tool is a self-contained Python script sharing a single `uv`-managed project environment, backed by a shared domain modeling layer.

**Current tools:**

| Script | Purpose |
|--------|---------|
| `ilm_review.py` | Inventories ILM-managed indices grouped by policy and emits per-data-stream recommendations to tune ILM policy and index template settings |
| `dangling_index_scanner.py` | Scans an ES data node's `indices/` directory for UUID directories the cluster no longer recognises; quarantines and reaps them with layered safety checks |
| `k8s_inventory.py` | Inventories k8s clusters; reports observability workloads (ECK, VictoriaMetrics, Logstash, Tempo) per pillar as markdown |

**Runtime:** Python 3.11+, managed by [uv](https://docs.astral.sh/uv/).

---

## Development Environment

**Prerequisites:** `uv` and `direnv` must be installed.

```bash
# First-time setup
cp .env.example .env
$EDITOR .env        # fill in ES_* credentials
direnv allow        # loads .env, runs uv sync, activates .venv
```

After `direnv allow`, re-entering the directory in any future shell automatically activates the venv and exports credentials.

### Environment Variables

**Output:**

| Variable | Required | Notes |
|----------|----------|-------|
| `LOG_FORMAT` | Optional | `human` (default) for Rich terminal output; `json` for newline-delimited JSON logs to stderr + JSON report to stdout |
| `DRY_RUN` | Optional | `true` (default) skips all mutations; set to `false` to enable writes. Only relevant for tools that perform mutations. |

**Elasticsearch:**

| Variable | Required | Notes |
|----------|----------|-------|
| `ES_HOST` | Yes (unless `ES_CLOUD_ID`) | Full URL, e.g. `https://localhost:9200` |
| `ES_CLOUD_ID` | Optional | Elastic Cloud ID — overrides `ES_HOST` |
| `ES_API_KEY` | Optional* | base64 `id:key` string; takes precedence over basic auth |
| `ES_USERNAME` | Optional* | Basic auth username |
| `ES_PASSWORD` | Optional* | Basic auth password |

\* Either `ES_API_KEY` or both `ES_USERNAME`+`ES_PASSWORD` must be set.

**Kubernetes** (for k8s tools):

| Variable | Required | Notes |
|----------|----------|-------|
| `KUBECONFIG` | Optional | Path to kubeconfig file; defaults to `~/.kube/config` |
| `K8S_CONTEXT` | Optional | kubectl context to use; defaults to current context |
| `K8S_NAMESPACE` | Optional | Namespace to target; tool-specific default applies if unset |
| `INVENTORY_DIR` | Optional | Output directory for `k8s_inventory.py` markdown pages; default `./inventory` |

---

## Running Tools

```bash
# With direnv active (recommended):
python ilm_review.py

# Without direnv:
uv run python ilm_review.py
```

Configuration comes from environment variables. Tools may also accept CLI arguments that override env vars — see each tool's documentation.

---

## Output Modes

Tools support two output modes via `LOG_FORMAT`:

| `LOG_FORMAT` | Behavior |
|---|---|
| (unset or `human`) | Rich terminal output — spinners during fetch, formatted tables for the report |
| `json` | JSON log records to stderr for progress/errors; single JSON document to stdout for the report |

`json` mode is designed for k8s and Temporal workflows where downstream systems need to parse the report programmatically or log aggregators (Loki, Datadog, etc.) need structured records.

```bash
# Human mode (default):
python ilm_review.py

# JSON mode — capture report separately from operational logs:
LOG_FORMAT=json python ilm_review.py > report.json 2> ops.jsonl
```

### Logging architecture

All logging is configured by `slopbox/logging.py`:

- `get_log_format()` — reads `LOG_FORMAT`, returns `"human"` or `"json"`
- `configure_logging(format=None)` — configures the root Python logger and returns the active format; call once at the top of `main()` before any other output or `build_client()` calls
- Human mode: `RichHandler` (integrates cleanly with Rich console output)
- JSON mode: `StreamHandler(stderr)` with `_JsonFormatter` — each record is a JSON object with `timestamp` (ISO-8601 UTC), `level`, `logger`, and `message`

The root logger is set to `WARNING` so third-party libraries (elasticsearch-py, urllib3) stay quiet. The `slopbox` and `ilm_review` logger hierarchies are set to `INFO`.

Tool scripts use a named logger rather than `__name__` to avoid the `"__main__"` hierarchy when run as scripts:

```python
logger = logging.getLogger("ilm_review")  # explicit, not __name__
```

---

## Running Tests

```bash
# With direnv active:
pytest

# Explicit:
uv run --group dev pytest
```

Tests live in `tests/` and use static mock data shaped after real ES 7.x and 8.x API responses. **No running cluster is required.**

Time-dependent tests patch `ilm_review.time.time` with a fixed timestamp for determinism. Tests for shared utilities live in `tests/test_formatting.py`, `tests/test_client.py`, and `tests/test_logging.py`.

---

## Key Files

| File | Role |
|------|------|
| `ilm_review.py` | ILM review tool |
| `dangling_index_scanner.py` | Dangling index scanner and reclaimer |
| `k8s_inventory.py` | k8s cluster inventory tool |
| `slopbox/client.py` | Shared Elasticsearch client factory: `build_client()` |
| `slopbox/formatting.py` | Shared formatting utilities: `format_bytes`, `format_duration`, `phase_style`, `health_style` |
| `slopbox/logging.py` | Shared logging configuration: `get_log_format()`, `configure_logging()` |
| `slopbox/k8s_client.py` | k8s client factory: `build_client()` (CoreV1Api), `build_api_bundle()` (KubernetesApiBundle — all 4 API types sharing one connection) |
| `domain/es/models.py` | ES domain objects (`IndexProfile`, …) — Pydantic v2 with `@computed_field` display strings |
| `domain/es/types.py` | Raw ES API boundary models (`RawCatIndexEntry`, `RawIlmExplainEntry`, `RawCatRecoveryEntry`, …) — owns all string→int coercion |
| `domain/k8s/models.py` | k8s domain objects (`PodProfile`, `NodeProfile`, `ResourceItem`, `PillarSnapshot`, `ClusterSnapshot`) — Pydantic, constructed via classmethods |
| `tests/test_ilm_review.py` | ILM tool unit tests — 58+ cases including full profiling and JSON report suite |
| `tests/test_dangling_index_scanner.py` | Dangling scanner unit tests — filesystem, cluster-state parsing, quarantine/reap logic, JSON report |
| `tests/test_k8s_inventory.py` | k8s inventory unit tests — 39 cases covering all layers (model, fetch, scan, render, main) |
| `tests/test_domain_es.py` | Boundary coercion + domain model tests |
| `tests/test_formatting.py` | Formatter helper tests |
| `tests/test_client.py` | Unit tests for `slopbox.client` |
| `tests/test_k8s_client.py` | Unit tests for `slopbox.k8s_client` — `build_client` and `build_api_bundle` |
| `tests/test_logging.py` | Unit tests for `slopbox.logging` |
| `pyproject.toml` | Project metadata, dependencies, pytest config |
| `uv.lock` | Pinned dependency tree (committed intentionally) |
| `.envrc` | direnv: loads `.env`, activates uv venv via `layout uv` |
| `.env.example` | Credential template — copy to `.env`, never commit `.env` |

---

## Shared library (`slopbox/`)

Utilities shared across tools live in the `slopbox/` package:

| Module | Contents |
|--------|----------|
| `slopbox/client.py` | `build_client()` — env var validation + Elasticsearch client construction |
| `slopbox/formatting.py` | `format_bytes`, `format_duration`, `phase_style`, `health_style` |
| `slopbox/logging.py` | `configure_logging()`, `get_log_format()` — human/JSON output mode |
| `slopbox/k8s_client.py` | `build_client()` (CoreV1Api only), `build_api_bundle()` (KubernetesApiBundle — CoreV1Api + AppsV1Api + CustomObjectsApi + VersionApi, one connection per cluster) |

Import them directly in any tool:

```python
from slopbox.client import build_client
from slopbox.formatting import format_bytes, format_duration, phase_style, health_style
from slopbox.logging import configure_logging
```

---

## Domain Modeling Architecture

The `domain/` package provides shared, typed data contracts used across all tools:

```
domain/
├── es/
│   ├── models.py   # Clean domain objects — IndexProfile, SnapshotProfile, …
│   └── types.py    # Raw API boundary models — RawCatIndexEntry, RawIlmExplainEntry, …
└── k8s/
    └── models.py   # k8s domain objects — PodProfile, DeploymentProfile, …
```

### ES tools — two-layer pattern

The ES `_cat` API returns all numerics as strings. `domain/es/types.py` owns all coercion at the boundary so tool scripts never need to call `int(raw_value or 0)`:

```python
# In a tool's correlate function:
raw_cat = RawCatIndexEntry.model_validate(cat_info)   # strings coerced to ints here
raw_ilm = RawIlmExplainEntry.model_validate(ilm_info)

profile = IndexProfile(
    docs=raw_cat.docs_count,          # already int
    size_bytes=raw_cat.store_size_bytes,  # already int
    ...
)
```

Domain objects use `@computed_field` for display strings — they behave as normal attributes but are never stored in the constructor:

```python
profile.size_human   # → "8.5 GB"  (computed from size_bytes)
profile.phase_time   # → "3d"      (computed from phase_age_days)
```

### k8s tools — one-layer pattern

The `kubernetes` Python client returns properly typed objects (`V1Pod`, etc.) — no string coercion needed. Domain models are built via classmethods:

```python
profile = PodProfile.from_v1pod(pod)
```

No `k8s/types.py` module is needed; the k8s client itself is the boundary type.

**Client construction:** Use `build_api_bundle()` for tools that need more than `CoreV1Api`. It calls `new_client_from_config()` exactly once per cluster and returns a `KubernetesApiBundle` (frozen dataclass) holding all four API wrappers sharing one connection. `KubernetesApiBundle` is infrastructure plumbing — it lives in `slopbox/k8s_client.py`, not `domain/`.

```python
from slopbox.k8s_client import build_api_bundle, KubernetesApiBundle

bundle = build_api_bundle(config)   # one connection, four API objects
bundle.core      # CoreV1Api
bundle.apps      # AppsV1Api
bundle.custom    # CustomObjectsApi
bundle.version   # VersionApi
```

---


## Architecture of `ilm_review.py`

The tool makes **exactly 5 API calls** per run:

```
fetch_ilm_policies()    →  GET /_ilm/policy
fetch_ilm_explain()     →  GET /*/_ilm/explain?only_managed=true
fetch_cat_indices()     →  GET /_cat/indices?bytes=b  (includes pri shard count)
fetch_cat_nodes()       →  GET /_cat/nodes?h=node.role  (counts data-role nodes)
fetch_data_streams()    →  GET /_data_stream/*  (template names + backing index lists)
```

Data flows through a clear pipeline:

```
fetch → parse → correlate → profile → render
```

Key components:

| Component | Role |
|-----------|------|
| Fetch functions | Five thin API wrappers |
| `parse_rollover_criteria`, `parse_all_policies` | Parse ILM policy shapes |
| `correlate_data()` | Joins ILM explain + cat stats + data stream membership via domain boundary types; groups by policy |
| `profile_data_streams()` | Computes per-data-stream rotation cadence; emits `DataStreamProfile` with recommendation |
| `_recommend()` | Stateless recommendation ladder: shard size → shard count → SPLIT |
| `render_report()` | Builds and prints Rich inventory table + recommendations table (human mode) |
| `render_report_json()` | Emits the full report as a single JSON document to stdout (json mode) |
| `main()` | Calls `configure_logging()`, orchestrates everything, top-level error handling |

Both ES 7.x and 8.x response shapes are supported throughout.

### Profiling logic

`profile_data_streams()` groups `IndexProfile` objects by data stream name (using the
`data_stream` field populated from `_data_stream/*`), then for each stream:

1. Sorts closed (non-write) indices by `creation_epoch_ms`
2. Computes `avg_rotation_hours` from gaps between consecutive creation times
3. Computes `avg_shard_size_bytes` from `size_bytes / primary_shards` for closed indices
4. Calls `_recommend()` to produce a recommendation

**Recommendation ladder** (requires `max_primary_shard_size` in the policy):

| Condition | Recommendation |
|-----------|----------------|
| rotation < 6 h, avg shard < 50 GB | `increase max_primary_shard_size` to proportional target |
| rotation < 6 h, shard at 50 GB, pri < 60 % of data nodes | `increase number_of_shards` |
| rotation < 6 h, both levers maxed | `SPLIT into multiple independent indices` |
| rotation > 24 h, pri > 1 | `decrease number_of_shards` |
| rotation > 24 h, pri = 1 | `decrease max_primary_shard_size` |
| 6 h ≤ rotation ≤ 24 h | `OK` |
| < 2 rolled-over indices | `insufficient history` |
| policy lacks `max_primary_shard_size` | `cannot profile` |

Constants `TARGET_MIN_HOURS`, `TARGET_MAX_HOURS`, `MAX_SHARD_BYTES`, and
`MAX_NODE_FRACTION` are module-level and can be tuned without touching logic.

---

## Architecture of `dangling_index_scanner.py`

The tool makes **2 API calls** per scan cycle (plus one additional cluster
state fetch per aged quarantine entry during the reap pass):

```
fetch_cluster_state()       →  GET /_cluster/state/metadata
fetch_active_recoveries()   →  GET /_cat/recovery?active_only=true
```

Data flows through a scan-quarantine-reap pipeline:

```
fetch → parse_known_uuids → scan_for_candidates → quarantine → reap
```

Key components:

| Component | Role |
|-----------|------|
| `fetch_cluster_state()` | Fetches full cluster metadata: live index UUIDs + graveyard tombstones |
| `fetch_active_recoveries()` | Fetches in-progress shard recoveries as `RawCatRecoveryEntry` objects |
| `parse_known_uuids()` | Extracts `(live_uuids, graveyard_uuids)` from cluster state dict |
| `parse_recovery_uuids()` | Maps recovery index names → UUIDs via the cluster state |
| `find_indices_dir()` | Locates the `indices/` path; handles both ES 7.x and 8.x layouts |
| `find_es_pid()` | Finds the ES JVM PID via `/proc/*/cmdline`; returns `None` on non-Linux |
| `has_open_fds()` | Checks `/proc/<pid>/fd` for open descriptors under a path; fails safe |
| `scan_for_candidates()` | Walks `indices/`, applies all 7 safety checks, returns `DanglingCandidate` list |
| `quarantine_candidate()` | Atomically renames a candidate into `.quarantine/` |
| `reap_quarantine()` | Reaps aged quarantine entries; restores any that reappear in cluster state |
| `run_scan()` | Orchestrates one full scan-quarantine-reap cycle; returns `ScanResult` |
| `render_report()` | Rich table output (human mode) |
| `render_report_json()` | Single JSON document to stdout (json mode) |
| `main()` | Reads env, calls `configure_logging()`, loops in daemon mode or runs once |

### Safety checks in `scan_for_candidates()`

Every candidate must pass **all** of the following; failure on any check skips
the candidate (never quarantines):

1. Name matches ES UUID pattern (alphanumeric + `_-`, ≥ 10 chars)
2. No `_state` subdirectory (rules out in-flight allocations)
3. UUID not in live cluster state
4. UUID not in index graveyard
5. UUID not targeted by an active shard recovery
6. ctime older than `ORPHAN_AGE_HOURS`
7. ES JVM holds no open file descriptors under the directory (Linux only)

If the cluster state or recovery API cannot be fetched, the entire scan cycle
is aborted — the tool always fails closed on uncertainty.

### Quarantine folder naming

Quarantine entries are named `<uuid>__<epoch_seconds>`. The epoch is parsed
during the reap pass to determine age — the filesystem is the state store, no
external coordination or database is needed.

### ES data directory layouts

| Layout | Path |
|--------|------|
| ES 8.x | `$ES_DATA_PATH/indices/` |
| ES 7.x | `$ES_DATA_PATH/nodes/0/indices/` |

`find_indices_dir()` tries the 8.x path first.

---

## Architecture of `k8s_inventory.py`

Connects to every Kubernetes cluster in `clusters.yaml` in parallel and produces a markdown inventory of observability workloads.

**API calls per cluster:**
```
bundle.version.get_code()                      # k8s server version
bundle.core.list_node()                        # node list
bundle.custom.list_namespaced_custom_object()  # once per CRD spec (ECK ES, Kibana, VMCluster, VMSingle)
bundle.apps.list_namespaced_deployment()       # once per Deployment spec (Logstash, Tempo)
```

**Data flow:**
```
clusters.yaml → ClusterRegistry → registry.kubernetes
  → ThreadPoolExecutor: scan_cluster(config)  [parallel, one thread per cluster]
    → build_api_bundle(config) → KubernetesApiBundle
    → fetch_nodes()            → list[NodeProfile]
    → for each pillar in PILLARS:
        for each spec in pillar:
          fetch_resources(bundle, spec, namespace) → (list[dict], bool)
          _extract_attributes(spec, raw)           → dict[str, str]
          → ResourceItem(kind, name, namespace, attributes)
      → PillarSnapshot(name, detected, resources)
    → ClusterSnapshot(config, k8s_version, node_profiles, pillars)
  → render_index_md(registry, snapshots) → str
  → render_cluster_md(snapshot) → str    (one per cluster)
  → write_inventory(output_dir, pages)   (disk + stdout)
```

**Key components:**

| Component | Role |
|-----------|------|
| `ResourceSpec` | Frozen dataclass (tool-internal config): describes one resource type to fetch — kind, api_group, version, plural, optional name_filter |
| `PILLARS` | `dict[str, list[ResourceSpec]]` — declarative pillar config; add a new resource type with one `ResourceSpec(...)` line |
| `fetch_resources()` | Single generic fetch: routes to `AppsV1Api` (Deployments) or `CustomObjectsApi` (CRDs); 404/405 → `([], False)` |
| `_v1deployment_to_raw()` | Normalises `V1Deployment` typed objects to the same dict shape as CRD items |
| `_extract_attributes()` | Per-kind attribute extraction from normalised dict → `dict[str, str]` display fields |
| `scan_cluster()` | Connects, fetches all pillars, assembles `ClusterSnapshot`; non-404/405 exceptions re-raise |
| `render_index_md()` | Fleet overview page: cluster table + ES registry table |
| `render_cluster_md()` | Per-cluster detail: nodes table + one sub-table per resource kind per pillar |
| `write_inventory()` | `mkdir -p`, write files, print to stdout |
| `main()` | `configure_logging()`, arg/env parsing, parallel scan, render, write |

**Pydantic vs dataclass:**
- `ResourceSpec`, `KubernetesApiBundle` — frozen **dataclasses**: static config/infrastructure, not external data
- `NodeProfile`, `ResourceItem`, `PillarSnapshot`, `ClusterSnapshot` — **Pydantic**: runtime domain data from the k8s API

**Output:** `INVENTORY_DIR/index.md` + `INVENTORY_DIR/{cluster-name}.md` (also printed to stdout)

---

## Code Conventions

### Naming
- Functions: `snake_case`
- Classes/models: `PascalCase`
- Environment variables: `SCREAMING_SNAKE_CASE`
- Constants: inline dicts (e.g., phase → color mappings)

### Style
- **Full type annotations** on all functions and model fields
- **Section-header comment blocks** (60-char separator lines) group related functions — maintain this pattern
- **Pure functions** with minimal side effects; no global mutable state
- **Rich** for all terminal output in human mode: `Console`, `Table`, `box.SIMPLE_HEAD`, `console.rule()`, `console.status()`

### Logging

**Mechanics:**
- Call `configure_logging()` once at the top of `main()` before any output or `build_client()` calls
- Use `logger = logging.getLogger("tool_name")` (named explicitly, not `__name__`) at module level
- In human mode, use `console.status()` spinners for fetch operations (not `logger.info`)
- In json mode, use `logger.info()` for all progress messages
- Never use `sys.exit("ERROR: message")` — use `logger.error(...)` + `sys.exit(1)` so errors route through the configured handler

**What to log — operational signal:**
- Every significant state transition: tool start/end, connection established, config resolved, scan cycle begin/end
- Counts and summaries after bulk operations: `"found 3 dangling candidates, 12 skipped"` — not a dump of each item
- Errors and warnings with enough context to act on: uuid, path, error message, relevant config value
- Degraded-mode fallbacks: e.g. `"ES JVM not found in /proc; open-FD check skipped"`
- Dry-run mode at startup and on each skipped mutation (see Dry-Run Mode below)

**What NOT to log at INFO or above:**
- Raw API response payloads — a cluster state for a large cluster can be hundreds of KB
- Per-item traces when iterating thousands of indices or shards: `"processing index foo-000001"` × 50 000 is noise
- Anything that scales linearly with cluster size and carries no actionable signal

**Scale awareness:**
- In json mode every INFO line is shipped to a log aggregator (Loki, Datadog, etc.). A single serialised cluster-state object for a 500-node cluster can be megabytes — this breaks log shippers and inflates ingestion costs.
- Large structures needed for debugging belong behind `logger.debug(...)`. The root logger is set to WARNING; slopbox tool loggers are set to INFO — DEBUG lines are never emitted in production unless a caller explicitly lowers the level.
- Prefer structured summary fields over embedded blobs: `"live_indices=4200 graveyard_entries=37"` rather than the raw dict.

**Log levels:**

| Level | When to use |
|-------|-------------|
| `INFO` | Normal operational progress an operator monitoring a workflow cares about |
| `WARNING` | Degraded or unexpected but recoverable: skipped safety check, missing optional field, fallback activated |
| `ERROR` | Operation failed; tool will exit or skip a safety-critical step |
| `DEBUG` | Per-item traces, large structure dumps — off by default, never emitted in production |

### Dry-Run Mode

Any tool that performs **costly, irreversible, or destructive operations** (filesystem mutations, index deletion, shard moves, policy writes, etc.) **must** implement a dry-run mode.

**Convention — consistent across all tools:**

| Aspect | Rule |
|--------|------|
| Env var | `DRY_RUN` |
| CLI flag | `--dry-run` / `--no-dry-run` (if the tool has CLI arg parsing; overrides the env var) |
| Default | `true` — safe by default; mutations only when explicitly opted in |
| Parse pattern | `os.getenv("DRY_RUN", "true").lower() != "false"` |
| Startup log | `logger.info("running in dry-run mode (set DRY_RUN=false to enable mutations)")` |
| Mutation log prefix | `[dry-run]` on every would-be mutation line |
| Behaviour | Fetch, parse, and evaluate as normal — skip all writes, renames, and deletes |

The env var is the primary mechanism for containerised and workflow deployments. CLI flags (when present) take precedence and are useful for interactive use.

**Pattern (from `dangling_index_scanner.py`):**

```python
if dry_run:
    logger.info("[dry-run] would quarantine %s → %s", src, dst)
else:
    src.rename(dst)
    logger.info("quarantined %s → %s", src, dst)
```

### Domain Modeling
- **Pydantic v2** for all domain objects (`BaseModel`, `ConfigDict(frozen=True)`)
- **Raw boundary types** (`domain/es/types.py`) own all API coercion — never coerce in tool scripts
- **`@computed_field`** for display strings derived from raw numeric fields
- **`model_validate()`** to construct boundary models from API response dicts

### Error Handling
- Catch specific exceptions: `AuthenticationException`, `ConnectionError`, `TransportError`
- Log with `logger.error(...)` and call `sys.exit(1)`; don't re-raise
- Validate only at system boundaries (env var parsing, API responses) — trust internal code

### Testing
- Use `@pytest.mark.parametrize` for edge cases
- Mock Elasticsearch responses as realistic dicts matching actual API shapes
- Patch `<tool_module>.time.time` for any time-dependent logic (e.g. `ilm_review.time.time`, `dangling_index_scanner.time.time`)
- Use `capsys` to test JSON report output (`render_report_json`)
- Use `caplog` to assert log messages from `logger.error()` / `logger.info()` calls
- Test names should be descriptive: `test_correlate_data_missing_cat_entry_increments_skipped`

---

## Adding a New Tool

1. Drop the script at the repo root: `my_tool.py`
2. Call `configure_logging()` at the top of `main()` and set up a named logger
3. Import shared utilities from `slopbox.*` as needed (client, formatting, logging)
4. Add any new dependencies to `[project.dependencies]` in `pyproject.toml`
5. Run `uv lock && uv sync` (or re-enter the directory so direnv triggers sync)
6. Add tests under `tests/`; extract new shared utilities to `slopbox/` if they'll be reused
7. If the tool performs any mutation (filesystem, index, policy, k8s resource, etc.), implement dry-run following the **Dry-Run Mode** convention above

**Logging pattern:**

```python
import logging
from slopbox.logging import configure_logging

logger = logging.getLogger("my_tool")  # explicit name, not __name__

def main() -> None:
    log_format = configure_logging()
    console = Console() if log_format == "human" else None
    ...
    if log_format == "human":
        render_report(console, ...)
    else:
        render_report_json(...)
```

**Domain objects:**
- If the tool introduces new domain objects, add them under `domain/<system>/models.py`
- If the tool calls the ES `_cat` API, add raw boundary types to `domain/es/types.py`
- If the tool calls a k8s API, construct domain models directly from client objects via a `from_<resource>` classmethod — no raw types module needed

Shared dependencies (`rich`, `elasticsearch`, `pydantic`) and `slopbox.*` utilities are already available without extra steps.

---

## Dependency Management

`uv.lock` is **committed intentionally** — this is an ops application, not a library. Pinning all transitive dependencies ensures reproducibility, security auditability, and offline use in restricted environments.

When bumping a dependency, always commit `pyproject.toml` and `uv.lock` together:

```bash
uv lock
git add pyproject.toml uv.lock
git commit -m "chore: bump <package> to x.y.z"
```

---

## Commit Style

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add snapshot review tool
fix: handle missing phase_time_millis in ES 7.x responses
chore: bump elasticsearch to 9.4.0
docs: document rollover criteria formatting
```

---

## Engineering Practices

### Commit discipline
- Make **small, logically grouped commits** — each commit should represent one coherent change that can be reviewed in isolation
- A single feature may span multiple commits (e.g. domain model, then business logic, then tests, then docs), but each commit must leave the repo in a working state

### Change completeness
Every change must be reflected across all affected artifacts:
- **Tests** — add or update tests for any changed behaviour
- **Documentation** — update `README.md`, `CLAUDE.md`, and any relevant docstrings
- **`pyproject.toml` / `uv.lock`** — update together whenever dependencies change

### Correctness and reliability over performance
These are ops tools that operators depend on for production decisions. Correctness and reliability are the top priorities. Performance is still relevant — tools run against large clusters with thousands of indices — but never sacrifice correctness for speed. Prefer clear, verifiable logic; optimise only where there is a demonstrated bottleneck.
