# slopbox

A collection of small Elasticsearch and ops tooling. Each tool is a standalone
Python script sharing a common project environment.

---

## Tools

### `dangling_index_scanner.py` — Dangling Index Scanner

Scans an Elasticsearch data node's `indices/` directory for UUID directories
that the cluster no longer recognises, **quarantines** them safely, and **reaps**
quarantine entries after a second grace period. Default mode is dry-run.

Makes **2 API calls** per scan cycle:

| Call | Purpose |
|------|---------|
| `GET /_cluster/state/metadata` | Fetch all live index UUIDs and graveyard tombstones |
| `GET /_cat/recovery?active_only=true` | Check for in-progress shard recoveries (belt-and-suspenders) |

A directory is a dangling candidate only when **all** of the following hold:
it is not in the live cluster state, not in the index graveyard, lacks a
`_state` subdirectory, is older than `ORPHAN_AGE_HOURS` by ctime, is not
targeted by an active recovery, and the ES JVM holds no open file descriptors
under it (Linux/`/proc` only).

Reclamation is two-phase: an atomic `rename(2)` into `.quarantine/` (Phase 1),
followed by permanent deletion once the quarantine entry is old enough and a
fresh cluster state re-confirms the UUID is still absent (Phase 2).

| Environment variable | Default | Description |
|----------------------|---------|-------------|
| `ES_DATA_PATH` | *(required)* | Path to the ES data directory |
| `DRY_RUN` | `true` | Set to `false` to enable filesystem mutations |
| `ORPHAN_AGE_HOURS` | `24` | Minimum ctime age before a dir is a candidate |
| `QUARANTINE_GRACE_HOURS` | `48` | Minimum quarantine age before reaping |
| `SCAN_INTERVAL_SECONDS` | `3600` | Sleep between iterations in daemon mode |
| `DAEMON` | `false` | `true` → loop forever; `false` → single run |

```bash
# Dry-run single scan (safe default):
ES_DATA_PATH=/var/lib/elasticsearch/data python dangling_index_scanner.py

# JSON mode — capture report separately from logs:
LOG_FORMAT=json ES_DATA_PATH=... python dangling_index_scanner.py > report.json 2> ops.jsonl

# Enable mutations only after validating dry-run output:
DRY_RUN=false ES_DATA_PATH=... python dangling_index_scanner.py
```

---

### `ilm_review.py` — ILM Policy Reviewer

Inventories all ILM-managed indices grouped by policy, profiles the rotation
cadence of each data stream, and emits per-data-stream recommendations to tune
`max_primary_shard_size` and `number_of_shards` in ILM policies and index
templates. Targets a rotation cadence of 6–24 hours per index.

Makes exactly **5 API calls**:

| Call | Purpose |
|------|---------|
| `GET /_ilm/policy` | Fetch all ILM policies and their rollover thresholds |
| `GET /*/_ilm/explain?only_managed=true` | Fetch lifecycle state for every managed index |
| `GET /_cat/indices?bytes=b` | Fetch current doc counts, store sizes, shard counts, and creation timestamps |
| `GET /_cat/nodes?h=node.role` | Count data-role nodes for shard spread calculations |
| `GET /_data_stream/*` | Fetch template names and backing index lists per data stream |

#### Sample output

```
─────────────────────── Elasticsearch ILM Policy Review ───────────────────────
  Generated: 2026-04-03 14:22 UTC  |  Policies: 2  |  Managed indices: 47

──────────────────── logs-default-policy  12 indices ───────────────────────────
  Phases: hot → warm → cold → delete
  Rollover criteria: max_age=7d  max_primary_shard_size=50gb

 Index                    Phase   Index Age   Phase Age   Docs           Size      Pri   Shard Size   Health   Status
 ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────
 logs-app-000023 ✎        hot     2d 4h       2d 4h       12,450,221     8.3 GB    1     8.3 GB       green
 logs-app-000022          warm    9d 1h       2d 1h       198,332,100    41.9 GB   1     41.9 GB      green
 logs-app-000021          cold    16d 3h      7d 0h       200,001,441    42.8 GB   1     42.8 GB      green

  Summary: 3 indices  |  Docs: 410,783,762  |  Size: 93.0 GB

─────────────────────────────── ILM Recommendations ───────────────────────────
  Target cadence: 6h – 24h per index  |  Max shard size: 50gb  |  Max shard spread: 60% of data nodes

 Data Stream / Index        Template              Avg Rotation   Avg Shard Size   Pri   Recommendation
 ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
 logs-app                   logs-app-template     18.3h          41.9 GB          1     OK
 logs-nginx                 logs-nginx-template   1.2h           3.1 GB           1     increase max_primary_shard_size → 25gb
 logs-slow                  logs-slow-template    48.7h          42.5 GB          2     decrease number_of_shards → 1
 logs-new                   logs-new-template     —              —                1     insufficient history
```

#### How recommendations work

The tool targets a rotation cadence of **6–24 hours per index**. It computes the
average time between rollovers for each data stream (requires at least 2
rolled-over indices) and applies the following ladder:

| Condition | Recommendation |
|-----------|----------------|
| rotation < 6h, avg shard < 50 GB | `increase max_primary_shard_size` — scale proportionally toward the 6h target |
| rotation < 6h, shard at cap, pri < 60% of data nodes | `increase number_of_shards` — capped at 60% node spread |
| rotation < 6h, both levers exhausted | `SPLIT into multiple independent indices` |
| rotation > 24h, pri > 1 | `decrease number_of_shards` — scale proportionally toward the 24h target |
| rotation > 24h, pri = 1 | `decrease max_primary_shard_size` — floor at 1 GB |
| 6h ≤ rotation ≤ 24h | `OK` |
| fewer than 2 rolled-over indices | `insufficient history` |
| policy has no `max_primary_shard_size` criterion | `cannot profile` |

---

## Requirements

- [uv](https://docs.astral.sh/uv/getting-started/installation/) — Python project and venv management
- [direnv](https://direnv.net/docs/installation.html) — automatic environment variable loading
- Elasticsearch 7.x or 8.x with a user that has at minimum:
  - `monitor` cluster privilege (for `_cat/indices` and `_ilm/explain`)
  - `manage_ilm` or `read_ilm` cluster privilege (for `_ilm/policy`)

---

## Setup

```bash
# 1. Clone the repo and enter it
git clone <repo-url> slopbox
cd slopbox

# 2. Copy the environment template and fill in your credentials
cp .env.example .env
$EDITOR .env

# 3. Allow direnv to load the environment and set up the venv
direnv allow
#    ^ This runs `uv sync` automatically and activates .venv
```

After `direnv allow` completes, your shell has the venv on `PATH` and all
`ES_*` variables exported. Re-entering the directory in future shells will
do this automatically.

---

## Running

```bash
python ilm_review.py
```

That's it. No arguments are needed — all configuration comes from the
environment variables set by direnv.

### Running without direnv

```bash
uv sync
export ES_HOST=https://my-cluster:9200
export ES_API_KEY=<base64-id:key>
uv run python ilm_review.py
```

---

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `LOG_FORMAT` | Optional | Output mode: `human` (default) for Rich terminal output, `json` for machine-readable JSON |
| `ES_HOST` | Yes (unless `ES_CLOUD_ID`) | Full cluster URL, e.g. `https://localhost:9200` |
| `ES_CLOUD_ID` | Optional | Elastic Cloud ID — overrides `ES_HOST` |
| `ES_API_KEY` | Optional* | API key string (base64 `id:key`) |
| `ES_USERNAME` | Optional* | Basic auth username |
| `ES_PASSWORD` | Optional* | Basic auth password |

\* Either `ES_API_KEY` **or** both `ES_USERNAME` + `ES_PASSWORD` must be set.
API key takes precedence if both are provided.

### JSON mode

Set `LOG_FORMAT=json` to get machine-readable output — useful for k8s jobs, Temporal
workflows, or piping into other tools:

```bash
# Operational log records → stderr; full report → stdout
LOG_FORMAT=json python ilm_review.py > report.json 2> ops.jsonl
```

The JSON report contains `summary`, `policies` (with per-index detail), and
`recommendations` (per-data-stream rotation analysis and suggested config
changes). Progress and error messages are emitted as individual JSON records
to stderr.

---

## Running tests

```bash
# With the venv already active (direnv):
pytest

# Or explicitly via uv:
uv run --group dev pytest
```

Tests use static mock data shaped after real Elasticsearch 7.x and 8.x API
responses. No running cluster is required.

---

## Dependency pinning (`uv.lock`)

`uv.lock` is committed intentionally. This is an ops tool — an application,
not a library — so pinning all transitive dependencies is the right default:

- **Reproducibility**: every clone installs byte-for-byte identical packages.
  No silent breakage from an upstream release.
- **Security**: the lockfile is auditable and diffs clearly when deps change,
  making supply-chain review straightforward.
- **Offline use**: ops tooling often runs in restricted environments. A
  committed lockfile lets `uv sync` work from a local cache without hitting
  PyPI.

When you bump a dependency in `pyproject.toml`, regenerate and commit the
lockfile in the same commit so they never diverge:

```bash
uv lock
git add pyproject.toml uv.lock
git commit -m "chore: bump <package> to x.y.z"
```

---

## Adding a new tool

1. Drop your script at the repo root: `my_tool.py`
2. Call `configure_logging()` at the top of `main()` and use a named logger for all progress/error messages
3. Add any new dependencies to `[project.dependencies]` in `pyproject.toml`
4. Run `uv lock && uv sync` (or re-enter the directory so direnv triggers sync)
5. Add tests under `tests/`

Shared dependencies (`rich`, `elasticsearch`, `slopbox.client`, `slopbox.formatting`,
`slopbox.logging`) are already available without any extra steps.
