# CLAUDE.md — AI Assistant Guide for slopbox

## Project Overview

**slopbox** is a collection of standalone Elasticsearch and Kubernetes ops tooling. Each tool is a self-contained Python script sharing a single `uv`-managed project environment, backed by a shared domain modeling layer.

**Current tools:**

| Script | Purpose |
|--------|---------|
| `ilm_review.py` | Inventories ILM-managed indices grouped by policy and emits per-data-stream recommendations to tune ILM policy and index template settings |

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

**Elasticsearch:**

| Variable | Required | Notes |
|----------|----------|-------|
| `ES_HOST` | Yes (unless `ES_CLOUD_ID`) | Full URL, e.g. `https://localhost:9200` |
| `ES_CLOUD_ID` | Optional | Elastic Cloud ID — overrides `ES_HOST` |
| `ES_API_KEY` | Optional* | base64 `id:key` string; takes precedence over basic auth |
| `ES_USERNAME` | Optional* | Basic auth username |
| `ES_PASSWORD` | Optional* | Basic auth password |

\* Either `ES_API_KEY` or both `ES_USERNAME`+`ES_PASSWORD` must be set.

**Kubernetes** (for k8s tools — not yet required):

| Variable | Required | Notes |
|----------|----------|-------|
| `KUBECONFIG` | Optional | Path to kubeconfig file; defaults to `~/.kube/config` |
| `K8S_CONTEXT` | Optional | kubectl context to use; defaults to current context |
| `K8S_NAMESPACE` | Optional | Namespace to target; tool-specific default applies if unset |

---

## Running Tools

```bash
# With direnv active (recommended):
python ilm_review.py

# Without direnv:
uv run python ilm_review.py
```

No CLI arguments — all configuration comes from environment variables.

---

## Running Tests

```bash
# With direnv active:
pytest

# Explicit:
uv run --group dev pytest
```

Tests live in `tests/` and use static mock data shaped after real ES 7.x and 8.x API responses. **No running cluster is required.**

Time-dependent tests patch `ilm_review.time.time` with a fixed timestamp for determinism. Tests for shared utilities live in `tests/test_formatting.py` and `tests/test_client.py`.

---

## Key Files

| File | Role |
|------|------|
| `ilm_review.py` | ILM review tool |
| `slopbox/formatting.py` | Shared formatting utilities: `format_bytes`, `format_duration`, `phase_style`, `health_style` |
| `slopbox/client.py` | Shared Elasticsearch client factory: `build_client()` |
| `domain/es/models.py` | ES domain objects (`IndexProfile`, …) — Pydantic v2 with `@computed_field` display strings |
| `domain/es/types.py` | Raw ES API boundary models (`RawCatIndexEntry`, `RawIlmExplainEntry`, …) — owns all string→int coercion |
| `domain/k8s/models.py` | k8s domain objects (`PodProfile`, …) — constructed via classmethods from k8s client objects |
| `tests/test_ilm_review.py` | ILM tool unit tests — 49+ cases including full profiling suite |
| `tests/test_domain_es.py` | Boundary coercion + domain model tests |
| `tests/test_formatting.py` | Formatter helper tests |
| `tests/test_client.py` | Unit tests for `slopbox.client` |
| `pyproject.toml` | Project metadata, dependencies, pytest config |
| `uv.lock` | Pinned dependency tree (committed intentionally) |
| `.envrc` | direnv: loads `.env`, activates uv venv via `layout uv` |
| `.env.example` | Credential template — copy to `.env`, never commit `.env` |

---

## Shared library (`slopbox/`)

Utilities shared across tools live in the `slopbox/` package:

| Module | Contents |
|--------|----------|
| `slopbox/formatting.py` | `format_bytes`, `format_duration`, `phase_style`, `health_style` |
| `slopbox/client.py` | `build_client()` — env var validation + Elasticsearch client construction |

Import them directly in any tool:

```python
from slopbox.client import build_client
from slopbox.formatting import format_bytes, format_duration, phase_style, health_style
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
| `render_report()` | Builds and prints inventory table + recommendations table |
| `main()` | Orchestrates everything, top-level error handling |

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
- **Rich** for all terminal output: `Console`, `Table`, `box.SIMPLE_HEAD`, `console.rule()`, `console.status()`

### Domain Modeling
- **Pydantic v2** for all domain objects (`BaseModel`, `ConfigDict(frozen=True)`)
- **Raw boundary types** (`domain/es/types.py`) own all API coercion — never coerce in tool scripts
- **`@computed_field`** for display strings derived from raw numeric fields
- **`model_validate()`** to construct boundary models from API response dicts

### Error Handling
- Catch specific exceptions: `AuthenticationException`, `ConnectionError`, `TransportError`
- Call `sys.exit()` with a clear user-facing message; don't re-raise
- Validate only at system boundaries (env var parsing, API responses) — trust internal code

### Testing
- Use `@pytest.mark.parametrize` for edge cases
- Mock Elasticsearch responses as realistic dicts matching actual API shapes
- Patch `ilm_review.time.time` for any time-dependent logic
- Test names should be descriptive: `test_correlate_data_missing_cat_entry_increments_skipped`

---

## Adding a New Tool

1. Drop the script at the repo root: `my_tool.py`
2. Import shared utilities from `slopbox.*` as needed (client, formatting)
3. Add any new dependencies to `[project.dependencies]` in `pyproject.toml`
4. Run `uv lock && uv sync` (or re-enter the directory so direnv triggers sync)
5. Add tests under `tests/`; extract new shared utilities to `slopbox/` if they'll be reused

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
- **Documentation** — update `CLAUDE.md` and any relevant docstrings
- **`pyproject.toml` / `uv.lock`** — update together whenever dependencies change

### Correctness and reliability over performance
These are ops tools that operators depend on for production decisions. Correctness and reliability are the top priorities. Performance is still relevant — tools run against large clusters with thousands of indices — but never sacrifice correctness for speed. Prefer clear, verifiable logic; optimise only where there is a demonstrated bottleneck.
