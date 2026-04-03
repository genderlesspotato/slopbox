# CLAUDE.md — AI Assistant Guide for slopbox

## Project Overview

**slopbox** is a collection of standalone Elasticsearch and ops tooling. Each tool is a self-contained Python script sharing a single `uv`-managed project environment.

**Current tools:**

| Script | Purpose |
|--------|---------|
| `ilm_review.py` | Profiles all ILM-managed indices grouped by policy; shows rollover criteria, phase, age, doc count, and size |

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

| Variable | Required | Notes |
|----------|----------|-------|
| `ES_HOST` | Yes (unless `ES_CLOUD_ID`) | Full URL, e.g. `https://localhost:9200` |
| `ES_CLOUD_ID` | Optional | Elastic Cloud ID — overrides `ES_HOST` |
| `ES_API_KEY` | Optional* | base64 `id:key` string; takes precedence over basic auth |
| `ES_USERNAME` | Optional* | Basic auth username |
| `ES_PASSWORD` | Optional* | Basic auth password |

\* Either `ES_API_KEY` or both `ES_USERNAME`+`ES_PASSWORD` must be set.

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
| `slopbox/formatting.py` | Shared formatting utilities: `format_bytes`, `format_duration`, `health_style` |
| `slopbox/client.py` | Shared Elasticsearch client factory: `build_client()` |
| `tests/test_ilm_review.py` | Unit tests for `ilm_review.py` |
| `tests/test_formatting.py` | Unit tests for `slopbox.formatting` |
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
| `slopbox/formatting.py` | `format_bytes`, `format_duration`, `health_style` |
| `slopbox/client.py` | `build_client()` — env var validation + Elasticsearch client construction |

Import them directly in any tool:

```python
from slopbox.client import build_client
from slopbox.formatting import format_bytes, format_duration, health_style
```

## Architecture of `ilm_review.py`

The tool makes **exactly 3 API calls** per run (design constraint — keep it that way):

```
fetch_ilm_policies()     →  GET /_ilm/policy
fetch_ilm_explain()      →  GET /*/_ilm/explain?only_managed=true
fetch_cat_indices()      →  GET /_cat/indices?bytes=b
```

Data flows through a clear pipeline:

```
fetch → parse → correlate → render
```

Key components:

| Component | Role |
|-----------|------|
| `IndexProfile` dataclass | Per-index data model |
| `phase_style()` | ILM-phase → Rich colour mapping (hot/warm/cold/frozen/delete) |
| Fetch functions | Three thin API wrappers |
| `parse_rollover_criteria`, `parse_all_policies` | ILM policy parsing |
| `correlate_data()` | Joins ILM explain + cat stats, calculates ages, groups by policy |
| `render_report()` | Builds and prints Rich table output |
| `main()` | Orchestrates everything, top-level error handling |

Both ES 7.x and 8.x response shapes are supported throughout.

---

## Code Conventions

### Naming
- Functions: `snake_case`
- Classes/dataclasses: `PascalCase`
- Environment variables: `SCREAMING_SNAKE_CASE`
- Constants: inline dicts (e.g., phase → color mappings)

### Style
- **Full type annotations** on all functions and dataclass fields
- **Section-header comment blocks** (60-char separator lines) group related functions — maintain this pattern
- **Pure functions** with minimal side effects; no global mutable state
- **Rich** for all terminal output: `Console`, `Table`, `box.SIMPLE_HEAD`, `console.rule()`, `console.status()`

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

Shared dependencies (`rich`, `elasticsearch`) and `slopbox.*` utilities are available without extra steps.

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
