# slopbox

A collection of small Elasticsearch and ops tooling. Each tool is a standalone
Python script sharing a common project environment.

---

## Tools

### `ilm_review.py` — ILM Policy Reviewer

Profiles all ILM-managed indices in a cluster. Groups them by policy and shows
rollover criteria alongside current phase, age, document count, and store size.
Useful for spotting indices that are rolling over too frequently or not often
enough.

Makes exactly **3 API calls**:

| Call | Purpose |
|------|---------|
| `GET /_ilm/policy` | Fetch all ILM policies and their rollover thresholds |
| `GET /*/_ilm/explain?only_managed=true` | Fetch lifecycle state for every managed index |
| `GET /_cat/indices?bytes=b` | Fetch current doc counts, store sizes, and creation timestamps |

#### Sample output

```
─────────────────────── Elasticsearch ILM Policy Review ───────────────────────
  Generated: 2026-04-03 14:22 UTC  |  Policies: 2  |  Managed indices: 47

──────────────────── logs-default-policy  12 indices ───────────────────────────
  Phases: hot → warm → cold → delete
  Rollover criteria: max_age=7d  max_size=50gb

 Index                   Phase   Index Age   Phase Age   Docs           Size        Health
 ──────────────────────────────────────────────────────────────────────────────────────────
 logs-app-000023         hot     2d 4h       2d 4h       12,450,221     8.3 GB      green
 logs-app-000022         warm    9d 1h       2d 1h       198,332,100    41.9 GB     green
 logs-app-000021         cold    16d 3h      7d 0h       200,001,441    42.8 GB     green

  Summary: 3 indices  |  Docs: 410,783,762  |  Size: 93.0 GB
```

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
| `ES_HOST` | Yes (unless `ES_CLOUD_ID`) | Full cluster URL, e.g. `https://localhost:9200` |
| `ES_CLOUD_ID` | Optional | Elastic Cloud ID — overrides `ES_HOST` |
| `ES_API_KEY` | Optional* | API key string (base64 `id:key`) |
| `ES_USERNAME` | Optional* | Basic auth username |
| `ES_PASSWORD` | Optional* | Basic auth password |

\* Either `ES_API_KEY` **or** both `ES_USERNAME` + `ES_PASSWORD` must be set.
API key takes precedence if both are provided.

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

## Adding a new tool

1. Drop your script at the repo root: `my_tool.py`
2. Add any new dependencies to `[project.dependencies]` in `pyproject.toml`
3. Run `uv sync` (or re-enter the directory so direnv triggers it)
4. Add tests under `tests/`

Shared dependencies (e.g. `rich`, `elasticsearch`) are already available to
all scripts without any extra steps.
