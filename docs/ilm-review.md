# `ilm_review.py` — ILM Policy Reviewer

Inventories all ILM-managed indices grouped by policy, profiles the rotation
cadence of each data stream, and emits per-data-stream recommendations to tune
`max_primary_shard_size` and `number_of_shards` in ILM policies and index
templates. Targets a rotation cadence of 6–24 hours per index.

---

## API calls

Makes exactly **5 API calls** per run:

| Call | Purpose |
|------|---------|
| `GET /_ilm/policy` | Fetch all ILM policies and their rollover thresholds |
| `GET /*/_ilm/explain?only_managed=true` | Fetch lifecycle state for every managed index |
| `GET /_cat/indices?bytes=b` | Fetch current doc counts, store sizes, shard counts, and creation timestamps |
| `GET /_cat/nodes?h=node.role` | Count data-role nodes for shard spread calculations |
| `GET /_data_stream/*` | Fetch template names and backing index lists per data stream |

---

## Running

```bash
# Human mode (default):
python ilm_review.py

# JSON mode — capture report separately from operational logs:
LOG_FORMAT=json python ilm_review.py > report.json 2> ops.jsonl
```

---

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `LOG_FORMAT` | Optional | `human` (default) for Rich terminal output; `json` for machine-readable output |
| `ES_HOST` | Yes (unless `ES_CLOUD_ID`) | Full cluster URL, e.g. `https://localhost:9200` |
| `ES_CLOUD_ID` | Optional | Elastic Cloud ID — overrides `ES_HOST` |
| `ES_API_KEY` | Optional* | API key string (base64 `id:key`) |
| `ES_USERNAME` | Optional* | Basic auth username |
| `ES_PASSWORD` | Optional* | Basic auth password |

\* Either `ES_API_KEY` **or** both `ES_USERNAME` + `ES_PASSWORD` must be set.
API key takes precedence if both are provided.

---

## Sample output

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

---

## How recommendations work

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

## Required Elasticsearch privileges

- `monitor` cluster privilege (for `_cat/indices` and `_ilm/explain`)
- `manage_ilm` or `read_ilm` cluster privilege (for `_ilm/policy`)
