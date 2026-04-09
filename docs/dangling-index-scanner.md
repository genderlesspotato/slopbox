# `dangling_index_scanner.py` — Dangling Index Scanner

Scans an Elasticsearch data node's `indices/` directory for UUID directories
that the cluster no longer recognises, **quarantines** them safely, and **reaps**
quarantine entries after a second grace period. Default mode is dry-run.

---

## API calls

Makes **2 API calls** per scan cycle:

| Call | Purpose |
|------|---------|
| `GET /_cluster/state/metadata` | Fetch all live index UUIDs and graveyard tombstones |
| `GET /_cat/recovery?active_only=true` | Check for in-progress shard recoveries (belt-and-suspenders) |

Plus one additional cluster state fetch per aged quarantine entry during the reap pass.

---

## Safety checks

A directory is a dangling candidate only when **all** of the following hold:

1. Name matches ES UUID pattern (alphanumeric + `_-`, ≥ 10 chars)
2. No `_state` subdirectory (rules out in-flight allocations)
3. UUID not in live cluster state
4. UUID not in index graveyard
5. UUID not targeted by an active shard recovery
6. ctime older than `ORPHAN_AGE_HOURS`
7. ES JVM holds no open file descriptors under the directory (Linux/`/proc` only)

If the cluster state or recovery API cannot be fetched, the entire scan cycle is
aborted — the tool always fails closed on uncertainty.

---

## Reclamation pipeline

Reclamation is two-phase:

1. **Quarantine** — atomic `rename(2)` of the candidate directory into `.quarantine/` (named `<uuid>__<epoch_seconds>`).
2. **Reap** — permanent deletion once the quarantine entry is old enough _and_ a fresh cluster state re-confirms the UUID is still absent. Any UUID that reappears in the cluster state is restored out of quarantine.

---

## Running

```bash
# Dry-run single scan (safe default):
ES_DATA_PATH=/var/lib/elasticsearch/data python dangling_index_scanner.py

# JSON mode — capture report separately from logs:
LOG_FORMAT=json ES_DATA_PATH=... python dangling_index_scanner.py > report.json 2> ops.jsonl

# Enable mutations only after validating dry-run output:
DRY_RUN=false ES_DATA_PATH=... python dangling_index_scanner.py

# Daemon mode (loop forever):
DAEMON=true DRY_RUN=false ES_DATA_PATH=... python dangling_index_scanner.py
```

---

## Environment variables

**Elasticsearch:**

| Variable | Required | Description |
|----------|----------|-------------|
| `ES_HOST` | Yes (unless `ES_CLOUD_ID`) | Full cluster URL, e.g. `https://localhost:9200` |
| `ES_CLOUD_ID` | Optional | Elastic Cloud ID — overrides `ES_HOST` |
| `ES_API_KEY` | Optional* | API key string (base64 `id:key`) |
| `ES_USERNAME` | Optional* | Basic auth username |
| `ES_PASSWORD` | Optional* | Basic auth password |

**Scanner:**

| Variable | Default | Description |
|----------|---------|-------------|
| `ES_DATA_PATH` | *(required)* | Path to the ES data directory |
| `LOG_FORMAT` | `human` | `human` for Rich terminal output; `json` for machine-readable output |
| `DRY_RUN` | `true` | Set to `false` to enable filesystem mutations |
| `ORPHAN_AGE_HOURS` | `24` | Minimum ctime age before a directory is a candidate |
| `QUARANTINE_GRACE_HOURS` | `48` | Minimum quarantine age before reaping |
| `SCAN_INTERVAL_SECONDS` | `3600` | Sleep between iterations in daemon mode |
| `DAEMON` | `false` | `true` to loop forever; `false` for a single run |

---

## ES data directory layouts

| Layout | Path |
|--------|------|
| ES 8.x | `$ES_DATA_PATH/indices/` |
| ES 7.x | `$ES_DATA_PATH/nodes/0/indices/` |

`find_indices_dir()` tries the 8.x path first.

---

## Required Elasticsearch privileges

- `monitor` cluster privilege (for `_cluster/state` and `_cat/recovery`)
