# `kibana_export` — Kibana Log Export Workflow

A [Temporal](https://temporal.io/) workflow that replaces manual Kibana CSV
exports with a chunked, crash-safe pipeline: ES PIT pagination → gzip NDJSON
chunks → S3, with `manifest.json` + `README.txt` on completion.

Package: `packages/temporal-workflows/`
Workflow class: `slopbox_temporal.kibana_export.workflow.KibanaLogExportWorkflow`

---

## What it does

1. **Validate** the export request — time range ≤ 14 days, doc count ≤ 250,000.
2. **Resolve** the index pattern to concrete index names (alphabetical = chronological).
3. For each index, **open a PIT** then **drain** it with `search_after` pagination,
   writing each page as a gzip-compressed NDJSON chunk to S3.
4. **Close the PIT** after each index (`try/finally`).
5. Write `manifest.json` and `README.txt` to the S3 prefix.

On any failure or cancellation the workflow calls `cleanup_partial_export` via
`asyncio.shield` to delete written chunk objects before re-raising — no orphan
files left in S3.

---

## Activities

| Activity | Purpose |
|----------|---------|
| `validate_export_request` | Count matching docs; reject oversized requests |
| `resolve_indices` | Expand index pattern to concrete index names |
| `open_pit` | Open a Point-in-Time on a single index |
| `export_chunk` | Fetch one page via PIT + `search_after`, write gzip NDJSON to S3 |
| `close_pit` | Release the PIT (best-effort; PITs also expire naturally) |
| `write_manifest` | Write `manifest.json` + `README.txt` to the S3 prefix |
| `cleanup_partial_export` | Batch-delete orphan S3 objects on failure/cancellation |

---

## Limits

| Limit | Value |
|-------|-------|
| Maximum time range | 14 days |
| Maximum document count | 250,000 |
| Default chunk size | 10,000 docs |
| PIT keep-alive | 5 minutes |
| Activity retry attempts | 3 (exponential back-off: 5 s → 30 s) |

---

## S3 output layout

```
{s3_prefix}
├── chunk-0000.ndjson.gz
├── chunk-0001.ndjson.gz
├── ...
├── manifest.json
└── README.txt
```

**`manifest.json`** contains the original request parameters, resolved index
list, total doc count, and a per-chunk entry listing the S3 key and doc count.

**`README.txt`** contains macOS `zcat` / AWS CLI concatenation instructions for
reassembling the chunks locally.

---

## Request model

```python
from datetime import datetime, timezone
from slopbox_temporal.kibana_export.models import KibanaLogExportRequest, TimeRange

request = KibanaLogExportRequest(
    index="logs-app-*",
    query={"match": {"level": "error"}},   # ES Query DSL; {} for match_all
    time_range=TimeRange(
        start=datetime(2026, 4, 1, tzinfo=timezone.utc),
        end=datetime(2026, 4, 7, tzinfo=timezone.utc),
    ),
    s3_bucket="my-exports-bucket",
    s3_prefix="exports/2026/run-abc/",     # trailing slash required
    chunk_size=10_000,                      # optional; default 10 000
)
```

---

## Environment variables

**Elasticsearch** (read by each activity at runtime — no shared client):

| Variable | Required | Description |
|----------|----------|-------------|
| `ES_HOST` | Yes (unless `ES_CLOUD_ID`) | Full cluster URL, e.g. `https://localhost:9200` |
| `ES_CLOUD_ID` | Optional | Elastic Cloud ID — overrides `ES_HOST` |
| `ES_API_KEY` | Optional* | API key string (base64 `id:key`) |
| `ES_USERNAME` | Optional* | Basic auth username |
| `ES_PASSWORD` | Optional* | Basic auth password |

**AWS** (standard boto3 credential chain):

| Variable | Description |
|----------|-------------|
| `AWS_ACCESS_KEY_ID` | AWS access key (or use IAM role / instance profile) |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key |
| `AWS_DEFAULT_REGION` | AWS region for S3 |

---

## Error handling

Temporal's built-in retry handles transient failures (network blips, 5xx).
The following are marked `non_retryable` so Temporal doesn't waste retries:

| Error | Cause |
|-------|-------|
| `AuthenticationException` | Bad ES credentials (config problem) |
| `NotFoundError` | Index disappeared between resolve and open_pit |
| `NoCredentialsError` | boto3 can't find AWS credentials (config problem) |

---

## Dependency

The workflow package depends on `slopbox-domain` (shared typed models) but
intentionally does **not** depend on `slopbox-tools` (kubernetes, rich) to keep
the worker image lean.

```toml
# packages/temporal-workflows/pyproject.toml
dependencies = [
    "temporalio",
    "slopbox-domain",
    "elasticsearch>=8.0.0",
    "boto3>=1.26.0",
]
```
