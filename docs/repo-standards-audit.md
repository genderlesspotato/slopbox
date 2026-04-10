# Repo Standards Audit — 2026-04-10

Findings from a scan of the codebase against the standards declared in `CLAUDE.md`.
Items are ordered roughly by impact. Each one is a self-contained unit of work.

---

## CLAUDE.md accuracy (stale documentation)

### 1. Rewrite all file-path references to match the uv workspace layout

Every path in `CLAUDE.md` points to the repo root (e.g. `ilm_review.py`, `slopbox/`,
`domain/`) but the actual layout is a `uv` workspace:

```
packages/tools/ilm_review.py
packages/tools/dangling_index_scanner.py
packages/tools/k8s_inventory.py
packages/tools/slopbox/
packages/domain/slopbox_domain/
packages/temporal-workflows/
```

Sections to update: Key Files table, all three tool Architecture sections, import
examples, and the "Adding a New Tool" guide (currently tells developers to drop
scripts at the repo root).

### 2. Add a Temporal Workflows section

`packages/temporal-workflows/slopbox_temporal` (including the `kibana_export`
workflow) exists and is documented in `docs/kibana-log-export.md`, but `CLAUDE.md`
makes zero mention of it. A developer reading `CLAUDE.md` alone would never know it
exists. Add a section covering its purpose, structure, and how it fits alongside the
three existing tools.

### 3. Document the three undiscovered domain modules

The following modules are not listed anywhere in `CLAUDE.md`:

- `packages/domain/slopbox_domain/es/cluster.py`
- `packages/domain/slopbox_domain/k8s/cluster.py`
- `packages/domain/slopbox_domain/registry.py`

Read each file and add them to the Key Files table and the Domain Modeling
Architecture section.

### 4. Correct test file list and stale test counts

Three test files exist that are not listed in `CLAUDE.md`:

- `tests/test_es_version.py` — 19 tests for `ClusterVersion` capability predicates
- `tests/test_cluster_registry.py` — cluster registry config
- `tests/test_kibana_export.py` — 4 tests for the kibana export workflow

Two listed counts are also stale:

| File | CLAUDE.md says | Actual |
|------|---------------|--------|
| `test_ilm_review.py` | 58+ | 41 |
| `test_k8s_inventory.py` | 39 | 38 |

### 5. Fix the separator line length spec

`CLAUDE.md` says "60-char separator lines" but every file in the repo uses
75-hyphen separators. Since 75 is universal practice, update the spec to match the
code rather than the other way around.

### 6. Add a `docs/` directory reference to the Key Files table

`docs/dangling-index-scanner.md`, `docs/ilm-review.md`, `docs/k8s-inventory.md`,
and `docs/kibana-log-export.md` exist but are invisible to anyone reading `CLAUDE.md`.
Add a row to the Key Files table pointing to `docs/` with a note that each tool has
a dedicated doc page there.

---

## Code fixes

### 7. Add `frozen=True` to all raw boundary type classes in `slopbox_domain/es/types.py`

`CLAUDE.md` mandates `ConfigDict(frozen=True)` for all Pydantic models. All six raw
boundary type classes are missing it:

| Class | Line |
|-------|------|
| `RawPhaseExecution` | ~21 |
| `RawIlmExplainEntry` | ~29 |
| `RawCatIndexEntry` | ~43 |
| `RawDataStreamIndex` | ~64 |
| `RawDataStream` | ~71 |
| `RawCatRecoveryEntry` | ~85 |

File: `packages/domain/slopbox_domain/es/types.py`

Verify the test suite still passes after the change — frozen models raise
`ValidationError` on mutation and may surface latent issues.

### 8. Add `console.status()` spinners to `k8s_inventory.py`

The tool has no `Console` object and uses `logger.info()` for all fetch progress
regardless of output mode. `CLAUDE.md` requires `console.status()` spinners for
fetch operations in human mode.

Follow the pattern in `ilm_review.py`:

```python
console = Console() if log_format == "human" else None
```

Thread `console` through to scan/fetch functions and wrap fetch calls in
`console.status()` in human mode. The parallel-per-cluster `ThreadPoolExecutor`
architecture needs thought for how spinners should behave under concurrency.

File: `packages/tools/k8s_inventory.py`

### 9. Add `console.status()` spinners to `dangling_index_scanner.py` fetch phase

The fetch calls in `run_scan()` (~lines 487, 497, 512, 517) use `logger.info()`
unconditionally. A `console` object is created in `main()` but not passed to
`run_scan()`. Thread it through and use `console.status()` for fetch steps in human
mode.

File: `packages/tools/dangling_index_scanner.py`

### 10. Fix the loose `object` type annotation on `_v1deployment_to_raw()`

The parameter should be typed as `V1Deployment` from the kubernetes client library.
Confirm the import is already present before changing.

File: `packages/tools/k8s_inventory.py` ~line 96

### 11. Resolve the broad `except Exception` in the k8s parallel scan

`k8s_inventory.py` ~line 444 catches bare `Exception` to prevent one cluster failure
from aborting the whole parallel scan. `CLAUDE.md` says to catch specific exceptions.

Options:
- Narrow the catch to concrete k8s client exceptions (e.g. `ApiException`,
  `ConfigException`) and let genuinely unexpected errors propagate.
- Or add an explicit carve-out note in `CLAUDE.md` for parallel scanner patterns
  where broad catching is intentional.

File: `packages/tools/k8s_inventory.py` ~line 444

---

## Test quality

### 12. Add `caplog`-based log output assertions to `test_logging.py`

The existing 22 tests only introspect handler and formatter types — none verify actual
log output. `CLAUDE.md` says to use `caplog` to assert log messages.

Add tests that call `logger.info()` / `logger.error()` through a configured handler
and assert on the captured records, covering both modes:

- Human mode: message captured at the right level
- JSON mode: formatter emits valid JSON with `timestamp`, `level`, `logger`,
  `message` fields

File: `packages/tools/tests/test_logging.py`

---

## Configuration

### 13. Expand `.env.example` with the missing documented env vars

The following vars are documented in `CLAUDE.md` but absent from `.env.example`:

| Variable | Section |
|----------|---------|
| `LOG_FORMAT` | Output |
| `DRY_RUN` | Output |
| `KUBECONFIG` | Kubernetes |
| `K8S_CONTEXT` | Kubernetes |
| `K8S_NAMESPACE` | Kubernetes |
| `INVENTORY_DIR` | Kubernetes |

File: `.env.example`
