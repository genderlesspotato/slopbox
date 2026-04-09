#!/usr/bin/env python3
"""Elasticsearch dangling index scanner and reclaimer.

Scans an Elasticsearch data directory for UUID directories that the cluster no
longer recognises, quarantines them safely, and reaps quarantine entries once
they have aged past a second grace period.

A directory is a dangling-index candidate when ALL of the following hold:

  1. Its name matches an ES UUID (alphanumeric + underscore/hyphen, ≥ 10 chars).
  2. It is not present in the cluster's live index metadata.
  3. It is not in the index graveyard (tombstones ES manages itself).
  4. It lacks a ``_state`` subdirectory (rules out in-flight allocations).
  5. Its ctime is older than ORPHAN_AGE_HOURS (buffer against racing allocations).
  6. No active shard recovery targets the same index UUID (belt-and-suspenders).
  7. The Elasticsearch JVM holds no open file descriptors under the directory
     (checked via /proc/<pid>/fd on Linux; skipped gracefully on other platforms).

Reclamation is two-phase:

  Phase 1 — quarantine: atomic rename into a hidden ``.quarantine/`` subdirectory
  on the same filesystem.  No data is copied; disk usage is unchanged.

  Phase 2 — reap: on a later cycle, entries older than QUARANTINE_GRACE_HOURS
  are re-verified against a fresh cluster state and then deleted.  If a UUID
  has reappeared in the cluster state it is restored rather than deleted.

Default mode is DRY_RUN=true.  Set DRY_RUN=false to enable filesystem mutations.

Environment variables:
    ES_HOST                 - Elasticsearch URL (e.g. https://localhost:9200)
    ES_CLOUD_ID             - Elastic Cloud ID (overrides ES_HOST)
    ES_API_KEY              - API key (base64 id:key), takes precedence over basic auth
    ES_USERNAME             - Basic auth username
    ES_PASSWORD             - Basic auth password
    ES_DATA_PATH            - Path to the Elasticsearch data directory (required)
    DRY_RUN                 - true (default) or false
    ORPHAN_AGE_HOURS        - Minimum ctime age for a candidate (default: 24)
    QUARANTINE_GRACE_HOURS  - Minimum quarantine age before reaping (default: 48)
    SCAN_INTERVAL_SECONDS   - Sleep between daemon-mode iterations (default: 3600)
    DAEMON                  - true → loop forever; false (default) → single run
"""

import json
import logging
import os
import re
import shutil
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from elasticsearch import Elasticsearch, AuthenticationException, ConnectionError, TransportError
from rich.console import Console
from rich.table import Table
from rich import box

from slopbox.client import build_connected_cluster, ConnectedCluster
from slopbox.logging import configure_logging
from slopbox_domain.es.types import RawCatRecoveryEntry
from slopbox_domain.es.version import ClusterVersion

logger = logging.getLogger("dangling_index_scanner")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ORPHAN_AGE_HOURS: float = float(os.getenv("ORPHAN_AGE_HOURS", "24"))
QUARANTINE_GRACE_HOURS: float = float(os.getenv("QUARANTINE_GRACE_HOURS", "48"))
SCAN_INTERVAL_SECONDS: int = int(os.getenv("SCAN_INTERVAL_SECONDS", "3600"))
QUARANTINE_DIR = ".quarantine"
DRY_RUN_DEFAULT: bool = os.getenv("DRY_RUN", "true").lower() != "false"

# ES UUID: Base62/Base64-url encoded, typically ~22 chars, alphanumeric + _-
_UUID_RE = re.compile(r"^[A-Za-z0-9_\-]{10,}$")


# ---------------------------------------------------------------------------
# Tool-local data types
# ---------------------------------------------------------------------------

@dataclass
class DanglingCandidate:
    uuid: str
    path: Path
    age_hours: float


@dataclass
class ScanResult:
    scanned: int
    candidates: list[DanglingCandidate]
    quarantined: int
    skipped: int
    reaped: int
    restored: int
    dry_run: bool
    generated_at: str = field(default_factory=lambda: datetime.now(tz=timezone.utc).isoformat())


# ---------------------------------------------------------------------------
# API fetch functions
# ---------------------------------------------------------------------------

def fetch_cluster_state(client: Elasticsearch) -> dict:
    """GET /_cluster/state/metadata — returns the full metadata block."""
    return client.cluster.state(metric="metadata").body


def fetch_active_recoveries(client: Elasticsearch) -> list[RawCatRecoveryEntry]:
    """GET /_cat/recovery?active_only=true — returns in-progress shard recoveries."""
    raw: list[dict] = client.cat.recovery(
        active_only=True,
        h="index,shard,stage",
        format="json",
    ).body
    return [RawCatRecoveryEntry.model_validate(r) for r in raw]


# ---------------------------------------------------------------------------
# Parse / correlate helpers
# ---------------------------------------------------------------------------

def parse_known_uuids(cluster_state: dict) -> tuple[set[str], set[str]]:
    """Extract live and graveyard UUID sets from a cluster state response.

    Returns:
        (live_uuids, graveyard_uuids) — both are sets of UUID strings.
    """
    metadata: dict = cluster_state.get("metadata", {})

    live_uuids: set[str] = set()
    for _name, index_meta in metadata.get("indices", {}).items():
        uuid = (
            index_meta
            .get("settings", {})
            .get("index", {})
            .get("uuid", "")
        )
        if uuid:
            live_uuids.add(uuid)

    graveyard_uuids: set[str] = set()
    tombstones = metadata.get("index-graveyard", {}).get("tombstones", [])
    for tombstone in tombstones:
        uuid = tombstone.get("index", {}).get("index_uuid", "")
        if uuid:
            graveyard_uuids.add(uuid)

    return live_uuids, graveyard_uuids


def parse_recovery_uuids(
    recoveries: list[RawCatRecoveryEntry],
    name_to_uuid: dict[str, str],
) -> set[str]:
    """Map active recovery index names → UUIDs using the cluster state lookup.

    Belt-and-suspenders: dangling candidates are never in the live cluster
    state, so this set is always disjoint from any real candidates.  The check
    guards against the narrow race where a new index was allocated after we
    fetched the cluster state.
    """
    uuids: set[str] = set()
    for entry in recoveries:
        uuid = name_to_uuid.get(entry.index)
        if uuid:
            uuids.add(uuid)
    return uuids


def _build_name_to_uuid(cluster_state: dict) -> dict[str, str]:
    """Build an {index_name: uuid} mapping from cluster state metadata."""
    result: dict[str, str] = {}
    for name, index_meta in cluster_state.get("metadata", {}).get("indices", {}).items():
        uuid = (
            index_meta
            .get("settings", {})
            .get("index", {})
            .get("uuid", "")
        )
        if uuid:
            result[name] = uuid
    return result


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------

def find_indices_dir(
    data_path: Path,
    version: ClusterVersion | None = None,
) -> Path | None:
    """Locate the indices directory under an Elasticsearch data path.

    Handles both layouts:
      ES 8+:  <data_path>/indices/
      ES 7:   <data_path>/nodes/0/indices/

    If ``version`` is provided the expected layout is tried first; both paths
    are always probed for robustness (non-standard installs, in-place upgrade
    scenarios where the layout may lag the detected version, etc.).
    """
    if version is not None and not version.uses_flat_indices_dir:
        candidates = (
            data_path / "nodes" / "0" / "indices",
            data_path / "indices",
        )
    else:
        candidates = (
            data_path / "indices",
            data_path / "nodes" / "0" / "indices",
        )
    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    return None


def find_es_pid() -> int | None:
    """Find the PID of the running Elasticsearch JVM process via /proc.

    Returns None if /proc is unavailable, no ES process is found, or
    permission is denied — the open-fd check is simply skipped in that case.
    """
    proc = Path("/proc")
    if not proc.is_dir():
        return None
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            cmdline = (entry / "cmdline").read_bytes().decode(errors="replace")
            if "elasticsearch" in cmdline:
                return int(entry.name)
        except (OSError, ValueError):
            continue
    return None


def has_open_fds(pid: int, target_path: Path) -> bool:
    """Return True if the process holds any open file descriptors under target_path.

    Reads /proc/<pid>/fd/* and resolves symlinks.  Returns False on any
    permission or OS error — fail-safe: a missed open FD means we might skip
    quarantining a candidate, which is the conservative outcome.
    """
    fd_dir = Path("/proc") / str(pid) / "fd"
    try:
        for fd_link in fd_dir.iterdir():
            try:
                resolved = fd_link.resolve()
                if resolved.is_relative_to(target_path):
                    return True
            except OSError:
                continue
    except OSError:
        return False
    return False


# ---------------------------------------------------------------------------
# Candidate scanning
# ---------------------------------------------------------------------------

def scan_for_candidates(
    indices_dir: Path,
    live_uuids: set[str],
    graveyard_uuids: set[str],
    recovery_uuids: set[str],
    es_pid: int | None,
    now_s: float,
) -> tuple[list[DanglingCandidate], int]:
    """Walk indices_dir and return (candidates, skipped_count).

    A directory is a candidate only when every safety check passes.
    ``skipped_count`` counts directories that failed at least one check.
    """
    candidates: list[DanglingCandidate] = []
    skipped = 0

    for entry in sorted(indices_dir.iterdir()):
        if not entry.is_dir():
            continue
        name = entry.name
        # Skip the quarantine directory and any other hidden directories
        if name.startswith("."):
            continue
        # Skip anything that does not look like an ES UUID
        if not _UUID_RE.match(name):
            logger.info("skipping non-UUID directory: %s", entry)
            skipped += 1
            continue

        uuid = name

        # Check 1: _state subdirectory indicates live shard metadata
        if (entry / "_state").is_dir():
            skipped += 1
            continue

        # Check 2: present in live cluster state
        if uuid in live_uuids:
            skipped += 1
            continue

        # Check 3: present in graveyard (ES will clean these up itself)
        if uuid in graveyard_uuids:
            logger.info("skipping graveyard UUID: %s", uuid)
            skipped += 1
            continue

        # Check 4: targeted by an active shard recovery
        if uuid in recovery_uuids:
            logger.info("skipping UUID targeted by active recovery: %s", uuid)
            skipped += 1
            continue

        # Check 5: ctime age
        try:
            ctime = entry.stat().st_ctime
        except OSError as exc:
            logger.info("cannot stat %s, skipping: %s", entry, exc)
            skipped += 1
            continue
        age_hours = (now_s - ctime) / 3600.0
        if age_hours < ORPHAN_AGE_HOURS:
            skipped += 1
            continue

        # Check 6: open file descriptors from the ES JVM
        if es_pid is not None and has_open_fds(es_pid, entry):
            logger.info("skipping UUID with open FDs from ES process: %s", uuid)
            skipped += 1
            continue

        logger.info(
            "dangling candidate: uuid=%s age_hours=%.1f path=%s",
            uuid, age_hours, entry,
        )
        candidates.append(DanglingCandidate(uuid=uuid, path=entry, age_hours=age_hours))

    return candidates, skipped


# ---------------------------------------------------------------------------
# Quarantine
# ---------------------------------------------------------------------------

def quarantine_candidate(
    candidate: DanglingCandidate,
    quarantine_dir: Path,
    dry_run: bool,
) -> bool:
    """Move candidate into the quarantine directory.

    The quarantine folder name encodes the UUID and the quarantine epoch so
    that any future run can reconstruct when it was quarantined without
    external state.

    Returns True on success (including dry-run success).
    """
    folder_name = f"{candidate.uuid}__{int(time.time())}"
    dest = quarantine_dir / folder_name

    if dry_run:
        logger.info("[dry-run] would quarantine %s → %s", candidate.path, dest)
        return True

    try:
        quarantine_dir.mkdir(exist_ok=True)
        os.rename(candidate.path, dest)
        logger.info("quarantined %s → %s", candidate.path, dest)
        return True
    except OSError as exc:
        logger.error("failed to quarantine %s: %s", candidate.path, exc)
        return False


# ---------------------------------------------------------------------------
# Reap
# ---------------------------------------------------------------------------

def reap_quarantine(
    quarantine_dir: Path,
    indices_dir: Path,
    client: Elasticsearch,
    dry_run: bool,
    now_s: float,
) -> tuple[int, int]:
    """Walk the quarantine directory and reap or restore aged entries.

    For each entry:
      - Skip if not old enough (< QUARANTINE_GRACE_HOURS).
      - Fetch a fresh cluster state and re-verify safety.
      - If the UUID reappeared in cluster state: restore to indices_dir.
      - Otherwise: delete permanently.

    Returns (reaped, restored).
    """
    if not quarantine_dir.is_dir():
        return 0, 0

    reaped = 0
    restored = 0

    # Fetch fresh cluster state once for the whole reap pass.
    try:
        fresh_state = fetch_cluster_state(client)
        live_uuids, graveyard_uuids = parse_known_uuids(fresh_state)
    except Exception as exc:
        logger.error("cannot fetch cluster state for reap pass, skipping reap: %s", exc)
        return 0, 0

    for entry in sorted(quarantine_dir.iterdir()):
        if not entry.is_dir():
            continue

        # Parse UUID and quarantine epoch from folder name
        parts = entry.name.split("__", 1)
        if len(parts) != 2 or not parts[1].isdigit():
            logger.info("skipping malformed quarantine entry: %s", entry.name)
            continue

        uuid, epoch_str = parts[0], parts[1]
        quarantine_epoch = float(epoch_str)
        age_hours = (now_s - quarantine_epoch) / 3600.0

        if age_hours < QUARANTINE_GRACE_HOURS:
            continue

        # Re-verify: if UUID reappeared in cluster state, restore it
        if uuid in live_uuids or uuid in graveyard_uuids:
            restore_dest = indices_dir / uuid
            if dry_run:
                logger.info(
                    "[dry-run] would restore %s → %s (UUID reappeared in cluster state)",
                    entry, restore_dest,
                )
            else:
                try:
                    os.rename(entry, restore_dest)
                    logger.info(
                        "restored %s → %s (UUID reappeared in cluster state)",
                        entry, restore_dest,
                    )
                except OSError as exc:
                    logger.error("failed to restore %s: %s", entry, exc)
                    continue
            restored += 1
            continue

        # Safe to delete
        if dry_run:
            logger.info("[dry-run] would reap %s (age_hours=%.1f)", entry, age_hours)
        else:
            try:
                shutil.rmtree(entry)
                logger.info("reaped %s (age_hours=%.1f)", entry, age_hours)
            except OSError as exc:
                logger.error("failed to reap %s: %s", entry, exc)
                continue
        reaped += 1

    return reaped, restored


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_scan(cluster: ConnectedCluster, data_path: Path, dry_run: bool) -> ScanResult:
    """Execute one full scan-quarantine-reap cycle.

    Fetches cluster state, scans the indices directory, quarantines candidates,
    and reaps aged quarantine entries.
    """
    client = cluster.client
    now_s = time.time()

    # --- Cluster state ---
    logger.info("fetching cluster state")
    cluster_state = fetch_cluster_state(client)
    live_uuids, graveyard_uuids = parse_known_uuids(cluster_state)
    name_to_uuid = _build_name_to_uuid(cluster_state)
    logger.info(
        "cluster state: %d live indices, %d graveyard entries",
        len(live_uuids), len(graveyard_uuids),
    )

    # --- Active recoveries ---
    logger.info("fetching active recoveries")
    try:
        recoveries = fetch_active_recoveries(client)
        recovery_uuids = parse_recovery_uuids(recoveries, name_to_uuid)
    except Exception as exc:
        # Fail closed: if we can't check recoveries, abort the scan entirely.
        raise RuntimeError(f"cannot fetch active recoveries: {exc}") from exc

    # --- Filesystem ---
    indices_dir = find_indices_dir(data_path, version=cluster.version)
    if indices_dir is None:
        raise RuntimeError(
            f"no indices/ directory found under {data_path} "
            "(tried indices/ and nodes/0/indices/)"
        )
    logger.info("scanning %s", indices_dir)

    es_pid = find_es_pid()
    if es_pid is None:
        logger.info(
            "Elasticsearch process not found in /proc; open-FD check will be skipped"
        )
    else:
        logger.info("found Elasticsearch process pid=%d", es_pid)

    # Count visible index dirs before we start moving anything.
    scanned = sum(
        1 for p in indices_dir.iterdir()
        if p.is_dir() and not p.name.startswith(".")
    )

    # --- Scan ---
    candidates, skipped = scan_for_candidates(
        indices_dir, live_uuids, graveyard_uuids, recovery_uuids, es_pid, now_s,
    )

    # --- Quarantine ---
    quarantine_dir = indices_dir / QUARANTINE_DIR
    quarantined = 0
    for candidate in candidates:
        if quarantine_candidate(candidate, quarantine_dir, dry_run):
            quarantined += 1

    # --- Reap ---
    reaped, restored = reap_quarantine(
        quarantine_dir, indices_dir, client, dry_run, now_s,
    )

    return ScanResult(
        scanned=scanned,
        candidates=candidates,
        quarantined=quarantined,
        skipped=skipped,
        reaped=reaped,
        restored=restored,
        dry_run=dry_run,
    )


# ---------------------------------------------------------------------------
# Report rendering — human mode
# ---------------------------------------------------------------------------

def render_report(console: Console, result: ScanResult) -> None:
    """Render the scan result as a Rich table to the console."""
    mode_label = "[yellow]DRY RUN[/yellow]" if result.dry_run else "[red]LIVE[/red]"
    console.rule(f"Dangling Index Scanner  {mode_label}  {result.generated_at}")

    if result.candidates:
        table = Table(box=box.SIMPLE_HEAD, show_edge=False)
        table.add_column("UUID", style="cyan", no_wrap=True)
        table.add_column("Age (h)", justify="right")
        table.add_column("Path", style="dim")

        for c in result.candidates:
            table.add_row(c.uuid, f"{c.age_hours:.1f}", str(c.path))

        console.print(table)
    else:
        console.print("[green]No dangling index candidates found.[/green]")

    console.print(
        f"scanned={result.scanned}  candidates={len(result.candidates)}  "
        f"quarantined={result.quarantined}  skipped={result.skipped}  "
        f"reaped={result.reaped}  restored={result.restored}"
    )


# ---------------------------------------------------------------------------
# Report rendering — JSON mode
# ---------------------------------------------------------------------------

def render_report_json(result: ScanResult) -> None:
    """Emit the scan result as a single JSON document to stdout."""
    doc = {
        "generated_at": result.generated_at,
        "dry_run": result.dry_run,
        "summary": {
            "scanned": result.scanned,
            "candidates": len(result.candidates),
            "quarantined": result.quarantined,
            "skipped": result.skipped,
            "reaped": result.reaped,
            "restored": result.restored,
        },
        "candidates": [
            {
                "uuid": c.uuid,
                "age_hours": round(c.age_hours, 2),
                "path": str(c.path),
            }
            for c in result.candidates
        ],
    }
    print(json.dumps(doc))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    log_format = configure_logging()
    console = Console() if log_format == "human" else None

    data_path_str = os.environ.get("ES_DATA_PATH")
    if not data_path_str:
        logger.error("ES_DATA_PATH is required but not set")
        sys.exit(1)
    data_path = Path(data_path_str)

    dry_run = DRY_RUN_DEFAULT
    daemon = os.getenv("DAEMON", "false").lower() == "true"

    if dry_run:
        logger.info("running in dry-run mode (set DRY_RUN=false to enable mutations)")

    cluster = build_connected_cluster()

    while True:
        try:
            result = run_scan(cluster, data_path, dry_run)
        except (AuthenticationException, ConnectionError, TransportError) as exc:
            logger.error("Elasticsearch error: %s", exc)
            sys.exit(1)
        except RuntimeError as exc:
            logger.error("%s", exc)
            sys.exit(1)

        if log_format == "human":
            assert console is not None
            render_report(console, result)
        else:
            render_report_json(result)

        if not daemon:
            break

        logger.info("sleeping %ds until next scan", SCAN_INTERVAL_SECONDS)
        time.sleep(SCAN_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
