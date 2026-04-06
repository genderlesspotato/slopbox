"""Unit tests for dangling_index_scanner.py.

All filesystem operations use pytest's tmp_path fixture — no real ES cluster
or real Elasticsearch data directory is required.

Time-dependent tests patch ``dangling_index_scanner.time.time`` with a fixed
epoch, matching the pattern used in test_ilm_review.py.
"""

import json
import logging
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dangling_index_scanner import (
    ORPHAN_AGE_HOURS,
    QUARANTINE_GRACE_HOURS,
    QUARANTINE_DIR,
    DanglingCandidate,
    ScanResult,
    find_indices_dir,
    has_open_fds,
    parse_known_uuids,
    parse_recovery_uuids,
    quarantine_candidate,
    reap_quarantine,
    render_report_json,
    scan_for_candidates,
)
from slopbox_domain.es.types import RawCatRecoveryEntry


# ---------------------------------------------------------------------------
# Fixed time for deterministic tests: 2024-03-01 00:00:00 UTC
# ---------------------------------------------------------------------------

NOW_S = 1_709_251_200.0
OLD_ENOUGH_CTIME = NOW_S - (ORPHAN_AGE_HOURS + 1) * 3600   # safely over threshold
TOO_YOUNG_CTIME = NOW_S - (ORPHAN_AGE_HOURS / 2) * 3600    # under threshold


def _now_s_past_threshold(path: Path) -> float:
    """Return a now_s that is ORPHAN_AGE_HOURS + 1h after path's real ctime.

    Avoids patching Path.stat (which breaks is_dir()) while still exercising
    the age-threshold logic with real filesystem directories.
    """
    ctime = path.stat().st_ctime
    return ctime + (ORPHAN_AGE_HOURS + 1) * 3600


def _now_s_before_threshold(path: Path) -> float:
    """Return a now_s that is ORPHAN_AGE_HOURS / 2 hours after path's real ctime."""
    ctime = path.stat().st_ctime
    return ctime + (ORPHAN_AGE_HOURS / 2) * 3600


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

LIVE_UUID_1 = "AbCdEfGhIjKlMnOpQrSt12"
LIVE_UUID_2 = "ZzZzZzZzZzZzZzZzZzZz99"
GRAVEYARD_UUID = "GrAvEyArDuUiDxXxXxXx01"
DANGLING_UUID_1 = "DaNgLiNgOnE1111111111A"
DANGLING_UUID_2 = "DaNgLiNgTwO2222222222B"

CLUSTER_STATE = {
    "metadata": {
        "indices": {
            "logs-app-000001": {
                "settings": {"index": {"uuid": LIVE_UUID_1}},
            },
            "metrics-app-000001": {
                "settings": {"index": {"uuid": LIVE_UUID_2}},
            },
        },
        "index-graveyard": {
            "tombstones": [
                {"index": {"index_uuid": GRAVEYARD_UUID}},
            ],
        },
    }
}

CLUSTER_STATE_EMPTY_GRAVEYARD = {
    "metadata": {
        "indices": {
            "logs-app-000001": {
                "settings": {"index": {"uuid": LIVE_UUID_1}},
            },
        },
        "index-graveyard": {"tombstones": []},
    }
}

CLUSTER_STATE_NO_GRAVEYARD_KEY = {
    "metadata": {
        "indices": {
            "logs-app-000001": {
                "settings": {"index": {"uuid": LIVE_UUID_1}},
            },
        },
    }
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_old_dir(parent: Path, name: str) -> Path:
    """Create a directory and backdate its ctime via utime on parent (best-effort)."""
    d = parent / name
    d.mkdir()
    return d


def _make_candidate(uuid: str, path: Path, age_hours: float = 30.0) -> DanglingCandidate:
    return DanglingCandidate(uuid=uuid, path=path, age_hours=age_hours)


def _make_scan_result(**kwargs) -> ScanResult:
    defaults = dict(
        scanned=5,
        candidates=[],
        quarantined=0,
        skipped=0,
        reaped=0,
        restored=0,
        dry_run=True,
        generated_at="2024-03-01T00:00:00+00:00",
    )
    defaults.update(kwargs)
    return ScanResult(**defaults)


# ---------------------------------------------------------------------------
# parse_known_uuids
# ---------------------------------------------------------------------------

def test_parse_known_uuids_extracts_live_and_graveyard():
    live, graveyard = parse_known_uuids(CLUSTER_STATE)
    assert LIVE_UUID_1 in live
    assert LIVE_UUID_2 in live
    assert GRAVEYARD_UUID in graveyard
    assert GRAVEYARD_UUID not in live


def test_parse_known_uuids_empty_graveyard():
    live, graveyard = parse_known_uuids(CLUSTER_STATE_EMPTY_GRAVEYARD)
    assert LIVE_UUID_1 in live
    assert graveyard == set()


def test_parse_known_uuids_missing_graveyard_key():
    live, graveyard = parse_known_uuids(CLUSTER_STATE_NO_GRAVEYARD_KEY)
    assert LIVE_UUID_1 in live
    assert graveyard == set()


def test_parse_known_uuids_empty_metadata():
    live, graveyard = parse_known_uuids({})
    assert live == set()
    assert graveyard == set()


def test_parse_known_uuids_skips_missing_uuid_field():
    state = {
        "metadata": {
            "indices": {
                "broken-index": {"settings": {}},
                "good-index": {"settings": {"index": {"uuid": LIVE_UUID_1}}},
            },
            "index-graveyard": {"tombstones": []},
        }
    }
    live, _ = parse_known_uuids(state)
    assert LIVE_UUID_1 in live
    assert len(live) == 1


# ---------------------------------------------------------------------------
# parse_recovery_uuids
# ---------------------------------------------------------------------------

def test_parse_recovery_uuids_maps_index_name_to_uuid():
    name_to_uuid = {"logs-app-000001": LIVE_UUID_1}
    recoveries = [RawCatRecoveryEntry(index="logs-app-000001", shard="0", stage="index")]
    result = parse_recovery_uuids(recoveries, name_to_uuid)
    assert result == {LIVE_UUID_1}


def test_parse_recovery_uuids_unknown_index_skipped():
    name_to_uuid = {"logs-app-000001": LIVE_UUID_1}
    recoveries = [RawCatRecoveryEntry(index="unknown-index", shard="0", stage="index")]
    result = parse_recovery_uuids(recoveries, name_to_uuid)
    assert result == set()


def test_parse_recovery_uuids_empty():
    result = parse_recovery_uuids([], {})
    assert result == set()


# ---------------------------------------------------------------------------
# find_indices_dir
# ---------------------------------------------------------------------------

def test_find_indices_dir_es8x_layout(tmp_path):
    indices = tmp_path / "indices"
    indices.mkdir()
    assert find_indices_dir(tmp_path) == indices


def test_find_indices_dir_es7x_layout(tmp_path):
    indices = tmp_path / "nodes" / "0" / "indices"
    indices.mkdir(parents=True)
    assert find_indices_dir(tmp_path) == indices


def test_find_indices_dir_es8x_preferred_over_7x(tmp_path):
    es8 = tmp_path / "indices"
    es8.mkdir()
    es7 = tmp_path / "nodes" / "0" / "indices"
    es7.mkdir(parents=True)
    assert find_indices_dir(tmp_path) == es8


def test_find_indices_dir_returns_none_when_absent(tmp_path):
    assert find_indices_dir(tmp_path) is None


# ---------------------------------------------------------------------------
# scan_for_candidates
# ---------------------------------------------------------------------------

def _setup_indices_dir(tmp_path: Path) -> Path:
    indices = tmp_path / "indices"
    indices.mkdir()
    return indices


def test_scan_finds_genuine_orphan(tmp_path):
    indices = _setup_indices_dir(tmp_path)
    d = indices / DANGLING_UUID_1
    d.mkdir()
    now_s = _now_s_past_threshold(d)

    candidates, skipped = scan_for_candidates(
        indices, set(), set(), set(), None, now_s
    )

    assert len(candidates) == 1
    assert candidates[0].uuid == DANGLING_UUID_1
    assert skipped == 0


def test_scan_excludes_live_uuid(tmp_path):
    indices = _setup_indices_dir(tmp_path)
    d = indices / LIVE_UUID_1
    d.mkdir()
    now_s = _now_s_past_threshold(d)

    candidates, skipped = scan_for_candidates(
        indices, {LIVE_UUID_1}, set(), set(), None, now_s
    )

    assert candidates == []
    assert skipped == 1


def test_scan_excludes_graveyard_uuid(tmp_path):
    indices = _setup_indices_dir(tmp_path)
    d = indices / GRAVEYARD_UUID
    d.mkdir()
    now_s = _now_s_past_threshold(d)

    candidates, skipped = scan_for_candidates(
        indices, set(), {GRAVEYARD_UUID}, set(), None, now_s
    )

    assert candidates == []
    assert skipped == 1


def test_scan_excludes_dir_with_state_subdir(tmp_path):
    indices = _setup_indices_dir(tmp_path)
    d = indices / DANGLING_UUID_1
    d.mkdir()
    (d / "_state").mkdir()
    now_s = _now_s_past_threshold(d)

    candidates, skipped = scan_for_candidates(
        indices, set(), set(), set(), None, now_s
    )

    assert candidates == []
    assert skipped == 1


def test_scan_excludes_too_young_dir(tmp_path):
    indices = _setup_indices_dir(tmp_path)
    d = indices / DANGLING_UUID_1
    d.mkdir()
    now_s = _now_s_before_threshold(d)

    candidates, skipped = scan_for_candidates(
        indices, set(), set(), set(), None, now_s
    )

    assert candidates == []
    assert skipped == 1


def test_scan_excludes_recovery_uuid(tmp_path):
    indices = _setup_indices_dir(tmp_path)
    d = indices / DANGLING_UUID_1
    d.mkdir()
    now_s = _now_s_past_threshold(d)

    candidates, skipped = scan_for_candidates(
        indices, set(), set(), {DANGLING_UUID_1}, None, now_s
    )

    assert candidates == []
    assert skipped == 1


@patch("dangling_index_scanner.time.time", return_value=NOW_S)
def test_scan_skips_quarantine_dir(mock_time, tmp_path):
    indices = _setup_indices_dir(tmp_path)
    q = indices / QUARANTINE_DIR
    q.mkdir()

    candidates, skipped = scan_for_candidates(
        indices, set(), set(), set(), None, NOW_S
    )

    assert candidates == []
    assert skipped == 0  # hidden dirs don't count as skipped


@patch("dangling_index_scanner.time.time", return_value=NOW_S)
def test_scan_skips_non_uuid_dir(mock_time, tmp_path):
    indices = _setup_indices_dir(tmp_path)
    (indices / "not-a-uuid").mkdir()

    candidates, skipped = scan_for_candidates(
        indices, set(), set(), set(), None, NOW_S
    )

    assert candidates == []
    assert skipped == 1


def test_scan_multiple_mixed(tmp_path):
    indices = _setup_indices_dir(tmp_path)
    # one genuine orphan
    d1 = indices / DANGLING_UUID_1
    d1.mkdir()
    # one live index (has _state)
    d2 = indices / LIVE_UUID_1
    d2.mkdir()
    (d2 / "_state").mkdir()
    now_s = _now_s_past_threshold(d1)

    candidates, skipped = scan_for_candidates(
        indices, set(), set(), set(), None, now_s
    )

    assert len(candidates) == 1
    assert candidates[0].uuid == DANGLING_UUID_1
    assert skipped == 1


def test_scan_skips_dir_with_open_fds(tmp_path):
    indices = _setup_indices_dir(tmp_path)
    d = indices / DANGLING_UUID_1
    d.mkdir()
    now_s = _now_s_past_threshold(d)

    with patch("dangling_index_scanner.has_open_fds", return_value=True):
        candidates, skipped = scan_for_candidates(
            indices, set(), set(), set(), es_pid=12345, now_s=now_s
        )

    assert candidates == []
    assert skipped == 1


# ---------------------------------------------------------------------------
# quarantine_candidate
# ---------------------------------------------------------------------------

@patch("dangling_index_scanner.time.time", return_value=NOW_S)
def test_quarantine_candidate_dry_run_does_not_rename(mock_time, tmp_path, caplog):
    indices = tmp_path / "indices"
    indices.mkdir()
    src = indices / DANGLING_UUID_1
    src.mkdir()
    quarantine_dir = indices / QUARANTINE_DIR

    candidate = _make_candidate(DANGLING_UUID_1, src)
    with caplog.at_level(logging.INFO, logger="dangling_index_scanner"):
        result = quarantine_candidate(candidate, quarantine_dir, dry_run=True)

    assert result is True
    assert src.exists()             # not moved
    assert not quarantine_dir.exists()  # not created
    assert "dry-run" in caplog.text


@patch("dangling_index_scanner.time.time", return_value=NOW_S)
def test_quarantine_candidate_live_renames_dir(mock_time, tmp_path):
    indices = tmp_path / "indices"
    indices.mkdir()
    src = indices / DANGLING_UUID_1
    src.mkdir()
    quarantine_dir = indices / QUARANTINE_DIR

    candidate = _make_candidate(DANGLING_UUID_1, src)
    result = quarantine_candidate(candidate, quarantine_dir, dry_run=False)

    assert result is True
    assert not src.exists()
    assert quarantine_dir.is_dir()
    expected_folder = f"{DANGLING_UUID_1}__{int(NOW_S)}"
    assert (quarantine_dir / expected_folder).is_dir()


@patch("dangling_index_scanner.time.time", return_value=NOW_S)
def test_quarantine_candidate_creates_quarantine_dir(mock_time, tmp_path):
    indices = tmp_path / "indices"
    indices.mkdir()
    src = indices / DANGLING_UUID_1
    src.mkdir()
    quarantine_dir = indices / QUARANTINE_DIR
    # quarantine_dir does not exist yet
    assert not quarantine_dir.exists()

    candidate = _make_candidate(DANGLING_UUID_1, src)
    quarantine_candidate(candidate, quarantine_dir, dry_run=False)

    assert quarantine_dir.is_dir()


# ---------------------------------------------------------------------------
# reap_quarantine
# ---------------------------------------------------------------------------

def _make_quarantine_entry(quarantine_dir: Path, uuid: str, quarantine_epoch: float) -> Path:
    quarantine_dir.mkdir(exist_ok=True)
    entry = quarantine_dir / f"{uuid}__{int(quarantine_epoch)}"
    entry.mkdir()
    return entry


def _make_mock_client(cluster_state: dict) -> MagicMock:
    client = MagicMock()
    client.cluster.state.return_value.body = cluster_state
    return client


def test_reap_skips_young_quarantine_entry(tmp_path):
    indices = tmp_path / "indices"
    indices.mkdir()
    quarantine_dir = indices / QUARANTINE_DIR
    recent_epoch = NOW_S - (QUARANTINE_GRACE_HOURS / 2) * 3600
    entry = _make_quarantine_entry(quarantine_dir, DANGLING_UUID_1, recent_epoch)

    client = _make_mock_client(CLUSTER_STATE)
    reaped, restored = reap_quarantine(
        quarantine_dir, indices, client, dry_run=False, now_s=NOW_S
    )

    assert reaped == 0
    assert restored == 0
    assert entry.exists()  # not touched


def test_reap_deletes_old_quarantine_entry(tmp_path):
    indices = tmp_path / "indices"
    indices.mkdir()
    quarantine_dir = indices / QUARANTINE_DIR
    old_epoch = NOW_S - (QUARANTINE_GRACE_HOURS + 1) * 3600
    entry = _make_quarantine_entry(quarantine_dir, DANGLING_UUID_1, old_epoch)

    client = _make_mock_client(CLUSTER_STATE)
    reaped, restored = reap_quarantine(
        quarantine_dir, indices, client, dry_run=False, now_s=NOW_S
    )

    assert reaped == 1
    assert restored == 0
    assert not entry.exists()


def test_reap_dry_run_does_not_delete(tmp_path, caplog):
    indices = tmp_path / "indices"
    indices.mkdir()
    quarantine_dir = indices / QUARANTINE_DIR
    old_epoch = NOW_S - (QUARANTINE_GRACE_HOURS + 1) * 3600
    entry = _make_quarantine_entry(quarantine_dir, DANGLING_UUID_1, old_epoch)

    client = _make_mock_client(CLUSTER_STATE)
    with caplog.at_level(logging.INFO, logger="dangling_index_scanner"):
        reaped, restored = reap_quarantine(
            quarantine_dir, indices, client, dry_run=True, now_s=NOW_S
        )

    assert reaped == 1
    assert entry.exists()  # not deleted in dry-run
    assert "dry-run" in caplog.text


def test_reap_restores_reappeared_uuid(tmp_path):
    indices = tmp_path / "indices"
    indices.mkdir()
    quarantine_dir = indices / QUARANTINE_DIR
    old_epoch = NOW_S - (QUARANTINE_GRACE_HOURS + 1) * 3600
    entry = _make_quarantine_entry(quarantine_dir, LIVE_UUID_1, old_epoch)

    # LIVE_UUID_1 is back in the cluster state
    client = _make_mock_client(CLUSTER_STATE)
    reaped, restored = reap_quarantine(
        quarantine_dir, indices, client, dry_run=False, now_s=NOW_S
    )

    assert restored == 1
    assert reaped == 0
    assert not entry.exists()
    assert (indices / LIVE_UUID_1).is_dir()


def test_reap_restores_reappeared_graveyard_uuid(tmp_path):
    indices = tmp_path / "indices"
    indices.mkdir()
    quarantine_dir = indices / QUARANTINE_DIR
    old_epoch = NOW_S - (QUARANTINE_GRACE_HOURS + 1) * 3600
    entry = _make_quarantine_entry(quarantine_dir, GRAVEYARD_UUID, old_epoch)

    client = _make_mock_client(CLUSTER_STATE)
    reaped, restored = reap_quarantine(
        quarantine_dir, indices, client, dry_run=False, now_s=NOW_S
    )

    assert restored == 1
    assert not entry.exists()
    assert (indices / GRAVEYARD_UUID).is_dir()


def test_reap_skips_malformed_entry_name(tmp_path):
    indices = tmp_path / "indices"
    indices.mkdir()
    quarantine_dir = indices / QUARANTINE_DIR
    quarantine_dir.mkdir()
    # Malformed: no __ separator
    bad = quarantine_dir / "malformed-no-separator"
    bad.mkdir()
    also_bad = quarantine_dir / f"{DANGLING_UUID_1}__notanumber"
    also_bad.mkdir()

    client = _make_mock_client(CLUSTER_STATE)
    reaped, restored = reap_quarantine(
        quarantine_dir, indices, client, dry_run=False, now_s=NOW_S
    )

    assert reaped == 0
    assert restored == 0
    assert bad.exists()
    assert also_bad.exists()


def test_reap_returns_zero_zero_when_quarantine_dir_absent(tmp_path):
    indices = tmp_path / "indices"
    indices.mkdir()
    quarantine_dir = indices / QUARANTINE_DIR  # does not exist

    client = _make_mock_client(CLUSTER_STATE)
    reaped, restored = reap_quarantine(
        quarantine_dir, indices, client, dry_run=False, now_s=NOW_S
    )

    assert (reaped, restored) == (0, 0)


def test_reap_aborts_if_cluster_state_fetch_fails(tmp_path, caplog):
    indices = tmp_path / "indices"
    indices.mkdir()
    quarantine_dir = indices / QUARANTINE_DIR
    old_epoch = NOW_S - (QUARANTINE_GRACE_HOURS + 1) * 3600
    entry = _make_quarantine_entry(quarantine_dir, DANGLING_UUID_1, old_epoch)

    client = MagicMock()
    client.cluster.state.side_effect = Exception("cluster unreachable")

    with caplog.at_level(logging.ERROR, logger="dangling_index_scanner"):
        reaped, restored = reap_quarantine(
            quarantine_dir, indices, client, dry_run=False, now_s=NOW_S
        )

    assert (reaped, restored) == (0, 0)
    assert entry.exists()  # untouched
    assert "cannot fetch cluster state" in caplog.text


# ---------------------------------------------------------------------------
# render_report_json
# ---------------------------------------------------------------------------

def test_render_report_json_schema(capsys):
    candidate = _make_candidate(
        DANGLING_UUID_1,
        Path("/var/lib/elasticsearch/data/indices") / DANGLING_UUID_1,
        age_hours=36.5,
    )
    result = _make_scan_result(
        scanned=10,
        candidates=[candidate],
        quarantined=1,
        skipped=8,
        reaped=0,
        restored=0,
        dry_run=True,
    )
    render_report_json(result)

    out = capsys.readouterr().out
    doc = json.loads(out)

    assert "generated_at" in doc
    assert doc["dry_run"] is True
    assert doc["summary"]["scanned"] == 10
    assert doc["summary"]["candidates"] == 1
    assert doc["summary"]["quarantined"] == 1
    assert doc["summary"]["skipped"] == 8
    assert doc["summary"]["reaped"] == 0
    assert doc["summary"]["restored"] == 0
    assert len(doc["candidates"]) == 1
    assert doc["candidates"][0]["uuid"] == DANGLING_UUID_1
    assert doc["candidates"][0]["age_hours"] == 36.5


def test_render_report_json_no_candidates(capsys):
    result = _make_scan_result(candidates=[], quarantined=0)
    render_report_json(result)
    doc = json.loads(capsys.readouterr().out)
    assert doc["candidates"] == []
    assert doc["summary"]["candidates"] == 0


def test_render_report_json_dry_run_false(capsys):
    result = _make_scan_result(dry_run=False)
    render_report_json(result)
    doc = json.loads(capsys.readouterr().out)
    assert doc["dry_run"] is False


# ---------------------------------------------------------------------------
# main() — environment validation
# ---------------------------------------------------------------------------

def test_main_missing_es_data_path_exits(monkeypatch, capsys):
    # configure_logging() clears all handlers (including caplog's), so we check
    # stdout/stderr via capsys instead of caplog.
    monkeypatch.delenv("ES_DATA_PATH", raising=False)
    from dangling_index_scanner import main
    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "ES_DATA_PATH" in captured.out or "ES_DATA_PATH" in captured.err


# ---------------------------------------------------------------------------
# has_open_fds — unit test with a real /proc/self/fd if available
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not Path("/proc").is_dir(), reason="requires /proc (Linux)")
def test_has_open_fds_detects_own_process(tmp_path):
    # Open a file under tmp_path and check that this process has an open FD there
    f = tmp_path / "test.dat"
    with open(f, "w") as fh:
        fh.write("x")
        pid = os.getpid()
        result = has_open_fds(pid, tmp_path)
    assert result is True


@pytest.mark.skipif(not Path("/proc").is_dir(), reason="requires /proc (Linux)")
def test_has_open_fds_no_fd_under_path(tmp_path):
    other = tmp_path / "other"
    other.mkdir()
    pid = os.getpid()
    # No open FDs to a directory we never opened
    result = has_open_fds(pid, other)
    assert result is False


def test_has_open_fds_returns_false_for_nonexistent_pid():
    # PID 0 never has a /proc entry
    result = has_open_fds(0, Path("/nonexistent"))
    assert result is False


# ---------------------------------------------------------------------------
# RawCatRecoveryEntry — boundary type
# ---------------------------------------------------------------------------

def test_raw_cat_recovery_entry_minimal():
    entry = RawCatRecoveryEntry.model_validate({"index": "logs-app-000001"})
    assert entry.index == "logs-app-000001"
    assert entry.shard == "0"
    assert entry.stage == "unknown"


def test_raw_cat_recovery_entry_ignores_extra_fields():
    entry = RawCatRecoveryEntry.model_validate({
        "index": "logs-app-000001",
        "shard": "2",
        "stage": "translog",
        "source_node": "node-1",
        "target_node": "node-2",
        "bytes_percent": "42%",
    })
    assert entry.index == "logs-app-000001"
    assert entry.shard == "2"
    assert entry.stage == "translog"
