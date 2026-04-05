"""Tests for domain/es boundary types and domain models."""

import pytest

from slopbox_domain.es.types import (
    RawCatIndexEntry,
    RawIlmExplainEntry,
    RawPhaseExecution,
    RawDataStream,
    RawDataStreamIndex,
)
from slopbox_domain.es.models import IndexProfile


# ---------------------------------------------------------------------------
# RawCatIndexEntry — string coercion and alias handling
# ---------------------------------------------------------------------------

def test_raw_cat_index_entry_coerces_string_ints():
    entry = RawCatIndexEntry.model_validate({
        "index": "logs-app-000001",
        "docs.count": "12450221",
        "store.size": "8912896000",
        "creation.date.epoch": "1709251200000",
        "pri": "3",
        "health": "green",
        "status": "open",
    })
    assert entry.docs_count == 12_450_221
    assert entry.store_size_bytes == 8_912_896_000
    assert entry.creation_epoch_ms == 1_709_251_200_000
    assert entry.primary_shards == 3


def test_raw_cat_index_entry_null_fields_default_to_zero():
    entry = RawCatIndexEntry.model_validate({
        "index": "logs-app-000001",
        "docs.count": None,
        "store.size": None,
        "creation.date.epoch": None,
        "pri": None,
        "health": "green",
        "status": "open",
    })
    assert entry.docs_count == 0
    assert entry.store_size_bytes == 0
    assert entry.creation_epoch_ms == 0
    assert entry.primary_shards == 0


def test_raw_cat_index_entry_missing_fields_default_to_zero():
    entry = RawCatIndexEntry.model_validate({
        "index": "logs-app-000001",
        "health": "yellow",
        "status": "open",
    })
    assert entry.docs_count == 0
    assert entry.store_size_bytes == 0
    assert entry.creation_epoch_ms == 0
    assert entry.primary_shards == 1  # default


def test_raw_cat_index_entry_unknown_health_status_preserved():
    entry = RawCatIndexEntry.model_validate({"index": "idx"})
    assert entry.health == "unknown"
    assert entry.status == "unknown"


def test_raw_cat_index_entry_populate_by_name():
    # Field-name (not alias) access also works due to populate_by_name=True
    entry = RawCatIndexEntry(
        index="logs-app-000001",
        docs_count=42,
        store_size_bytes=1024,
        creation_epoch_ms=0,
        primary_shards=2,
    )
    assert entry.docs_count == 42
    assert entry.primary_shards == 2


# ---------------------------------------------------------------------------
# RawIlmExplainEntry — optional phase_execution and is_write_index
# ---------------------------------------------------------------------------

def test_raw_ilm_explain_entry_full():
    entry = RawIlmExplainEntry.model_validate({
        "index": "logs-app-000001",
        "policy": "logs-default",
        "phase": "hot",
        "is_write_index": True,
        "phase_execution": {
            "modified_date_in_millis": 1_709_251_200_000,
        },
    })
    assert entry.policy == "logs-default"
    assert entry.phase == "hot"
    assert entry.is_write_index is True
    assert entry.phase_execution.modified_date_in_millis == 1_709_251_200_000


def test_raw_ilm_explain_entry_not_write_index():
    entry = RawIlmExplainEntry.model_validate({
        "index": "logs-app-000001",
        "policy": "logs-default",
        "phase": "hot",
        "is_write_index": False,
    })
    assert entry.is_write_index is False


def test_raw_ilm_explain_entry_is_write_index_defaults_false():
    entry = RawIlmExplainEntry.model_validate({"index": "logs-app-000001"})
    assert entry.is_write_index is False


def test_raw_ilm_explain_entry_null_phase_execution():
    entry = RawIlmExplainEntry.model_validate({
        "index": "logs-app-000001",
        "policy": "logs-default",
        "phase": "warm",
        "phase_execution": None,
    })
    assert entry.phase_execution is None


def test_raw_ilm_explain_entry_missing_optional_fields():
    entry = RawIlmExplainEntry.model_validate({"index": "logs-app-000001"})
    assert entry.policy == "unknown"
    assert entry.phase == "unknown"
    assert entry.phase_execution is None


# ---------------------------------------------------------------------------
# RawDataStream — data stream and backing index shapes
# ---------------------------------------------------------------------------

def test_raw_data_stream_full():
    ds = RawDataStream.model_validate({
        "name": "logs-nginx",
        "template": "logs-nginx-default",
        "indices": [
            {"index_name": ".ds-logs-nginx-2024.03.01-000001", "index_uuid": "abc123"},
            {"index_name": ".ds-logs-nginx-2024.03.08-000002", "index_uuid": "def456"},
        ],
    })
    assert ds.name == "logs-nginx"
    assert ds.template == "logs-nginx-default"
    assert len(ds.indices) == 2
    assert ds.indices[0].index_name == ".ds-logs-nginx-2024.03.01-000001"
    assert ds.indices[-1].index_name == ".ds-logs-nginx-2024.03.08-000002"


def test_raw_data_stream_empty_indices():
    ds = RawDataStream.model_validate({"name": "logs-empty", "template": "logs-default"})
    assert ds.indices == []


def test_raw_data_stream_missing_template_defaults():
    ds = RawDataStream.model_validate({"name": "logs-no-template"})
    assert ds.template == "unknown"


def test_raw_data_stream_write_index_is_last():
    """Backing indices list is ordered oldest→newest; last entry is the write index."""
    ds = RawDataStream.model_validate({
        "name": "logs-app",
        "template": "logs-app-default",
        "indices": [
            {"index_name": ".ds-logs-app-000001", "index_uuid": "aaa"},
            {"index_name": ".ds-logs-app-000002", "index_uuid": "bbb"},
            {"index_name": ".ds-logs-app-000003", "index_uuid": "ccc"},
        ],
    })
    assert ds.indices[-1].index_name == ".ds-logs-app-000003"


# ---------------------------------------------------------------------------
# IndexProfile — computed display fields
# ---------------------------------------------------------------------------

def _make_profile(**kwargs) -> IndexProfile:
    defaults = dict(
        name="logs-app-000001",
        policy="logs-default",
        phase="hot",
        index_age_days=10.0,
        phase_age_days=3.0,
        docs=12_450_221,
        size_bytes=8_912_896_000,
        primary_shards=2,
        is_write_index=False,
        data_stream="logs-app",
        creation_epoch_ms=1_709_251_200_000,
        health="green",
        status="open",
    )
    defaults.update(kwargs)
    return IndexProfile(**defaults)


def test_index_profile_size_human():
    p = _make_profile(size_bytes=1_073_741_824)  # 1 GB
    assert p.size_human == "1.0 GB"


def test_index_profile_index_age():
    p = _make_profile(index_age_days=10.0)
    assert p.index_age == "10d"


def test_index_profile_phase_time():
    p = _make_profile(phase_age_days=3.0)
    assert p.phase_time == "3d"


def test_index_profile_phase_time_unknown():
    p = _make_profile(phase_age_days=-1.0)
    assert p.phase_time == "unknown"


def test_index_profile_shard_size_bytes_divides_evenly():
    p = _make_profile(size_bytes=10 * 1024**3, primary_shards=2)
    assert p.shard_size_bytes == 5 * 1024**3


def test_index_profile_shard_size_bytes_single_shard():
    p = _make_profile(size_bytes=8 * 1024**3, primary_shards=1)
    assert p.shard_size_bytes == 8 * 1024**3


def test_index_profile_shard_size_bytes_zero_guard():
    # primary_shards=0 should not divide by zero
    p = _make_profile(size_bytes=4 * 1024**3, primary_shards=0)
    assert p.shard_size_bytes == 4 * 1024**3  # max(0, 1) → treated as 1


def test_index_profile_shard_size_human():
    p = _make_profile(size_bytes=10 * 1024**3, primary_shards=2)
    assert p.shard_size_human == "5.0 GB"


def test_index_profile_is_write_index_stored():
    p = _make_profile(is_write_index=True)
    assert p.is_write_index is True


def test_index_profile_data_stream_none_for_standalone():
    p = _make_profile(data_stream=None)
    assert p.data_stream is None


def test_index_profile_is_frozen():
    p = _make_profile()
    with pytest.raises(Exception):  # frozen=True prevents attribute assignment
        p.docs = 0  # type: ignore[misc]


def test_index_profile_computed_fields_not_in_constructor():
    # Computed fields must NOT be passed as constructor args
    with pytest.raises(Exception):
        IndexProfile(
            name="x", policy="p", phase="hot",
            index_age_days=1.0, phase_age_days=1.0,
            docs=0, size_bytes=0, primary_shards=1,
            is_write_index=False, data_stream=None,
            creation_epoch_ms=0, health="green", status="open",
            size_human="bad",  # should be rejected
        )
