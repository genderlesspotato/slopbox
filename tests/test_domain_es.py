"""Tests for domain/es boundary types and domain models."""

import pytest

from domain.es.types import RawCatIndexEntry, RawIlmExplainEntry, RawPhaseExecution
from domain.es.models import IndexProfile


# ---------------------------------------------------------------------------
# RawCatIndexEntry — string coercion and alias handling
# ---------------------------------------------------------------------------

def test_raw_cat_index_entry_coerces_string_ints():
    entry = RawCatIndexEntry.model_validate({
        "index": "logs-app-000001",
        "docs.count": "12450221",
        "store.size": "8912896000",
        "creation.date.epoch": "1709251200000",
        "health": "green",
        "status": "open",
    })
    assert entry.docs_count == 12_450_221
    assert entry.store_size_bytes == 8_912_896_000
    assert entry.creation_epoch_ms == 1_709_251_200_000


def test_raw_cat_index_entry_null_fields_default_to_zero():
    entry = RawCatIndexEntry.model_validate({
        "index": "logs-app-000001",
        "docs.count": None,
        "store.size": None,
        "creation.date.epoch": None,
        "health": "green",
        "status": "open",
    })
    assert entry.docs_count == 0
    assert entry.store_size_bytes == 0
    assert entry.creation_epoch_ms == 0


def test_raw_cat_index_entry_missing_fields_default_to_zero():
    entry = RawCatIndexEntry.model_validate({
        "index": "logs-app-000001",
        "health": "yellow",
        "status": "open",
    })
    assert entry.docs_count == 0
    assert entry.store_size_bytes == 0
    assert entry.creation_epoch_ms == 0


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
    )
    assert entry.docs_count == 42


# ---------------------------------------------------------------------------
# RawIlmExplainEntry — optional phase_execution
# ---------------------------------------------------------------------------

def test_raw_ilm_explain_entry_full():
    entry = RawIlmExplainEntry.model_validate({
        "index": "logs-app-000001",
        "policy": "logs-default",
        "phase": "hot",
        "phase_execution": {
            "modified_date_in_millis": 1_709_251_200_000,
        },
    })
    assert entry.policy == "logs-default"
    assert entry.phase == "hot"
    assert entry.phase_execution.modified_date_in_millis == 1_709_251_200_000


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


def test_index_profile_is_frozen():
    p = _make_profile()
    with pytest.raises(Exception):  # frozen=True prevents attribute assignment
        p.docs = 0  # type: ignore[misc]


def test_index_profile_computed_fields_not_in_constructor():
    # size_human, index_age, phase_time must NOT be passed as constructor args
    with pytest.raises(Exception):
        IndexProfile(
            name="x", policy="p", phase="hot",
            index_age_days=1.0, phase_age_days=1.0,
            docs=0, size_bytes=0, health="green", status="open",
            size_human="bad",  # should be rejected
        )
