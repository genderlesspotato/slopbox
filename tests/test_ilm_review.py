"""Unit tests for ilm_review.py.

Mock data covers two Elasticsearch generations:
  - 7.x: classic rollover by max_age/max_size, hot/warm/cold/delete phases
  - 8.x: primary-shard-based rollover, frozen phase with searchable snapshots,
          max_primary_shard_docs (added in 8.2)
"""

import pytest
from unittest.mock import patch

from ilm_review import (
    IndexProfile,
    correlate_data,
    format_bytes,
    format_duration,
    parse_all_policies,
    parse_rollover_criteria,
    rollover_criteria_str,
)


# ---------------------------------------------------------------------------
# Fixtures — representative Elasticsearch API response shapes
# ---------------------------------------------------------------------------

# ES 7.x: rollover by max_age + max_size, classic four-phase lifecycle
POLICY_7X = {
    "logs-default": {
        "version": 1,
        "modified_date": "2023-06-01T00:00:00.000Z",
        "policy": {
            "phases": {
                "hot": {
                    "min_age": "0ms",
                    "actions": {
                        "rollover": {"max_age": "7d", "max_size": "50gb"},
                        "set_priority": {"priority": 100},
                    },
                },
                "warm": {
                    "min_age": "7d",
                    "actions": {
                        "shrink": {"number_of_shards": 1},
                        "forcemerge": {"max_num_segments": 1},
                    },
                },
                "cold": {
                    "min_age": "30d",
                    "actions": {"freeze": {}},
                },
                "delete": {
                    "min_age": "90d",
                    "actions": {"delete": {}},
                },
            }
        },
    }
}

# ES 8.x: primary-shard-based criteria, frozen phase with searchable snapshots
POLICY_8X = {
    "logs-default": {
        "version": 2,
        "modified_date": "2024-01-15T00:00:00.000Z",
        "policy": {
            "phases": {
                "hot": {
                    "min_age": "0ms",
                    "actions": {
                        "rollover": {
                            "max_age": "7d",
                            "max_primary_shard_size": "50gb",
                            "max_primary_shard_docs": 200_000_000,
                        },
                    },
                },
                "warm": {
                    "min_age": "7d",
                    "actions": {"shrink": {"number_of_shards": 1}},
                },
                "cold": {
                    "min_age": "30d",
                    "actions": {},
                },
                "frozen": {
                    "min_age": "60d",
                    "actions": {
                        "searchable_snapshot": {"snapshot_repository": "my-repo"}
                    },
                },
                "delete": {
                    "min_age": "365d",
                    "actions": {"delete": {}},
                },
            }
        },
    },
    "metrics-default": {
        "version": 1,
        "modified_date": "2024-01-15T00:00:00.000Z",
        "policy": {
            "phases": {
                "hot": {
                    "min_age": "0ms",
                    "actions": {
                        "rollover": {
                            "max_docs": 50_000_000,
                        }
                    },
                },
                "delete": {
                    "min_age": "30d",
                    "actions": {"delete": {}},
                },
            }
        },
    },
}

# Policy with no rollover action (e.g. a pure-delete lifecycle)
POLICY_NO_ROLLOVER = {
    "cleanup-only": {
        "version": 1,
        "modified_date": "2023-01-01T00:00:00.000Z",
        "policy": {
            "phases": {
                "delete": {
                    "min_age": "30d",
                    "actions": {"delete": {}},
                }
            }
        },
    }
}

# Fixed "now" for deterministic age calculations: 2024-03-01 00:00:00 UTC
NOW_MS = 1_709_251_200_000
NOW_S = NOW_MS / 1000

CREATION_10D_MS = NOW_MS - 10 * 86_400_000   # index created 10 days ago
CREATION_17D_MS = NOW_MS - 17 * 86_400_000   # index created 17 days ago
PHASE_ENTRY_3D_MS = NOW_MS - 3 * 86_400_000  # entered current phase 3 days ago

ILM_EXPLAIN = {
    "logs-app-000023": {
        "index": "logs-app-000023",
        "managed": True,
        "policy": "logs-default",
        "phase": "hot",
        "action": "rollover",
        "step": "check-rollover-ready",
        "phase_execution": {
            "policy": "logs-default",
            "version": 1,
            "modified_date_in_millis": PHASE_ENTRY_3D_MS,
        },
    },
    "logs-app-000022": {
        "index": "logs-app-000022",
        "managed": True,
        "policy": "logs-default",
        "phase": "warm",
        "action": "shrink",
        "step": "wait-for-shard-history-leases",
        "phase_execution": {
            "policy": "logs-default",
            "version": 1,
            "modified_date_in_millis": PHASE_ENTRY_3D_MS,
        },
    },
}

CAT_INDICES = {
    "logs-app-000023": {
        "index": "logs-app-000023",
        "docs.count": "12450221",
        "store.size": "8912896000",
        "creation.date.epoch": str(CREATION_10D_MS),
        "health": "green",
        "status": "open",
    },
    "logs-app-000022": {
        "index": "logs-app-000022",
        "docs.count": "198000000",
        "store.size": "45000000000",
        "creation.date.epoch": str(CREATION_17D_MS),
        "health": "yellow",
        "status": "open",
    },
}


# ---------------------------------------------------------------------------
# format_bytes
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("n, expected", [
    (0,                  "0.0 B"),
    (512,                "512.0 B"),
    (1023,               "1023.0 B"),
    (1024,               "1.0 KB"),
    (1536,               "1.5 KB"),
    (1_048_576,          "1.0 MB"),
    (1_073_741_824,      "1.0 GB"),
    (10_737_418_240,     "10.0 GB"),
    (1_099_511_627_776,  "1.0 TB"),
    (2_251_799_813_685_248, "2.0 PB"),
])
def test_format_bytes(n, expected):
    assert format_bytes(n) == expected


# ---------------------------------------------------------------------------
# format_duration
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("days, expected", [
    (-1.0,   "unknown"),
    (-0.1,   "unknown"),
    (0.0,    "0m"),
    (0.5 / 24,  "30m"),       # 30 minutes
    (1 / 24,    "1h 0m"),     # exactly 1 hour
    (1.5 / 24,  "1h 30m"),    # 1h 30m
    (23 / 24,   "23h 0m"),    # 23 hours
    (1.0,    "1d"),
    (1.5,    "1d 12h"),
    (7.0,    "7d"),
    (7.25,   "7d 6h"),
    (30.0,   "30d"),
    (365.0,  "365d"),
])
def test_format_duration(days, expected):
    assert format_duration(days) == expected


# ---------------------------------------------------------------------------
# parse_rollover_criteria — ES 7.x
# ---------------------------------------------------------------------------

def test_parse_rollover_criteria_7x_max_age_and_size():
    criteria = parse_rollover_criteria(POLICY_7X["logs-default"])
    assert criteria == {"max_age": "7d", "max_size": "50gb"}


def test_parse_rollover_criteria_7x_phases_without_rollover_are_ignored():
    # warm/cold/delete phases have no rollover; only hot should contribute
    criteria = parse_rollover_criteria(POLICY_7X["logs-default"])
    assert "max_docs" not in criteria


# ---------------------------------------------------------------------------
# parse_rollover_criteria — ES 8.x
# ---------------------------------------------------------------------------

def test_parse_rollover_criteria_8x_primary_shard_fields():
    criteria = parse_rollover_criteria(POLICY_8X["logs-default"])
    assert criteria["max_age"] == "7d"
    assert criteria["max_primary_shard_size"] == "50gb"
    assert criteria["max_primary_shard_docs"] == 200_000_000
    assert "max_size" not in criteria


def test_parse_rollover_criteria_8x_max_docs_only():
    criteria = parse_rollover_criteria(POLICY_8X["metrics-default"])
    assert criteria == {"max_docs": 50_000_000}


def test_parse_rollover_criteria_no_rollover_returns_empty():
    criteria = parse_rollover_criteria(POLICY_NO_ROLLOVER["cleanup-only"])
    assert criteria == {}


# ---------------------------------------------------------------------------
# parse_all_policies
# ---------------------------------------------------------------------------

def test_parse_all_policies_7x_phase_list():
    parsed = parse_all_policies(POLICY_7X)
    assert "logs-default" in parsed
    assert set(parsed["logs-default"]["phases"]) == {"hot", "warm", "cold", "delete"}


def test_parse_all_policies_7x_rollover_criteria():
    parsed = parse_all_policies(POLICY_7X)
    assert parsed["logs-default"]["rollover"] == {"max_age": "7d", "max_size": "50gb"}


def test_parse_all_policies_8x_frozen_phase_present():
    parsed = parse_all_policies(POLICY_8X)
    assert "frozen" in parsed["logs-default"]["phases"]


def test_parse_all_policies_8x_multiple_policies():
    parsed = parse_all_policies(POLICY_8X)
    assert set(parsed.keys()) == {"logs-default", "metrics-default"}


def test_parse_all_policies_no_rollover_policy():
    parsed = parse_all_policies(POLICY_NO_ROLLOVER)
    assert parsed["cleanup-only"]["rollover"] == {}
    assert "delete" in parsed["cleanup-only"]["phases"]


# ---------------------------------------------------------------------------
# rollover_criteria_str
# ---------------------------------------------------------------------------

def test_rollover_criteria_str_empty():
    assert rollover_criteria_str({}) == "none"


def test_rollover_criteria_str_single():
    assert rollover_criteria_str({"max_age": "7d"}) == "max_age=7d"


def test_rollover_criteria_str_multiple():
    result = rollover_criteria_str({"max_age": "7d", "max_size": "50gb"})
    assert "max_age=7d" in result
    assert "max_size=50gb" in result


# ---------------------------------------------------------------------------
# correlate_data
# ---------------------------------------------------------------------------

@patch("ilm_review.time.time", return_value=NOW_S)
def test_correlate_data_basic(_mock_time):
    parsed_policies = parse_all_policies(POLICY_7X)
    grouped, skipped = correlate_data(ILM_EXPLAIN, CAT_INDICES, parsed_policies)

    assert skipped == 0
    assert "logs-default" in grouped
    assert len(grouped["logs-default"]) == 2


@patch("ilm_review.time.time", return_value=NOW_S)
def test_correlate_data_index_age(_mock_time):
    parsed_policies = parse_all_policies(POLICY_7X)
    grouped, _ = correlate_data(ILM_EXPLAIN, CAT_INDICES, parsed_policies)

    profiles = {p.name: p for p in grouped["logs-default"]}
    assert abs(profiles["logs-app-000023"].index_age_days - 10.0) < 0.01
    assert abs(profiles["logs-app-000022"].index_age_days - 17.0) < 0.01


@patch("ilm_review.time.time", return_value=NOW_S)
def test_correlate_data_phase_age_string(_mock_time):
    parsed_policies = parse_all_policies(POLICY_7X)
    grouped, _ = correlate_data(ILM_EXPLAIN, CAT_INDICES, parsed_policies)

    profiles = {p.name: p for p in grouped["logs-default"]}
    # Both indices entered their phase 3 days ago
    assert profiles["logs-app-000023"].phase_time == "3d"
    assert profiles["logs-app-000022"].phase_time == "3d"


@patch("ilm_review.time.time", return_value=NOW_S)
def test_correlate_data_docs_and_size(_mock_time):
    parsed_policies = parse_all_policies(POLICY_7X)
    grouped, _ = correlate_data(ILM_EXPLAIN, CAT_INDICES, parsed_policies)

    profiles = {p.name: p for p in grouped["logs-default"]}
    p = profiles["logs-app-000023"]
    assert p.docs == 12_450_221
    assert p.size_bytes == 8_912_896_000
    assert p.health == "green"
    assert p.status == "open"


@patch("ilm_review.time.time", return_value=NOW_S)
def test_correlate_data_missing_cat_entry_increments_skipped(_mock_time):
    ilm_with_extra = {
        **ILM_EXPLAIN,
        "logs-app-000021": {
            "index": "logs-app-000021",
            "managed": True,
            "policy": "logs-default",
            "phase": "cold",
            "action": "freeze",
            "step": "freeze",
            "phase_execution": {
                "policy": "logs-default",
                "version": 1,
                "modified_date_in_millis": PHASE_ENTRY_3D_MS,
            },
        },
    }
    parsed_policies = parse_all_policies(POLICY_7X)
    grouped, skipped = correlate_data(ilm_with_extra, CAT_INDICES, parsed_policies)

    assert skipped == 1
    assert len(grouped["logs-default"]) == 2  # extra index silently skipped


@patch("ilm_review.time.time", return_value=NOW_S)
def test_correlate_data_null_docs_and_size(_mock_time):
    cat_with_nulls = {
        **CAT_INDICES,
        "logs-app-000023": {
            **CAT_INDICES["logs-app-000023"],
            "docs.count": None,
            "store.size": None,
        },
    }
    parsed_policies = parse_all_policies(POLICY_7X)
    grouped, _ = correlate_data(ILM_EXPLAIN, cat_with_nulls, parsed_policies)

    profiles = {p.name: p for p in grouped["logs-default"]}
    p = profiles["logs-app-000023"]
    assert p.docs == 0
    assert p.size_bytes == 0


@patch("ilm_review.time.time", return_value=NOW_S)
def test_correlate_data_no_phase_execution(_mock_time):
    ilm_no_phase_exec = {
        "logs-app-000023": {
            **ILM_EXPLAIN["logs-app-000023"],
            "phase_execution": None,
        }
    }
    parsed_policies = parse_all_policies(POLICY_7X)
    grouped, skipped = correlate_data(ilm_no_phase_exec, CAT_INDICES, parsed_policies)

    assert skipped == 0
    profiles = {p.name: p for p in grouped["logs-default"]}
    assert profiles["logs-app-000023"].phase_time == "unknown"


@patch("ilm_review.time.time", return_value=NOW_S)
def test_correlate_data_unknown_policy_reference(_mock_time):
    """Index referencing a policy that no longer exists in /_ilm/policy."""
    ilm_orphan = {
        "logs-app-000023": {
            **ILM_EXPLAIN["logs-app-000023"],
            "policy": "deleted-policy",
        }
    }
    parsed_policies = parse_all_policies(POLICY_7X)  # does not contain deleted-policy
    grouped, skipped = correlate_data(ilm_orphan, CAT_INDICES, parsed_policies)

    assert skipped == 0
    assert "deleted-policy" in grouped


@patch("ilm_review.time.time", return_value=NOW_S)
def test_correlate_data_8x_frozen_phase(_mock_time):
    ilm_frozen = {
        "logs-app-000020": {
            "index": "logs-app-000020",
            "managed": True,
            "policy": "logs-default",
            "phase": "frozen",
            "action": "searchable_snapshot",
            "step": "mount-searchable-snapshot",
            "phase_execution": {
                "policy": "logs-default",
                "version": 2,
                "modified_date_in_millis": PHASE_ENTRY_3D_MS,
            },
        }
    }
    cat_frozen = {
        "logs-app-000020": {
            "index": "logs-app-000020",
            "docs.count": "200000000",
            "store.size": "1073741824",
            "creation.date.epoch": str(CREATION_17D_MS),
            "health": "green",
            "status": "open",
        }
    }
    parsed_policies = parse_all_policies(POLICY_8X)
    grouped, skipped = correlate_data(ilm_frozen, cat_frozen, parsed_policies)

    assert skipped == 0
    profiles = {p.name: p for p in grouped["logs-default"]}
    assert profiles["logs-app-000020"].phase == "frozen"
