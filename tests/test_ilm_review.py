"""Unit tests for ilm_review.py.

Mock data covers two Elasticsearch generations:
  - 7.x: classic rollover by max_age/max_size, hot/warm/cold/delete phases
  - 8.x: primary-shard-based rollover, frozen phase with searchable snapshots,
          max_primary_shard_docs (added in 8.2)
"""

import pytest
from unittest.mock import patch

from slopbox.formatting import format_bytes, format_duration
from ilm_review import (
    correlate_data,
    parse_all_policies,
    parse_rollover_criteria,
    profile_data_streams,
    rollover_criteria_str,
    _parse_size_bytes,
    _recommend,
    TARGET_MIN_HOURS,
    TARGET_MAX_HOURS,
    MAX_SHARD_BYTES,
)
from domain.es.types import RawDataStream


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
        "pri": "2",
        "health": "green",
        "status": "open",
    },
    "logs-app-000022": {
        "index": "logs-app-000022",
        "docs.count": "198000000",
        "store.size": "45000000000",
        "creation.date.epoch": str(CREATION_17D_MS),
        "pri": "2",
        "health": "yellow",
        "status": "open",
    },
}

# Empty data streams list for tests that don't exercise profiling
NO_DATA_STREAMS: list[RawDataStream] = []


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
    grouped, skipped = correlate_data(ILM_EXPLAIN, CAT_INDICES, parsed_policies, NO_DATA_STREAMS)

    assert skipped == 0
    assert "logs-default" in grouped
    assert len(grouped["logs-default"]) == 2


@patch("ilm_review.time.time", return_value=NOW_S)
def test_correlate_data_index_age(_mock_time):
    parsed_policies = parse_all_policies(POLICY_7X)
    grouped, _ = correlate_data(ILM_EXPLAIN, CAT_INDICES, parsed_policies, NO_DATA_STREAMS)

    profiles = {p.name: p for p in grouped["logs-default"]}
    assert abs(profiles["logs-app-000023"].index_age_days - 10.0) < 0.01
    assert abs(profiles["logs-app-000022"].index_age_days - 17.0) < 0.01


@patch("ilm_review.time.time", return_value=NOW_S)
def test_correlate_data_phase_age_string(_mock_time):
    parsed_policies = parse_all_policies(POLICY_7X)
    grouped, _ = correlate_data(ILM_EXPLAIN, CAT_INDICES, parsed_policies, NO_DATA_STREAMS)

    profiles = {p.name: p for p in grouped["logs-default"]}
    # Both indices entered their phase 3 days ago
    assert profiles["logs-app-000023"].phase_time == "3d"
    assert profiles["logs-app-000022"].phase_time == "3d"


@patch("ilm_review.time.time", return_value=NOW_S)
def test_correlate_data_docs_and_size(_mock_time):
    parsed_policies = parse_all_policies(POLICY_7X)
    grouped, _ = correlate_data(ILM_EXPLAIN, CAT_INDICES, parsed_policies, NO_DATA_STREAMS)

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
    grouped, skipped = correlate_data(ilm_with_extra, CAT_INDICES, parsed_policies, NO_DATA_STREAMS)

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
    grouped, _ = correlate_data(ILM_EXPLAIN, cat_with_nulls, parsed_policies, NO_DATA_STREAMS)

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
    grouped, skipped = correlate_data(ilm_no_phase_exec, CAT_INDICES, parsed_policies, NO_DATA_STREAMS)

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
    grouped, skipped = correlate_data(ilm_orphan, CAT_INDICES, parsed_policies, NO_DATA_STREAMS)

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
    grouped, skipped = correlate_data(ilm_frozen, cat_frozen, parsed_policies, NO_DATA_STREAMS)

    assert skipped == 0
    profiles = {p.name: p for p in grouped["logs-default"]}
    assert profiles["logs-app-000020"].phase == "frozen"


# ---------------------------------------------------------------------------
# _parse_size_bytes
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("size_str,expected", [
    ("50gb",  50 * 1024**3),
    ("50GB",  50 * 1024**3),
    ("512mb", 512 * 1024**2),
    ("1tb",   1024**4),
    ("4096",  4096),
    ("1kb",   1024),
])
def test_parse_size_bytes(size_str, expected):
    assert _parse_size_bytes(size_str) == expected


# ---------------------------------------------------------------------------
# _recommend — unit tests for the recommendation ladder
# ---------------------------------------------------------------------------

DATA_NODE_COUNT = 10
MAX_SHARDS_BELOW_SPLIT = int(DATA_NODE_COUNT * 0.6) - 1   # 5  — below 60 % threshold
MAX_SHARDS_AT_SPLIT    = int(DATA_NODE_COUNT * 0.6)        # 6  — at threshold


@pytest.mark.parametrize("scenario,kwargs,expected_rec,check_detail", [
    (
        "too fast, shard below 50gb → increase shard size",
        dict(
            avg_rotation_hours=2.0,
            avg_shard_size_bytes=10 * 1024**3,   # 10 GB
            primary_shards=2,
            max_shard_size_str="10gb",
            data_node_count=DATA_NODE_COUNT,
        ),
        "increase max_primary_shard_size",
        lambda d: d is not None and d.endswith("gb"),
    ),
    (
        "too fast, shard at 50gb, shards below split threshold → increase shards",
        dict(
            avg_rotation_hours=2.0,
            avg_shard_size_bytes=MAX_SHARD_BYTES,
            primary_shards=MAX_SHARDS_BELOW_SPLIT,
            max_shard_size_str="50gb",
            data_node_count=DATA_NODE_COUNT,
        ),
        "increase number_of_shards",
        lambda d: d is not None and int(d) > MAX_SHARDS_BELOW_SPLIT,
    ),
    (
        "too fast, shard at 50gb, shards at split threshold → SPLIT",
        dict(
            avg_rotation_hours=2.0,
            avg_shard_size_bytes=MAX_SHARD_BYTES,
            primary_shards=MAX_SHARDS_AT_SPLIT,
            max_shard_size_str="50gb",
            data_node_count=DATA_NODE_COUNT,
        ),
        "SPLIT into multiple independent indices",
        lambda d: d is not None,
    ),
    (
        "too slow, multiple shards → decrease shards",
        dict(
            avg_rotation_hours=72.0,
            avg_shard_size_bytes=5 * 1024**3,
            primary_shards=4,
            max_shard_size_str="30gb",
            data_node_count=DATA_NODE_COUNT,
        ),
        "decrease number_of_shards",
        lambda d: d is not None and int(d) < 4,
    ),
    (
        "too slow, single shard → decrease shard size",
        dict(
            avg_rotation_hours=72.0,
            avg_shard_size_bytes=30 * 1024**3,
            primary_shards=1,
            max_shard_size_str="30gb",
            data_node_count=DATA_NODE_COUNT,
        ),
        "decrease max_primary_shard_size",
        lambda d: d is not None and d.endswith("gb"),
    ),
    (
        "within target range → OK",
        dict(
            avg_rotation_hours=12.0,
            avg_shard_size_bytes=20 * 1024**3,
            primary_shards=2,
            max_shard_size_str="30gb",
            data_node_count=DATA_NODE_COUNT,
        ),
        "OK",
        lambda d: d is None,
    ),
    (
        "at exactly TARGET_MIN boundary → OK",
        dict(
            avg_rotation_hours=TARGET_MIN_HOURS,
            avg_shard_size_bytes=10 * 1024**3,
            primary_shards=2,
            max_shard_size_str="15gb",
            data_node_count=DATA_NODE_COUNT,
        ),
        "OK",
        lambda d: d is None,
    ),
    (
        "at exactly TARGET_MAX boundary → OK",
        dict(
            avg_rotation_hours=TARGET_MAX_HOURS,
            avg_shard_size_bytes=10 * 1024**3,
            primary_shards=2,
            max_shard_size_str="15gb",
            data_node_count=DATA_NODE_COUNT,
        ),
        "OK",
        lambda d: d is None,
    ),
])
def test_recommend(scenario, kwargs, expected_rec, check_detail):
    rec, detail = _recommend(**kwargs)
    assert rec == expected_rec, f"[{scenario}] expected '{expected_rec}', got '{rec}'"
    assert check_detail(detail), f"[{scenario}] unexpected detail '{detail}'"


def test_recommend_shard_size_increase_capped_at_50gb():
    """Shard size recommendation must never exceed 50 GB."""
    rec, detail = _recommend(
        avg_rotation_hours=0.5,   # rotating very fast
        avg_shard_size_bytes=40 * 1024**3,
        primary_shards=1,
        max_shard_size_str="40gb",
        data_node_count=DATA_NODE_COUNT,
    )
    assert rec == "increase max_primary_shard_size"
    assert detail == "50gb"


def test_recommend_shards_increase_capped_at_node_fraction():
    """Shard count recommendation must not exceed floor(0.6 * data_node_count)."""
    rec, detail = _recommend(
        avg_rotation_hours=0.1,   # extremely fast rotation
        avg_shard_size_bytes=MAX_SHARD_BYTES,
        primary_shards=1,
        max_shard_size_str="50gb",
        data_node_count=DATA_NODE_COUNT,
    )
    assert rec == "increase number_of_shards"
    assert int(detail) <= int(DATA_NODE_COUNT * 0.6)


# ---------------------------------------------------------------------------
# profile_data_streams — integration-level tests
# ---------------------------------------------------------------------------

# Helpers to build grouped + policies structures for profile tests

def _make_test_profile(
    name: str,
    creation_epoch_ms: int,
    size_bytes: int,
    primary_shards: int = 2,
    is_write_index: bool = False,
    data_stream: str | None = "logs-app",
    policy: str = "logs-default",
) -> "IndexProfile":
    from domain.es.models import IndexProfile
    return IndexProfile(
        name=name,
        policy=policy,
        phase="hot",
        index_age_days=1.0,
        phase_age_days=1.0,
        docs=1_000_000,
        size_bytes=size_bytes,
        primary_shards=primary_shards,
        is_write_index=is_write_index,
        data_stream=data_stream,
        creation_epoch_ms=creation_epoch_ms,
        health="green",
        status="open",
    )


# Base creation times: 4 indices, each 8h apart → 8h avg rotation
T0 = NOW_MS
T1 = T0 + 8 * 3_600_000    # +8h
T2 = T1 + 8 * 3_600_000    # +8h (closed)
T3 = T2 + 8 * 3_600_000    # write index

PROFILES_8H_ROTATION = [
    _make_test_profile(".ds-logs-app-000001", T0, 10 * 1024**3, is_write_index=False),
    _make_test_profile(".ds-logs-app-000002", T1, 10 * 1024**3, is_write_index=False),
    _make_test_profile(".ds-logs-app-000003", T2, 10 * 1024**3, is_write_index=False),
    _make_test_profile(".ds-logs-app-000004", T3, 5 * 1024**3,  is_write_index=True),
]

POLICY_HOT_30GB = {
    "logs-default": {
        "version": 1,
        "modified_date": "2024-01-01T00:00:00Z",
        "policy": {
            "phases": {
                "hot": {
                    "min_age": "0ms",
                    "actions": {
                        "rollover": {"max_primary_shard_size": "30gb"},
                    },
                }
            }
        },
    }
}

DATA_STREAMS_LOGS_APP = [
    RawDataStream.model_validate({
        "name": "logs-app",
        "template": "logs-app-default",
        "indices": [
            {"index_name": ".ds-logs-app-000001", "index_uuid": "u1"},
            {"index_name": ".ds-logs-app-000002", "index_uuid": "u2"},
            {"index_name": ".ds-logs-app-000003", "index_uuid": "u3"},
            {"index_name": ".ds-logs-app-000004", "index_uuid": "u4"},
        ],
    })
]


@pytest.mark.parametrize("scenario,rotation_hours,size_bytes,max_shard_str,"
                          "primary_shards,data_nodes,expected_rec", [
    (
        "ok: 8h rotation, within 6–24h window",
        8.0, 10 * 1024**3, "30gb", 2, 10, "OK",
    ),
    (
        "too fast: 2h rotation, shard below 50gb → increase shard size",
        2.0, 5 * 1024**3, "10gb", 2, 10, "increase max_primary_shard_size",
    ),
    (
        "too fast: 2h rotation, shard at 50gb, room for more shards → increase shards",
        2.0, MAX_SHARD_BYTES, "50gb", 2, 10, "increase number_of_shards",
    ),
    (
        "too fast: 2h rotation, both levers maxed → SPLIT",
        2.0, MAX_SHARD_BYTES, "50gb", 6, 10, "SPLIT into multiple independent indices",
    ),
    (
        "too slow: 48h rotation, multiple shards → decrease shards",
        48.0, 10 * 1024**3, "30gb", 4, 10, "decrease number_of_shards",
    ),
    (
        "too slow: 48h rotation, single shard → decrease shard size",
        48.0, 30 * 1024**3, "30gb", 1, 10, "decrease max_primary_shard_size",
    ),
])
@patch("ilm_review.time.time", return_value=NOW_S)
def test_profile_data_streams_recommendation(
    _mock_time, scenario, rotation_hours, size_bytes, max_shard_str,
    primary_shards, data_nodes, expected_rec,
):
    gap_ms = int(rotation_hours * 3_600_000)
    # Build 4 indices: 3 closed + 1 write, with the given rotation gap
    t = [T0, T0 + gap_ms, T0 + 2 * gap_ms, T0 + 3 * gap_ms]
    profs = [
        _make_test_profile(f".ds-logs-app-{i:06d}", t[i], size_bytes,
                           primary_shards=primary_shards,
                           is_write_index=(i == 3))
        for i in range(4)
    ]
    grouped = {"logs-default": profs}

    policy_raw = {
        "logs-default": {
            "version": 1,
            "modified_date": "2024-01-01T00:00:00Z",
            "policy": {
                "phases": {
                    "hot": {
                        "min_age": "0ms",
                        "actions": {"rollover": {"max_primary_shard_size": max_shard_str}},
                    }
                }
            },
        }
    }
    policies = parse_all_policies(policy_raw)
    result = profile_data_streams(grouped, policies, DATA_STREAMS_LOGS_APP, data_nodes)

    assert len(result) == 1, scenario
    dp = result[0]
    assert dp.recommendation == expected_rec, (
        f"[{scenario}] got '{dp.recommendation}', expected '{expected_rec}'"
    )


@patch("ilm_review.time.time", return_value=NOW_S)
def test_profile_data_streams_insufficient_history(_mock_time):
    """A single index (no rollovers yet) → insufficient history."""
    profs = [_make_test_profile(".ds-logs-app-000001", T0, 5 * 1024**3, is_write_index=True)]
    grouped = {"logs-default": profs}
    policies = parse_all_policies(POLICY_HOT_30GB)
    result = profile_data_streams(grouped, policies, DATA_STREAMS_LOGS_APP, DATA_NODE_COUNT)

    assert result[0].recommendation == "insufficient history"
    assert result[0].avg_rotation_hours is None


@patch("ilm_review.time.time", return_value=NOW_S)
def test_profile_data_streams_no_max_primary_shard_size(_mock_time):
    """Policy without max_primary_shard_size → cannot profile."""
    profs = [
        _make_test_profile(f".ds-logs-app-{i:06d}", T0 + i * 8 * 3_600_000,
                           10 * 1024**3, is_write_index=(i == 3))
        for i in range(4)
    ]
    grouped = {"logs-default": profs}
    policies = parse_all_policies(POLICY_7X)  # uses max_age + max_size, not max_primary_shard_size
    result = profile_data_streams(grouped, policies, DATA_STREAMS_LOGS_APP, DATA_NODE_COUNT)

    assert result[0].recommendation == "cannot profile"


@patch("ilm_review.time.time", return_value=NOW_S)
def test_profile_data_streams_template_name_propagated(_mock_time):
    """Template name from _data_stream is surfaced on DataStreamProfile."""
    grouped = {"logs-default": PROFILES_8H_ROTATION}
    policies = parse_all_policies(POLICY_HOT_30GB)
    result = profile_data_streams(grouped, policies, DATA_STREAMS_LOGS_APP, DATA_NODE_COUNT)

    assert result[0].template == "logs-app-default"


@patch("ilm_review.time.time", return_value=NOW_S)
def test_profile_data_streams_avg_rotation_hours(_mock_time):
    """Average rotation computed correctly from 3 closed intervals of 8h each."""
    grouped = {"logs-default": PROFILES_8H_ROTATION}
    policies = parse_all_policies(POLICY_HOT_30GB)
    result = profile_data_streams(grouped, policies, DATA_STREAMS_LOGS_APP, DATA_NODE_COUNT)

    assert result[0].avg_rotation_hours is not None
    assert abs(result[0].avg_rotation_hours - 8.0) < 0.01


@patch("ilm_review.time.time", return_value=NOW_S)
def test_profile_data_streams_avg_shard_size_excludes_write_index(_mock_time):
    """Write index (partially filled) is excluded from avg shard size."""
    grouped = {"logs-default": PROFILES_8H_ROTATION}
    policies = parse_all_policies(POLICY_HOT_30GB)
    result = profile_data_streams(grouped, policies, DATA_STREAMS_LOGS_APP, DATA_NODE_COUNT)

    # Write index has 5 GB but closed indices have 10 GB each over 2 shards → 5 GB/shard
    dp = result[0]
    assert dp.avg_shard_size_bytes is not None
    assert dp.avg_shard_size_bytes == 5 * 1024**3
