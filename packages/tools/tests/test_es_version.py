"""Unit tests for slopbox_domain.es.version.ClusterVersion."""

import pytest

from slopbox_domain.es.version import ClusterVersion


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _info(version_str: str) -> dict:
    """Build a minimal client.info() response body for a given version string."""
    return {"version": {"number": version_str}}


def _v(major: int, minor: int, patch: int = 0) -> ClusterVersion:
    return ClusterVersion(major=major, minor=minor, patch=patch)


# ---------------------------------------------------------------------------
# from_info — parsing
# ---------------------------------------------------------------------------

def test_from_info_parses_es7():
    v = ClusterVersion.from_info(_info("7.17.0"))
    assert v.major == 7
    assert v.minor == 17
    assert v.patch == 0


def test_from_info_parses_es8():
    v = ClusterVersion.from_info(_info("8.15.3"))
    assert v.major == 8
    assert v.minor == 15
    assert v.patch == 3


def test_from_info_parses_es9():
    v = ClusterVersion.from_info(_info("9.0.1"))
    assert v.major == 9
    assert v.minor == 0
    assert v.patch == 1


def test_from_info_strips_snapshot_suffix():
    v = ClusterVersion.from_info(_info("8.15.0-SNAPSHOT"))
    assert v == ClusterVersion(major=8, minor=15, patch=0)


def test_from_info_strips_beta_suffix():
    v = ClusterVersion.from_info(_info("9.0.0-beta1"))
    assert v == ClusterVersion(major=9, minor=0, patch=0)


def test_from_info_strips_rc_suffix():
    v = ClusterVersion.from_info(_info("9.0.0-rc1"))
    assert v == ClusterVersion(major=9, minor=0, patch=0)


def test_from_info_missing_patch_defaults_to_zero():
    v = ClusterVersion.from_info(_info("8.15"))
    assert v.patch == 0


def test_from_info_raises_on_empty_string():
    with pytest.raises(ValueError, match="cannot parse"):
        ClusterVersion.from_info(_info(""))


def test_from_info_raises_on_non_numeric():
    with pytest.raises(ValueError, match="cannot parse"):
        ClusterVersion.from_info(_info("not-a-version"))


def test_from_info_raises_on_missing_version_key():
    with pytest.raises(ValueError, match="cannot parse"):
        ClusterVersion.from_info({})


# ---------------------------------------------------------------------------
# __str__
# ---------------------------------------------------------------------------

def test_str_representation():
    v = ClusterVersion(major=8, minor=15, patch=3)
    assert str(v) == "8.15.3"


def test_str_representation_zero_patch():
    v = ClusterVersion(major=7, minor=17, patch=0)
    assert str(v) == "7.17.0"


# ---------------------------------------------------------------------------
# Capability: uses_flat_indices_dir
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("major,minor,expected", [
    (7, 10, False),
    (7, 17, False),
    (8, 0,  True),
    (8, 15, True),
    (9, 0,  True),
])
def test_uses_flat_indices_dir(major, minor, expected):
    assert _v(major, minor).uses_flat_indices_dir is expected


# ---------------------------------------------------------------------------
# Capability: has_primary_shard_rollover
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("major,minor,expected", [
    (7, 10, False),
    (7, 17, False),
    (8, 0,  True),
    (8, 15, True),
    (9, 0,  True),
])
def test_has_primary_shard_rollover(major, minor, expected):
    assert _v(major, minor).has_primary_shard_rollover is expected


# ---------------------------------------------------------------------------
# Capability: has_frozen_tier  (ES 7.12+)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("major,minor,expected", [
    (7, 10, False),
    (7, 11, False),
    (7, 12, True),
    (7, 17, True),
    (8, 0,  True),
    (8, 15, True),
    (9, 0,  True),
])
def test_has_frozen_tier(major, minor, expected):
    assert _v(major, minor).has_frozen_tier is expected


# ---------------------------------------------------------------------------
# Capability: has_max_primary_shard_docs  (ES 8.2+)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("major,minor,expected", [
    (7, 17, False),
    (8, 0,  False),
    (8, 1,  False),
    (8, 2,  True),
    (8, 15, True),
    (9, 0,  True),
])
def test_has_max_primary_shard_docs(major, minor, expected):
    assert _v(major, minor).has_max_primary_shard_docs is expected


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------

def test_cluster_version_is_frozen():
    v = ClusterVersion(major=8, minor=15, patch=3)
    with pytest.raises(Exception):
        v.major = 9  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Equality
# ---------------------------------------------------------------------------

def test_cluster_version_equality():
    assert _v(8, 15, 3) == _v(8, 15, 3)


def test_cluster_version_inequality():
    assert _v(8, 15, 3) != _v(8, 15, 2)
    assert _v(8, 15, 3) != _v(9, 0, 0)
