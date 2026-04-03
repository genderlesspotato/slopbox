"""Unit tests for slopbox.formatting."""

import pytest

from slopbox.formatting import format_bytes, format_duration, health_style


# ---------------------------------------------------------------------------
# format_bytes
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("n, expected", [
    (0,                     "0.0 B"),
    (512,                   "512.0 B"),
    (1023,                  "1023.0 B"),
    (1024,                  "1.0 KB"),
    (1536,                  "1.5 KB"),
    (1_048_576,             "1.0 MB"),
    (1_073_741_824,         "1.0 GB"),
    (10_737_418_240,        "10.0 GB"),
    (1_099_511_627_776,     "1.0 TB"),
    (2_251_799_813_685_248, "2.0 PB"),
])
def test_format_bytes(n, expected):
    assert format_bytes(n) == expected


# ---------------------------------------------------------------------------
# format_duration
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("days, expected", [
    (-1.0,      "unknown"),
    (-0.1,      "unknown"),
    (0.0,       "0m"),
    (0.5 / 24,  "30m"),      # 30 minutes
    (1 / 24,    "1h 0m"),    # exactly 1 hour
    (1.5 / 24,  "1h 30m"),   # 1h 30m
    (23 / 24,   "23h 0m"),   # 23 hours
    (1.0,       "1d"),
    (1.5,       "1d 12h"),
    (7.0,       "7d"),
    (7.25,      "7d 6h"),
    (30.0,      "30d"),
    (365.0,     "365d"),
])
def test_format_duration(days, expected):
    assert format_duration(days) == expected


# ---------------------------------------------------------------------------
# health_style
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("health, expected", [
    ("green",   "green"),
    ("yellow",  "yellow"),
    ("red",     "red"),
    ("unknown", "white"),
    ("",        "white"),
])
def test_health_style(health, expected):
    assert health_style(health) == expected
