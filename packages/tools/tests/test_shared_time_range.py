"""Tests for slopbox_temporal._shared.time_range."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone

from slopbox_temporal._shared.time_range import TimeRange


def test_time_range_is_dataclass() -> None:
    tr = TimeRange(
        start=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )
    assert is_dataclass(tr)
    assert tr.start < tr.end


def test_time_range_round_trip() -> None:
    tr = TimeRange(
        start=datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc),
        end=datetime(2024, 6, 2, 12, 0, tzinfo=timezone.utc),
    )
    payload = asdict(tr)
    restored = TimeRange(**payload)
    assert restored == tr


def test_time_range_reexported_from_workflow_models() -> None:
    from slopbox_temporal.kibana_export.models import TimeRange as KibanaTR
    from slopbox_temporal.log_scrub.models import TimeRange as ScrubTR

    assert KibanaTR is TimeRange
    assert ScrubTR is TimeRange
