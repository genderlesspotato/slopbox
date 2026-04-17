"""Shared ``TimeRange`` dataclass used across workflows.

Plain dataclass so Temporal's built-in JSON converter handles it without
pulling Pydantic into the worker image.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class TimeRange:
    start: datetime  # UTC-aware
    end: datetime    # UTC-aware
