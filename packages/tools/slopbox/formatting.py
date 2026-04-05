"""Shared formatting utilities for slopbox tools.

Re-exports all formatting helpers from slopbox_domain.formatting so that
tool scripts can import from a single location regardless of which package
owns the implementation.
"""

from slopbox_domain.formatting import (  # noqa: F401
    format_bytes,
    format_duration,
    phase_style,
    health_style,
)
