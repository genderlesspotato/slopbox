"""Default retry policy for slopbox Temporal workflows.

Short-lived activities (validation, resolution, metadata fetches) share a
single conservative policy: 5s initial, 2.0 backoff, 30s cap, 3 attempts.

Workflow-specific policies — e.g. long-running delete-by-query that needs
more attempts because each retry re-attaches to an existing ES task — stay
local to the workflow that owns them.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio.common import RetryPolicy


DEFAULT_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=3,
)
