"""Plumbing shared by all slopbox Temporal workflows.

Theme-agnostic helpers that any workflow is free to reuse:

* ``time_range.TimeRange`` — UTC-aware start/end dataclass used by multiple
  workflows for scoping an operation.
* ``es_client.build_es_client`` — construct an Elasticsearch client from the
  standard ES_* environment variables; raises ``ApplicationError`` with
  ``non_retryable=True`` on misconfiguration.
* ``retry.DEFAULT_RETRY`` — the default RetryPolicy used for short-lived
  activities (validation, resolution, metadata fetches).
"""
