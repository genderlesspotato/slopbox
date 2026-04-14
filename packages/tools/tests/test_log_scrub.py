"""Unit tests for slopbox_temporal.log_scrub.

All ES interactions are mocked — no running cluster required.  Tests follow
the same MagicMock / patch pattern used in test_kibana_export.py.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from temporalio.exceptions import ApplicationError

from slopbox_temporal.log_scrub.activities import (
    MAX_SCRUB_DAYS,
    TASK_POLL_INTERVAL_SECONDS,
    _build_delete_query,
    delete_index_docs,
    resolve_backing_indices,
    validate_scrub_request,
)
from slopbox_temporal.log_scrub.models import (
    DeleteIndexParams,
    DeleteIndexResult,
    LogScrubRequest,
    TimeRange,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _utc(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


BASE_TIME_RANGE = TimeRange(start=_utc(2024, 1, 1), end=_utc(2024, 1, 31))

BASE_REQUEST = LogScrubRequest(
    index="logs-myapp-*",
    query={"term": {"env": "prod"}},
    time_range=BASE_TIME_RANGE,
    requests_per_second=500.0,
    dry_run=True,
)

BASE_DELETE_PARAMS = DeleteIndexParams(
    index=".ds-logs-myapp-2024.01.01-000001",
    query={"term": {"env": "prod"}},
    time_range=BASE_TIME_RANGE,
    requests_per_second=500.0,
    dry_run=False,
)

BASE_ENV = {
    "ES_HOST": "https://localhost:9200",
    "ES_API_KEY": "dGVzdDprZXk=",
}


def _mock_es() -> MagicMock:
    return MagicMock()


def _mock_activity(heartbeat_details: list | None = None) -> MagicMock:
    """Build a mock for the ``temporalio.activity`` module."""
    m = MagicMock()
    m.info.return_value.heartbeat_details = heartbeat_details or []
    m.heartbeat = MagicMock()
    return m


# ---------------------------------------------------------------------------
# _build_delete_query (internal helper)
# ---------------------------------------------------------------------------


def test_build_delete_query_includes_time_filter():
    body = _build_delete_query(BASE_DELETE_PARAMS)
    filters = body["bool"]["filter"]
    assert len(filters) == 1
    assert "range" in filters[0]
    assert "@timestamp" in filters[0]["range"]


def test_build_delete_query_wraps_user_query_in_must():
    body = _build_delete_query(BASE_DELETE_PARAMS)
    assert body["bool"]["must"] == [{"term": {"env": "prod"}}]


def test_build_delete_query_empty_query_yields_empty_must():
    params = DeleteIndexParams(
        index="idx",
        query={},
        time_range=BASE_TIME_RANGE,
        requests_per_second=1000.0,
        dry_run=False,
    )
    body = _build_delete_query(params)
    assert body["bool"]["must"] == []
    assert len(body["bool"]["filter"]) == 1


def test_build_delete_query_time_range_uses_utc_iso():
    body = _build_delete_query(BASE_DELETE_PARAMS)
    r = body["bool"]["filter"][0]["range"]["@timestamp"]
    assert r["gte"] == "2024-01-01T00:00:00+00:00"
    assert r["lte"] == "2024-01-31T00:00:00+00:00"


# ---------------------------------------------------------------------------
# validate_scrub_request
# ---------------------------------------------------------------------------


@patch("slopbox_temporal.log_scrub.activities._build_es_client")
async def test_validate_time_range_too_long(mock_build):
    """Time ranges exceeding MAX_SCRUB_DAYS raise a non-retryable ApplicationError."""
    long_range = TimeRange(
        start=_utc(2023, 1, 1),
        end=_utc(2023, 1, 1) + timedelta(days=MAX_SCRUB_DAYS + 1),
    )
    request = LogScrubRequest(
        index="logs-*", query={}, time_range=long_range
    )
    with pytest.raises(ApplicationError) as exc_info:
        await validate_scrub_request(request)
    assert exc_info.value.non_retryable is True
    assert str(MAX_SCRUB_DAYS) in str(exc_info.value)
    mock_build.assert_not_called()


@patch("slopbox_temporal.log_scrub.activities._build_es_client")
async def test_validate_passes_returns_count(mock_build):
    """Valid request returns the matching document count."""
    es = _mock_es()
    es.count.return_value = {"count": 75_000}
    mock_build.return_value = es

    result = await validate_scrub_request(BASE_REQUEST)

    assert result == 75_000
    es.count.assert_called_once()


@patch("slopbox_temporal.log_scrub.activities._build_es_client")
async def test_validate_count_query_uses_time_range(mock_build):
    """The count body always includes the time range filter."""
    es = _mock_es()
    es.count.return_value = {"count": 0}
    mock_build.return_value = es

    await validate_scrub_request(BASE_REQUEST)

    body = es.count.call_args.kwargs["body"]
    filters = body["query"]["bool"]["filter"]
    assert any("range" in f for f in filters)


@patch("slopbox_temporal.log_scrub.activities._build_es_client")
async def test_validate_auth_failure_is_non_retryable(mock_build):
    """AuthenticationException from count is wrapped as non-retryable."""
    from elasticsearch import AuthenticationException

    es = _mock_es()
    es.count.side_effect = AuthenticationException(401, MagicMock(), {})
    mock_build.return_value = es

    with pytest.raises(ApplicationError) as exc_info:
        await validate_scrub_request(BASE_REQUEST)
    assert exc_info.value.non_retryable is True


@patch("slopbox_temporal.log_scrub.activities._build_es_client")
async def test_validate_dry_run_logs_mode(mock_build, caplog):
    """dry_run=True logs the dry-run notice and still returns count."""
    import logging

    es = _mock_es()
    es.count.return_value = {"count": 10}
    mock_build.return_value = es

    with caplog.at_level(logging.INFO, logger="log_scrub"):
        result = await validate_scrub_request(BASE_REQUEST)

    assert result == 10
    assert any("dry-run" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# resolve_backing_indices
# ---------------------------------------------------------------------------


@patch("slopbox_temporal.log_scrub.activities._build_es_client")
async def test_resolve_returns_indices_sorted_oldest_first(mock_build):
    """Indices are returned sorted by creation date ascending."""
    es = _mock_es()
    es.cat.indices.return_value = [
        {"index": ".ds-logs-myapp-2024.01.15-000002", "creation.date": "1705276800000"},
        {"index": ".ds-logs-myapp-2024.01.01-000001", "creation.date": "1704067200000"},
    ]
    es.indices.get_data_stream.return_value = {"data_streams": []}
    mock_build.return_value = es

    result = await resolve_backing_indices(BASE_REQUEST)

    assert result == [
        ".ds-logs-myapp-2024.01.01-000001",
        ".ds-logs-myapp-2024.01.15-000002",
    ]


@patch("slopbox_temporal.log_scrub.activities._build_es_client")
async def test_resolve_empty_pattern_returns_empty_list(mock_build):
    """NotFoundError (pattern matches nothing) returns an empty list."""
    from elasticsearch import NotFoundError as ESNotFoundError

    es = _mock_es()
    es.cat.indices.side_effect = ESNotFoundError(404, "index_not_found_exception", {})
    mock_build.return_value = es

    result = await resolve_backing_indices(BASE_REQUEST)

    assert result == []


@patch("slopbox_temporal.log_scrub.activities._build_es_client")
async def test_resolve_excludes_write_index(mock_build):
    """The last index in each data stream (write index) is excluded."""
    es = _mock_es()
    es.cat.indices.return_value = [
        {"index": ".ds-logs-myapp-2024.01.01-000001", "creation.date": "1704067200000"},
        {"index": ".ds-logs-myapp-2024.01.15-000002", "creation.date": "1705276800000"},
    ]
    es.indices.get_data_stream.return_value = {
        "data_streams": [
            {
                "name": "logs-myapp",
                "indices": [
                    {"index_name": ".ds-logs-myapp-2024.01.01-000001"},
                    {"index_name": ".ds-logs-myapp-2024.01.15-000002"},
                ],
            }
        ]
    }
    mock_build.return_value = es

    result = await resolve_backing_indices(BASE_REQUEST)

    assert result == [".ds-logs-myapp-2024.01.01-000001"]
    assert ".ds-logs-myapp-2024.01.15-000002" not in result


@patch("slopbox_temporal.log_scrub.activities._build_es_client")
async def test_resolve_excludes_indices_created_after_window_end(mock_build):
    """Indices created after time_range.end are skipped."""
    # window_end = 2024-01-31 00:00:00 UTC = 1706659200000 ms
    es = _mock_es()
    es.cat.indices.return_value = [
        {"index": ".ds-logs-myapp-2024.01.01-000001", "creation.date": "1704067200000"},
        # Created 2024-02-15 — after the window
        {"index": ".ds-logs-myapp-2024.02.15-000002", "creation.date": "1707955200000"},
    ]
    es.indices.get_data_stream.return_value = {"data_streams": []}
    mock_build.return_value = es

    result = await resolve_backing_indices(BASE_REQUEST)

    assert result == [".ds-logs-myapp-2024.01.01-000001"]
    assert ".ds-logs-myapp-2024.02.15-000002" not in result


@patch("slopbox_temporal.log_scrub.activities._build_es_client")
async def test_resolve_handles_missing_data_stream_api(mock_build, caplog):
    """Failure from get_data_stream logs a warning but does not abort."""
    import logging

    es = _mock_es()
    es.cat.indices.return_value = [
        {"index": "my-plain-index", "creation.date": "1704067200000"},
    ]
    es.indices.get_data_stream.side_effect = RuntimeError("not a data stream")
    mock_build.return_value = es

    with caplog.at_level(logging.WARNING, logger="log_scrub"):
        result = await resolve_backing_indices(BASE_REQUEST)

    assert result == ["my-plain-index"]
    assert any("write-index exclusion skipped" in r.message for r in caplog.records)


@patch("slopbox_temporal.log_scrub.activities._build_es_client")
async def test_resolve_auth_failure_is_non_retryable(mock_build):
    """AuthenticationException from cat.indices is wrapped as non-retryable."""
    from elasticsearch import AuthenticationException

    es = _mock_es()
    es.cat.indices.side_effect = AuthenticationException(401, MagicMock(), {})
    mock_build.return_value = es

    with pytest.raises(ApplicationError) as exc_info:
        await resolve_backing_indices(BASE_REQUEST)
    assert exc_info.value.non_retryable is True


# ---------------------------------------------------------------------------
# delete_index_docs — dry-run
# ---------------------------------------------------------------------------


async def test_delete_dry_run_returns_zero_without_es_call():
    """dry_run=True returns immediately with deleted=0; no ES client is built."""
    params = DeleteIndexParams(
        index=".ds-logs-myapp-2024.01.01-000001",
        query={"term": {"env": "prod"}},
        time_range=BASE_TIME_RANGE,
        requests_per_second=1000.0,
        dry_run=True,
    )
    with patch("slopbox_temporal.log_scrub.activities._build_es_client") as mock_build:
        result = await delete_index_docs(params)

    assert result.deleted == 0
    assert result.took_ms == 0
    assert result.index == params.index
    mock_build.assert_not_called()


# ---------------------------------------------------------------------------
# delete_index_docs — happy path (submit + poll)
# ---------------------------------------------------------------------------


@patch("slopbox_temporal.log_scrub.activities.asyncio.sleep", new_callable=AsyncMock)
@patch("slopbox_temporal.log_scrub.activities.activity")
@patch("slopbox_temporal.log_scrub.activities._build_es_client")
async def test_delete_submits_dbq_and_polls_to_completion(
    mock_build, mock_activity_mod, mock_sleep
):
    """Submit DBQ, poll once (not done), poll again (done) → return result."""
    mock_activity_mod.info.return_value.heartbeat_details = []

    es = _mock_es()
    es.delete_by_query.return_value = {"task": "node1:42"}
    es.tasks.get.side_effect = [
        {"completed": False},
        {"completed": True, "response": {"deleted": 12_000, "took": 3_500}},
    ]
    mock_build.return_value = es

    result = await delete_index_docs(BASE_DELETE_PARAMS)

    assert result.deleted == 12_000
    assert result.took_ms == 3_500
    assert result.index == BASE_DELETE_PARAMS.index
    assert es.tasks.get.call_count == 2


@patch("slopbox_temporal.log_scrub.activities.asyncio.sleep", new_callable=AsyncMock)
@patch("slopbox_temporal.log_scrub.activities.activity")
@patch("slopbox_temporal.log_scrub.activities._build_es_client")
async def test_delete_passes_correct_dbq_parameters(
    mock_build, mock_activity_mod, mock_sleep
):
    """DBQ is submitted with conflicts=proceed, slices=auto, and the right throttle."""
    mock_activity_mod.info.return_value.heartbeat_details = []

    es = _mock_es()
    es.delete_by_query.return_value = {"task": "node1:1"}
    es.tasks.get.return_value = {
        "completed": True,
        "response": {"deleted": 0, "took": 1},
    }
    mock_build.return_value = es

    await delete_index_docs(BASE_DELETE_PARAMS)

    call_kwargs = es.delete_by_query.call_args.kwargs
    assert call_kwargs["wait_for_completion"] is False
    assert call_kwargs["conflicts"] == "proceed"
    assert call_kwargs["slices"] == "auto"
    assert call_kwargs["requests_per_second"] == BASE_DELETE_PARAMS.requests_per_second


@patch("slopbox_temporal.log_scrub.activities.asyncio.sleep", new_callable=AsyncMock)
@patch("slopbox_temporal.log_scrub.activities.activity")
@patch("slopbox_temporal.log_scrub.activities._build_es_client")
async def test_delete_heartbeats_task_id_after_submit_and_on_each_poll(
    mock_build, mock_activity_mod, mock_sleep
):
    """Task id is heartbeated right after submission and after every tasks.get call."""
    mock_activity_mod.info.return_value.heartbeat_details = []

    es = _mock_es()
    es.delete_by_query.return_value = {"task": "node1:99"}
    es.tasks.get.side_effect = [
        {"completed": False},
        {"completed": True, "response": {"deleted": 1, "took": 10}},
    ]
    mock_build.return_value = es

    await delete_index_docs(BASE_DELETE_PARAMS)

    # heartbeat called: once after submit + once per poll iteration = 3 total
    assert mock_activity_mod.heartbeat.call_count == 3
    for c in mock_activity_mod.heartbeat.call_args_list:
        assert c.args[0] == "node1:99"


# ---------------------------------------------------------------------------
# delete_index_docs — heartbeat recovery
# ---------------------------------------------------------------------------


@patch("slopbox_temporal.log_scrub.activities.asyncio.sleep", new_callable=AsyncMock)
@patch("slopbox_temporal.log_scrub.activities.activity")
@patch("slopbox_temporal.log_scrub.activities._build_es_client")
async def test_delete_resumes_existing_task_from_heartbeat(
    mock_build, mock_activity_mod, mock_sleep
):
    """heartbeat_details contains a task id → skip submission, poll existing task."""
    mock_activity_mod.info.return_value.heartbeat_details = ["node1:42"]

    es = _mock_es()
    # First tasks.get (recovery probe): task in progress
    # Second tasks.get (poll loop): completed
    es.tasks.get.side_effect = [
        {"completed": False},
        {"completed": True, "response": {"deleted": 5_000, "took": 800}},
    ]
    mock_build.return_value = es

    result = await delete_index_docs(BASE_DELETE_PARAMS)

    es.delete_by_query.assert_not_called()
    assert result.deleted == 5_000


@patch("slopbox_temporal.log_scrub.activities.asyncio.sleep", new_callable=AsyncMock)
@patch("slopbox_temporal.log_scrub.activities.activity")
@patch("slopbox_temporal.log_scrub.activities._build_es_client")
async def test_delete_returns_immediately_when_recovered_task_already_complete(
    mock_build, mock_activity_mod, mock_sleep
):
    """If the recovered task is already complete, return without polling."""
    mock_activity_mod.info.return_value.heartbeat_details = ["node1:42"]

    es = _mock_es()
    es.tasks.get.return_value = {
        "completed": True,
        "response": {"deleted": 9_999, "took": 500},
    }
    mock_build.return_value = es

    result = await delete_index_docs(BASE_DELETE_PARAMS)

    es.delete_by_query.assert_not_called()
    assert es.tasks.get.call_count == 1
    assert result.deleted == 9_999
    # No polling sleep needed
    mock_sleep.assert_not_called()


@patch("slopbox_temporal.log_scrub.activities.asyncio.sleep", new_callable=AsyncMock)
@patch("slopbox_temporal.log_scrub.activities.activity")
@patch("slopbox_temporal.log_scrub.activities._build_es_client")
async def test_delete_resubmits_when_recovered_task_not_found(
    mock_build, mock_activity_mod, mock_sleep
):
    """Task id in heartbeat details but tasks.get raises → resubmit a fresh DBQ."""
    mock_activity_mod.info.return_value.heartbeat_details = ["node1:stale"]

    es = _mock_es()
    # Recovery probe: task not found
    # Poll loop: completed on first check
    es.tasks.get.side_effect = [
        RuntimeError("task not found"),
        {"completed": True, "response": {"deleted": 200, "took": 50}},
    ]
    es.delete_by_query.return_value = {"task": "node2:7"}
    mock_build.return_value = es

    result = await delete_index_docs(BASE_DELETE_PARAMS)

    es.delete_by_query.assert_called_once()
    assert result.deleted == 200


# ---------------------------------------------------------------------------
# delete_index_docs — non-retryable errors on submission
# ---------------------------------------------------------------------------


@patch("slopbox_temporal.log_scrub.activities.activity")
@patch("slopbox_temporal.log_scrub.activities._build_es_client")
async def test_delete_auth_failure_on_submit_is_non_retryable(
    mock_build, mock_activity_mod
):
    """AuthenticationException during DBQ submission → non-retryable ApplicationError."""
    from elasticsearch import AuthenticationException

    mock_activity_mod.info.return_value.heartbeat_details = []

    es = _mock_es()
    es.delete_by_query.side_effect = AuthenticationException(401, MagicMock(), {})
    mock_build.return_value = es

    with pytest.raises(ApplicationError) as exc_info:
        await delete_index_docs(BASE_DELETE_PARAMS)
    assert exc_info.value.non_retryable is True


@patch("slopbox_temporal.log_scrub.activities.activity")
@patch("slopbox_temporal.log_scrub.activities._build_es_client")
async def test_delete_not_found_on_submit_is_non_retryable(
    mock_build, mock_activity_mod
):
    """NotFoundError during DBQ submission (index vanished) → non-retryable ApplicationError."""
    from elasticsearch import NotFoundError as ESNotFoundError

    mock_activity_mod.info.return_value.heartbeat_details = []

    es = _mock_es()
    es.delete_by_query.side_effect = ESNotFoundError(404, MagicMock(), {})
    mock_build.return_value = es

    with pytest.raises(ApplicationError) as exc_info:
        await delete_index_docs(BASE_DELETE_PARAMS)
    assert exc_info.value.non_retryable is True
    assert BASE_DELETE_PARAMS.index in str(exc_info.value)


# ---------------------------------------------------------------------------
# delete_index_docs — delete body includes time filter
# ---------------------------------------------------------------------------


@patch("slopbox_temporal.log_scrub.activities.asyncio.sleep", new_callable=AsyncMock)
@patch("slopbox_temporal.log_scrub.activities.activity")
@patch("slopbox_temporal.log_scrub.activities._build_es_client")
async def test_delete_body_always_includes_time_range_filter(
    mock_build, mock_activity_mod, mock_sleep
):
    """The delete body sent to ES always contains the time range as a filter."""
    mock_activity_mod.info.return_value.heartbeat_details = []

    es = _mock_es()
    es.delete_by_query.return_value = {"task": "n:1"}
    es.tasks.get.return_value = {
        "completed": True,
        "response": {"deleted": 0, "took": 1},
    }
    mock_build.return_value = es

    await delete_index_docs(BASE_DELETE_PARAMS)

    body = es.delete_by_query.call_args.kwargs["body"]
    assert "filter" in body["query"]["bool"]
    filters = body["query"]["bool"]["filter"]
    assert any("range" in f and "@timestamp" in f["range"] for f in filters)
