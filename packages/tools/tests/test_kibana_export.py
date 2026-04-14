"""Unit tests for slopbox_temporal.kibana_export.

All ES and S3 interactions are mocked — no running cluster or AWS account
required.  The tests follow the same MagicMock pattern used elsewhere in the
slopbox test suite.
"""

from __future__ import annotations

import gzip
import io
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from temporalio.exceptions import ApplicationError

from slopbox_temporal.kibana_export.activities import (
    MAX_EXPORT_DAYS,
    MAX_EXPORT_DOCS,
    _build_query,
    cleanup_partial_export,
    close_pit,
    export_chunk,
    open_pit,
    resolve_indices,
    validate_export_request,
    write_manifest,
)
from slopbox_temporal.kibana_export.models import (
    CleanupParams,
    ExportChunkParams,
    ExportChunkResult,
    KibanaLogExportRequest,
    TimeRange,
    WriteManifestParams,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _utc(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


BASE_REQUEST = KibanaLogExportRequest(
    index="logs-*",
    query={"term": {"env": "prod"}},
    time_range=TimeRange(start=_utc(2024, 1, 1), end=_utc(2024, 1, 7)),
    s3_bucket="my-bucket",
    s3_prefix="exports/run-001/",
)

BASE_ENV = {
    "ES_HOST": "https://localhost:9200",
    "ES_API_KEY": "dGVzdDprZXk=",
}


def _mock_es():
    """Return a MagicMock that stands in for an Elasticsearch client."""
    return MagicMock()


def _mock_s3():
    """Return a MagicMock for a boto3 S3 client."""
    return MagicMock()


# ---------------------------------------------------------------------------
# validate_export_request
# ---------------------------------------------------------------------------



@patch("slopbox_temporal.kibana_export.activities._build_es_client")
async def test_validate_time_range_too_long(mock_build, monkeypatch):
    """Time ranges exceeding MAX_EXPORT_DAYS raise a non-retryable ApplicationError."""
    long_range = TimeRange(
        start=_utc(2024, 1, 1),
        end=_utc(2024, 1, 1) + timedelta(days=MAX_EXPORT_DAYS + 1),
    )
    request = KibanaLogExportRequest(
        index="logs-*",
        query={},
        time_range=long_range,
        s3_bucket="b",
        s3_prefix="p/",
    )
    with pytest.raises(ApplicationError) as exc_info:
        await validate_export_request(request)
    assert exc_info.value.non_retryable is True
    assert "Acme" in str(exc_info.value)
    mock_build.assert_not_called()  # short-circuits before ES call



@patch("slopbox_temporal.kibana_export.activities._build_es_client")
async def test_validate_doc_count_exceeded(mock_build, monkeypatch):
    """Queries matching more than MAX_EXPORT_DOCS raise a non-retryable ApplicationError."""
    es = _mock_es()
    es.count.return_value = {"count": MAX_EXPORT_DOCS + 1}
    mock_build.return_value = es

    with pytest.raises(ApplicationError) as exc_info:
        await validate_export_request(BASE_REQUEST)
    assert exc_info.value.non_retryable is True
    assert "Fu" in str(exc_info.value)
    # Count is formatted with commas
    assert f"{MAX_EXPORT_DOCS + 1:,}" in str(exc_info.value)



@patch("slopbox_temporal.kibana_export.activities._build_es_client")
async def test_validate_passes_returns_count(mock_build):
    """Valid request returns the doc count as an int."""
    es = _mock_es()
    es.count.return_value = {"count": 42_000}
    mock_build.return_value = es

    result = await validate_export_request(BASE_REQUEST)

    assert result == 42_000
    es.count.assert_called_once()


# ---------------------------------------------------------------------------
# resolve_indices
# ---------------------------------------------------------------------------



@patch("slopbox_temporal.kibana_export.activities._build_es_client")
async def test_resolve_indices_returns_sorted_list(mock_build):
    """Concrete index names are returned alphabetically."""
    es = _mock_es()
    es.cat.indices.return_value = [
        {"index": "logs-2024.01.15-000001"},
        {"index": "logs-2024.01.01-000001"},
        {"index": "logs-2024.01.08-000001"},
    ]
    mock_build.return_value = es

    result = await resolve_indices("logs-*", BASE_REQUEST.time_range)

    assert result == [
        "logs-2024.01.01-000001",
        "logs-2024.01.08-000001",
        "logs-2024.01.15-000001",
    ]



@patch("slopbox_temporal.kibana_export.activities._build_es_client")
async def test_resolve_indices_empty_returns_empty_list(mock_build):
    """NotFoundError (pattern matches nothing) returns an empty list."""
    from elasticsearch import NotFoundError as ESNotFoundError

    es = _mock_es()
    es.cat.indices.side_effect = ESNotFoundError(404, "index_not_found_exception", {})
    mock_build.return_value = es

    result = await resolve_indices("logs-missing-*", BASE_REQUEST.time_range)

    assert result == []


# ---------------------------------------------------------------------------
# open_pit / close_pit
# ---------------------------------------------------------------------------



@patch("slopbox_temporal.kibana_export.activities._build_es_client")
async def test_open_pit_returns_id(mock_build):
    es = _mock_es()
    es.open_point_in_time.return_value = {"id": "abc123=="}
    mock_build.return_value = es

    pit_id = await open_pit("logs-2024.01.01-000001")

    assert pit_id == "abc123=="
    es.open_point_in_time.assert_called_once_with(
        index="logs-2024.01.01-000001", keep_alive="5m"
    )



@patch("slopbox_temporal.kibana_export.activities._build_es_client")
async def test_close_pit_calls_delete(mock_build):
    es = _mock_es()
    mock_build.return_value = es

    await close_pit("abc123==")

    es.close_point_in_time.assert_called_once_with(body={"id": "abc123=="})



@patch("slopbox_temporal.kibana_export.activities._build_es_client")
async def test_close_pit_swallows_errors(mock_build):
    """close_pit is best-effort; exceptions should not propagate."""
    es = _mock_es()
    es.close_point_in_time.side_effect = RuntimeError("gone")
    mock_build.return_value = es

    # Should not raise
    await close_pit("abc123==")


# ---------------------------------------------------------------------------
# _build_query (internal helper)
# ---------------------------------------------------------------------------


def test_build_query_wraps_user_query_in_bool_must():
    params = ExportChunkParams(
        pit_id="abc",
        search_after=None,
        query={"term": {"env": "prod"}},
        time_range=BASE_REQUEST.time_range,
        s3_bucket="b",
        s3_prefix="p/",
        chunk_size=1000,
        chunk_index=0,
    )
    body = _build_query(params)

    assert body["query"]["bool"]["must"] == [{"term": {"env": "prod"}}]
    assert len(body["query"]["bool"]["filter"]) == 1
    assert "range" in body["query"]["bool"]["filter"][0]


def test_build_query_empty_user_query_has_time_filter_only():
    params = ExportChunkParams(
        pit_id="abc",
        search_after=None,
        query={},
        time_range=BASE_REQUEST.time_range,
        s3_bucket="b",
        s3_prefix="p/",
        chunk_size=1000,
        chunk_index=0,
    )
    body = _build_query(params)

    assert body["query"]["bool"]["must"] == []
    assert len(body["query"]["bool"]["filter"]) == 1


def test_build_query_first_page_omits_search_after():
    params = ExportChunkParams(
        pit_id="abc",
        search_after=None,
        query={},
        time_range=BASE_REQUEST.time_range,
        s3_bucket="b",
        s3_prefix="p/",
        chunk_size=1000,
        chunk_index=0,
    )
    body = _build_query(params)
    assert "search_after" not in body


def test_build_query_passes_search_after_when_set():
    params = ExportChunkParams(
        pit_id="abc",
        search_after=[1704067200000, 42],
        query={},
        time_range=BASE_REQUEST.time_range,
        s3_bucket="b",
        s3_prefix="p/",
        chunk_size=1000,
        chunk_index=0,
    )
    body = _build_query(params)
    assert body["search_after"] == [1704067200000, 42]


# ---------------------------------------------------------------------------
# export_chunk
# ---------------------------------------------------------------------------


def _chunk_params(
    chunk_index: int = 0,
    search_after: list | None = None,
    chunk_size: int = 3,
) -> ExportChunkParams:
    return ExportChunkParams(
        pit_id="pit-id-1",
        search_after=search_after,
        query=BASE_REQUEST.query,
        time_range=BASE_REQUEST.time_range,
        s3_bucket=BASE_REQUEST.s3_bucket,
        s3_prefix=BASE_REQUEST.s3_prefix,
        chunk_size=chunk_size,
        chunk_index=chunk_index,
    )


def _fake_hits(n: int) -> list[dict]:
    return [
        {"_source": {"msg": f"log line {i}", "idx": i}, "sort": [i * 1000, i]}
        for i in range(n)
    ]



@patch("slopbox_temporal.kibana_export.activities.boto3")
@patch("slopbox_temporal.kibana_export.activities._build_es_client")
async def test_export_chunk_writes_ndjson_gz_to_s3(mock_build, mock_boto3):
    """Written S3 body is valid gzip NDJSON matching the hits' _source."""
    es = _mock_es()
    hits = _fake_hits(3)
    es.search.return_value = {"hits": {"hits": hits}, "pit_id": "pit-id-2"}
    mock_build.return_value = es

    s3 = _mock_s3()
    mock_boto3.client.return_value = s3

    result = await export_chunk(_chunk_params(chunk_index=0, chunk_size=10))

    assert result.docs_written == 3
    assert result.s3_key == "exports/run-001/chunk-0000.ndjson.gz"
    assert result.done is True  # 3 < chunk_size=10

    # Inspect the body written to S3
    put_call = s3.put_object.call_args
    body_bytes = put_call.kwargs["Body"].read()
    decompressed = gzip.decompress(body_bytes).decode()
    lines = [ln for ln in decompressed.strip().split("\n") if ln]
    assert len(lines) == 3
    for i, line in enumerate(lines):
        doc = json.loads(line)
        assert doc == hits[i]["_source"]



@patch("slopbox_temporal.kibana_export.activities.boto3")
@patch("slopbox_temporal.kibana_export.activities._build_es_client")
async def test_export_chunk_done_when_partial_page(mock_build, mock_boto3):
    """done=True when hits < chunk_size (last page)."""
    es = _mock_es()
    es.search.return_value = {"hits": {"hits": _fake_hits(5)}, "pit_id": "pit-id-2"}
    mock_build.return_value = es
    mock_boto3.client.return_value = _mock_s3()

    result = await export_chunk(_chunk_params(chunk_size=10))

    assert result.done is True



@patch("slopbox_temporal.kibana_export.activities.boto3")
@patch("slopbox_temporal.kibana_export.activities._build_es_client")
async def test_export_chunk_not_done_when_full_page(mock_build, mock_boto3):
    """done=False when hits == chunk_size (more pages may follow)."""
    es = _mock_es()
    es.search.return_value = {"hits": {"hits": _fake_hits(3)}, "pit_id": "pit-id-2"}
    mock_build.return_value = es
    mock_boto3.client.return_value = _mock_s3()

    result = await export_chunk(_chunk_params(chunk_size=3))

    assert result.done is False



@patch("slopbox_temporal.kibana_export.activities.boto3")
@patch("slopbox_temporal.kibana_export.activities._build_es_client")
async def test_export_chunk_done_when_empty_page(mock_build, mock_boto3):
    """done=True and s3_key=None when hits is empty (no S3 write)."""
    es = _mock_es()
    es.search.return_value = {"hits": {"hits": []}, "pit_id": "pit-id-2"}
    mock_build.return_value = es
    s3 = _mock_s3()
    mock_boto3.client.return_value = s3

    result = await export_chunk(_chunk_params())

    assert result.done is True
    assert result.s3_key is None
    assert result.docs_written == 0
    s3.put_object.assert_not_called()



@patch("slopbox_temporal.kibana_export.activities.boto3")
@patch("slopbox_temporal.kibana_export.activities._build_es_client")
async def test_export_chunk_propagates_new_pit_id(mock_build, mock_boto3):
    """The PIT id from the response is returned as new_pit_id."""
    es = _mock_es()
    es.search.return_value = {"hits": {"hits": _fake_hits(1)}, "pit_id": "pit-id-refreshed"}
    mock_build.return_value = es
    mock_boto3.client.return_value = _mock_s3()

    result = await export_chunk(_chunk_params())

    assert result.new_pit_id == "pit-id-refreshed"



@patch("slopbox_temporal.kibana_export.activities.boto3")
@patch("slopbox_temporal.kibana_export.activities._build_es_client")
async def test_export_chunk_search_after_set_from_last_hit(mock_build, mock_boto3):
    """search_after is the sort field of the last hit."""
    hits = _fake_hits(2)
    es = _mock_es()
    es.search.return_value = {"hits": {"hits": hits}, "pit_id": "pit-id-2"}
    mock_build.return_value = es
    mock_boto3.client.return_value = _mock_s3()

    result = await export_chunk(_chunk_params(chunk_size=10))

    assert result.search_after == hits[-1]["sort"]



@patch("slopbox_temporal.kibana_export.activities.boto3")
@patch("slopbox_temporal.kibana_export.activities._build_es_client")
async def test_export_chunk_s3_key_uses_padded_chunk_index(mock_build, mock_boto3):
    """S3 key uses 4-digit zero-padded chunk index."""
    es = _mock_es()
    es.search.return_value = {"hits": {"hits": _fake_hits(1)}, "pit_id": "p"}
    mock_build.return_value = es
    mock_boto3.client.return_value = _mock_s3()

    result = await export_chunk(_chunk_params(chunk_index=7))

    assert result.s3_key == "exports/run-001/chunk-0007.ndjson.gz"


# ---------------------------------------------------------------------------
# cleanup_partial_export
# ---------------------------------------------------------------------------



@patch("slopbox_temporal.kibana_export.activities.boto3")
async def test_cleanup_partial_export_calls_delete_objects(mock_boto3):
    s3 = _mock_s3()
    mock_boto3.client.return_value = s3

    keys = ["exports/run-001/chunk-0000.ndjson.gz", "exports/run-001/chunk-0001.ndjson.gz"]
    await cleanup_partial_export(CleanupParams(s3_bucket="my-bucket", s3_keys=keys))

    s3.delete_objects.assert_called_once_with(
        Bucket="my-bucket",
        Delete={
            "Objects": [{"Key": k} for k in keys],
            "Quiet": True,
        },
    )



@patch("slopbox_temporal.kibana_export.activities.boto3")
async def test_cleanup_partial_export_empty_list_is_noop(mock_boto3):
    """An empty key list should not make any S3 calls."""
    s3 = _mock_s3()
    mock_boto3.client.return_value = s3

    await cleanup_partial_export(CleanupParams(s3_bucket="my-bucket", s3_keys=[]))

    mock_boto3.client.assert_not_called()
    s3.delete_objects.assert_not_called()



@patch("slopbox_temporal.kibana_export.activities.boto3")
async def test_cleanup_partial_export_batches_large_key_lists(mock_boto3):
    """Key lists larger than 1 000 are split into multiple delete_objects calls."""
    s3 = _mock_s3()
    mock_boto3.client.return_value = s3

    keys = [f"exports/chunk-{i:04d}.ndjson.gz" for i in range(2500)]
    await cleanup_partial_export(CleanupParams(s3_bucket="my-bucket", s3_keys=keys))

    assert s3.delete_objects.call_count == 3  # 1000 + 1000 + 500


# ---------------------------------------------------------------------------
# write_manifest
# ---------------------------------------------------------------------------



@patch("slopbox_temporal.kibana_export.activities.boto3")
async def test_write_manifest_contains_expected_keys(mock_boto3):
    """manifest.json contains all required top-level fields."""
    s3 = _mock_s3()
    mock_boto3.client.return_value = s3

    chunks = [
        ExportChunkResult(
            chunk_index=0,
            docs_written=100,
            s3_key="exports/run-001/chunk-0000.ndjson.gz",
            new_pit_id="p",
            search_after=[1, 2],
            done=False,
        ),
        ExportChunkResult(
            chunk_index=1,
            docs_written=50,
            s3_key="exports/run-001/chunk-0001.ndjson.gz",
            new_pit_id="p",
            search_after=None,
            done=True,
        ),
    ]
    params = WriteManifestParams(
        request=BASE_REQUEST,
        resolved_indices=["logs-2024.01.01-000001"],
        chunks=chunks,
        total_docs=150,
    )

    manifest_key = await write_manifest(params)

    assert manifest_key == "exports/run-001/manifest.json"

    # Grab the body that was PUT as manifest.json
    calls = s3.put_object.call_args_list
    manifest_call = next(c for c in calls if c.kwargs["Key"].endswith("manifest.json"))
    manifest = json.loads(manifest_call.kwargs["Body"].decode())

    assert manifest["index"] == "logs-*"
    assert manifest["total_docs"] == 150
    assert manifest["total_chunks"] == 2
    assert manifest["chunk_size"] == 10_000
    assert len(manifest["chunks"]) == 2
    assert manifest["chunks"][0]["docs"] == 100
    assert "exported_at" in manifest
    assert "time_range" in manifest
    assert "resolved_indices" in manifest



@patch("slopbox_temporal.kibana_export.activities.boto3")
async def test_write_manifest_uploads_readme(mock_boto3):
    """README.txt is uploaded alongside manifest.json with concat instructions."""
    s3 = _mock_s3()
    mock_boto3.client.return_value = s3

    chunks = [
        ExportChunkResult(
            chunk_index=0,
            docs_written=10,
            s3_key="exports/run-001/chunk-0000.ndjson.gz",
            new_pit_id="p",
            search_after=None,
            done=True,
        )
    ]
    params = WriteManifestParams(
        request=BASE_REQUEST,
        resolved_indices=["logs-2024.01.01-000001"],
        chunks=chunks,
        total_docs=10,
    )

    await write_manifest(params)

    calls = s3.put_object.call_args_list
    readme_call = next(
        (c for c in calls if c.kwargs["Key"].endswith("README.txt")), None
    )
    assert readme_call is not None, "README.txt was not uploaded"

    readme_body = readme_call.kwargs["Body"].decode()
    assert "zcat" in readme_body
    assert "aws s3 cp" in readme_body
    assert "combined.ndjson" in readme_body
    assert BASE_REQUEST.s3_bucket in readme_body


# ---------------------------------------------------------------------------
# dry_run flag
# ---------------------------------------------------------------------------


@patch("slopbox_temporal.kibana_export.activities._build_es_client")
async def test_validate_dry_run_logs_mode(mock_build, caplog):
    """dry_run=True logs the dry-run notice and still returns the doc count."""
    import logging

    es = _mock_es()
    es.count.return_value = {"count": 500}
    mock_build.return_value = es

    dry_run_request = KibanaLogExportRequest(
        index="logs-*",
        query={},
        time_range=BASE_REQUEST.time_range,
        s3_bucket="b",
        s3_prefix="p/",
        dry_run=True,
    )

    with caplog.at_level(logging.INFO, logger="kibana_export"):
        result = await validate_export_request(dry_run_request)

    assert result == 500
    assert any("dry-run" in r.message for r in caplog.records)


@patch("slopbox_temporal.kibana_export.activities._build_es_client")
async def test_validate_non_dry_run_does_not_log_dry_run_mode(mock_build, caplog):
    """dry_run=False does not emit a dry-run log line."""
    import logging

    es = _mock_es()
    es.count.return_value = {"count": 500}
    mock_build.return_value = es

    live_request = KibanaLogExportRequest(
        index="logs-*",
        query={},
        time_range=BASE_REQUEST.time_range,
        s3_bucket="b",
        s3_prefix="p/",
        dry_run=False,
    )

    with caplog.at_level(logging.INFO, logger="kibana_export"):
        await validate_export_request(live_request)

    assert not any("dry-run" in r.message for r in caplog.records)
