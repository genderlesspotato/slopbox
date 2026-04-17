"""Tests for slopbox_temporal._shared.es_client.build_es_client."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from temporalio.exceptions import ApplicationError

from slopbox_temporal._shared.es_client import build_es_client


def test_build_es_client_prefers_cloud_id_over_host(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ES_HOST", "https://host.example:9200")
    monkeypatch.setenv("ES_CLOUD_ID", "deployment:Zm9vYmFy")
    monkeypatch.setenv("ES_API_KEY", "abc")
    with patch("slopbox_temporal._shared.es_client.Elasticsearch") as mock_es:
        build_es_client()
    mock_es.assert_called_once_with(cloud_id="deployment:Zm9vYmFy", api_key="abc")


def test_build_es_client_falls_back_to_host(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ES_CLOUD_ID", raising=False)
    monkeypatch.setenv("ES_HOST", "https://host.example:9200")
    monkeypatch.setenv("ES_API_KEY", "abc")
    with patch("slopbox_temporal._shared.es_client.Elasticsearch") as mock_es:
        build_es_client()
    mock_es.assert_called_once_with("https://host.example:9200", api_key="abc")


def test_build_es_client_uses_basic_auth_when_no_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ES_CLOUD_ID", raising=False)
    monkeypatch.delenv("ES_API_KEY", raising=False)
    monkeypatch.setenv("ES_HOST", "https://host.example:9200")
    monkeypatch.setenv("ES_USERNAME", "alice")
    monkeypatch.setenv("ES_PASSWORD", "s3cret")
    with patch("slopbox_temporal._shared.es_client.Elasticsearch") as mock_es:
        build_es_client()
    mock_es.assert_called_once_with(
        "https://host.example:9200", basic_auth=("alice", "s3cret")
    )


def test_build_es_client_raises_nonretryable_when_host_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for var in ("ES_HOST", "ES_CLOUD_ID", "ES_API_KEY", "ES_USERNAME", "ES_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(ApplicationError) as exc_info:
        build_es_client()
    assert exc_info.value.non_retryable is True
    assert "ES_HOST or ES_CLOUD_ID" in str(exc_info.value)
