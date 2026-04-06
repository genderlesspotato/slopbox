"""Unit tests for slopbox.client.build_client()."""

import logging

import pytest
from unittest.mock import patch, MagicMock

from slopbox.client import build_client


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_ENV = {
    "ES_HOST": "https://localhost:9200",
    "ES_API_KEY": "dGVzdDprZXk=",
}


def _env(**overrides):
    """Return a copy of BASE_ENV with overrides applied. Pass None to remove a key."""
    env = dict(BASE_ENV)
    for k, v in overrides.items():
        if v is None:
            env.pop(k, None)
        else:
            env[k] = v
    return env


# ---------------------------------------------------------------------------
# Validation — missing connection settings
# ---------------------------------------------------------------------------

def test_build_client_exits_without_host_or_cloud_id(monkeypatch, caplog):
    monkeypatch.delenv("ES_HOST", raising=False)
    monkeypatch.delenv("ES_CLOUD_ID", raising=False)
    monkeypatch.delenv("ES_API_KEY", raising=False)
    monkeypatch.delenv("ES_USERNAME", raising=False)
    monkeypatch.delenv("ES_PASSWORD", raising=False)
    with caplog.at_level(logging.ERROR, logger="slopbox.client"):
        with pytest.raises(SystemExit) as exc_info:
            build_client()
    assert exc_info.value.code == 1
    assert "ES_HOST or ES_CLOUD_ID" in caplog.text


# ---------------------------------------------------------------------------
# Validation — missing auth
# ---------------------------------------------------------------------------

def test_build_client_exits_without_any_auth(monkeypatch, caplog):
    monkeypatch.setenv("ES_HOST", "https://localhost:9200")
    monkeypatch.delenv("ES_API_KEY", raising=False)
    monkeypatch.delenv("ES_USERNAME", raising=False)
    monkeypatch.delenv("ES_PASSWORD", raising=False)
    with caplog.at_level(logging.ERROR, logger="slopbox.client"):
        with pytest.raises(SystemExit) as exc_info:
            build_client()
    assert exc_info.value.code == 1
    assert "ES_API_KEY or both ES_USERNAME and ES_PASSWORD" in caplog.text


def test_build_client_exits_with_username_only(monkeypatch):
    monkeypatch.setenv("ES_HOST", "https://localhost:9200")
    monkeypatch.setenv("ES_USERNAME", "elastic")
    monkeypatch.delenv("ES_API_KEY", raising=False)
    monkeypatch.delenv("ES_PASSWORD", raising=False)
    with pytest.raises(SystemExit):
        build_client()


# ---------------------------------------------------------------------------
# Successful construction — host + api_key
# ---------------------------------------------------------------------------

@patch("slopbox.client.Elasticsearch")
def test_build_client_host_and_api_key(mock_es, monkeypatch):
    monkeypatch.setenv("ES_HOST", "https://localhost:9200")
    monkeypatch.setenv("ES_API_KEY", "dGVzdDprZXk=")
    monkeypatch.delenv("ES_CLOUD_ID", raising=False)
    monkeypatch.delenv("ES_USERNAME", raising=False)
    monkeypatch.delenv("ES_PASSWORD", raising=False)

    build_client()

    mock_es.assert_called_once_with(hosts=["https://localhost:9200"], api_key="dGVzdDprZXk=")


# ---------------------------------------------------------------------------
# Successful construction — host + basic auth
# ---------------------------------------------------------------------------

@patch("slopbox.client.Elasticsearch")
def test_build_client_host_and_basic_auth(mock_es, monkeypatch):
    monkeypatch.setenv("ES_HOST", "https://localhost:9200")
    monkeypatch.setenv("ES_USERNAME", "elastic")
    monkeypatch.setenv("ES_PASSWORD", "changeme")
    monkeypatch.delenv("ES_CLOUD_ID", raising=False)
    monkeypatch.delenv("ES_API_KEY", raising=False)

    build_client()

    mock_es.assert_called_once_with(hosts=["https://localhost:9200"], basic_auth=("elastic", "changeme"))


# ---------------------------------------------------------------------------
# Successful construction — cloud_id overrides host
# ---------------------------------------------------------------------------

@patch("slopbox.client.Elasticsearch")
def test_build_client_cloud_id_takes_precedence_over_host(mock_es, monkeypatch):
    monkeypatch.setenv("ES_HOST", "https://localhost:9200")
    monkeypatch.setenv("ES_CLOUD_ID", "my-cluster:dXMtZWFzdC0xLmF3cy5mb3VuZC5pbyQ=")
    monkeypatch.setenv("ES_API_KEY", "dGVzdDprZXk=")
    monkeypatch.delenv("ES_USERNAME", raising=False)
    monkeypatch.delenv("ES_PASSWORD", raising=False)

    build_client()

    call_kwargs = mock_es.call_args[1]
    assert "cloud_id" in call_kwargs
    assert "hosts" not in call_kwargs


# ---------------------------------------------------------------------------
# api_key takes precedence over basic auth when both are set
# ---------------------------------------------------------------------------

@patch("slopbox.client.Elasticsearch")
def test_build_client_api_key_takes_precedence_over_basic_auth(mock_es, monkeypatch):
    monkeypatch.setenv("ES_HOST", "https://localhost:9200")
    monkeypatch.setenv("ES_API_KEY", "dGVzdDprZXk=")
    monkeypatch.setenv("ES_USERNAME", "elastic")
    monkeypatch.setenv("ES_PASSWORD", "changeme")
    monkeypatch.delenv("ES_CLOUD_ID", raising=False)

    build_client()

    call_kwargs = mock_es.call_args[1]
    assert "api_key" in call_kwargs
    assert "basic_auth" not in call_kwargs
