"""Unit tests for slopbox.logging."""

import json
import logging
import sys

import pytest
from rich.logging import RichHandler

from slopbox.logging import (
    LogFormat,
    _JsonFormatter,
    configure_logging,
    get_log_format,
)


# ---------------------------------------------------------------------------
# get_log_format
# ---------------------------------------------------------------------------

def test_get_log_format_defaults_to_human(monkeypatch):
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    assert get_log_format() == "human"


def test_get_log_format_empty_string_returns_human(monkeypatch):
    monkeypatch.setenv("LOG_FORMAT", "")
    assert get_log_format() == "human"


def test_get_log_format_json_when_env_set(monkeypatch):
    monkeypatch.setenv("LOG_FORMAT", "json")
    assert get_log_format() == "json"


def test_get_log_format_case_insensitive(monkeypatch):
    monkeypatch.setenv("LOG_FORMAT", "JSON")
    assert get_log_format() == "json"


def test_get_log_format_unknown_value_returns_human(monkeypatch):
    monkeypatch.setenv("LOG_FORMAT", "xml")
    assert get_log_format() == "human"


# ---------------------------------------------------------------------------
# configure_logging — return value and handler type
# ---------------------------------------------------------------------------

def test_configure_logging_returns_human_by_default(monkeypatch):
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    result = configure_logging()
    assert result == "human"


def test_configure_logging_returns_json_when_set(monkeypatch):
    monkeypatch.setenv("LOG_FORMAT", "json")
    result = configure_logging()
    assert result == "json"


def test_configure_logging_explicit_format_overrides_env(monkeypatch):
    monkeypatch.setenv("LOG_FORMAT", "json")
    result = configure_logging("human")
    assert result == "human"


def test_configure_logging_human_uses_rich_handler(monkeypatch):
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    configure_logging("human")
    root_handlers = logging.getLogger().handlers
    assert any(isinstance(h, RichHandler) for h in root_handlers)


def test_configure_logging_json_uses_stream_handler_to_stderr(monkeypatch):
    monkeypatch.setenv("LOG_FORMAT", "json")
    configure_logging("json")
    root_handlers = logging.getLogger().handlers
    stream_handlers = [h for h in root_handlers if isinstance(h, logging.StreamHandler) and not isinstance(h, RichHandler)]
    assert any(h.stream is sys.stderr for h in stream_handlers)


def test_configure_logging_json_handler_uses_json_formatter(monkeypatch):
    configure_logging("json")
    root_handlers = logging.getLogger().handlers
    stream_handlers = [h for h in root_handlers if isinstance(h, logging.StreamHandler) and not isinstance(h, RichHandler)]
    assert any(isinstance(h.formatter, _JsonFormatter) for h in stream_handlers)


# ---------------------------------------------------------------------------
# configure_logging — idempotency
# ---------------------------------------------------------------------------

def test_configure_logging_is_idempotent(monkeypatch):
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    configure_logging("human")
    configure_logging("human")
    assert len(logging.getLogger().handlers) == 1


def test_configure_logging_switching_format_replaces_handler(monkeypatch):
    configure_logging("human")
    configure_logging("json")
    root_handlers = logging.getLogger().handlers
    assert len(root_handlers) == 1
    assert not any(isinstance(h, RichHandler) for h in root_handlers)


# ---------------------------------------------------------------------------
# configure_logging — logger levels
# ---------------------------------------------------------------------------

def test_configure_logging_sets_slopbox_logger_to_info():
    configure_logging("human")
    assert logging.getLogger("slopbox").level == logging.INFO


def test_configure_logging_sets_ilm_review_logger_to_info():
    configure_logging("human")
    assert logging.getLogger("ilm_review").level == logging.INFO


def test_configure_logging_keeps_root_at_warning():
    configure_logging("human")
    assert logging.getLogger().level == logging.WARNING


# ---------------------------------------------------------------------------
# _JsonFormatter
# ---------------------------------------------------------------------------

def test_json_formatter_output_is_valid_json():
    fmt = _JsonFormatter()
    record = logging.LogRecord("test", logging.INFO, "", 0, "hello world", (), None)
    output = fmt.format(record)
    parsed = json.loads(output)  # must not raise
    assert isinstance(parsed, dict)


def test_json_formatter_has_required_fields():
    fmt = _JsonFormatter()
    record = logging.LogRecord("mylogger", logging.WARNING, "", 0, "test msg", (), None)
    parsed = json.loads(fmt.format(record))
    assert "timestamp" in parsed
    assert "level" in parsed
    assert "logger" in parsed
    assert "message" in parsed


def test_json_formatter_level_is_string():
    fmt = _JsonFormatter()
    record = logging.LogRecord("test", logging.ERROR, "", 0, "oops", (), None)
    parsed = json.loads(fmt.format(record))
    assert parsed["level"] == "ERROR"


def test_json_formatter_logger_name():
    fmt = _JsonFormatter()
    record = logging.LogRecord("slopbox.client", logging.INFO, "", 0, "msg", (), None)
    parsed = json.loads(fmt.format(record))
    assert parsed["logger"] == "slopbox.client"


def test_json_formatter_message_content():
    fmt = _JsonFormatter()
    record = logging.LogRecord("test", logging.INFO, "", 0, "hello %s", ("world",), None)
    parsed = json.loads(fmt.format(record))
    assert parsed["message"] == "hello world"


def test_json_formatter_timestamp_is_iso8601():
    fmt = _JsonFormatter()
    record = logging.LogRecord("test", logging.INFO, "", 0, "ts test", (), None)
    parsed = json.loads(fmt.format(record))
    ts = parsed["timestamp"]
    # Must be parseable as ISO-8601 with timezone offset
    from datetime import datetime
    dt = datetime.fromisoformat(ts)
    assert dt.tzinfo is not None
