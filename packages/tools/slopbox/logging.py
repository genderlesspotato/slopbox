"""Shared logging configuration for slopbox tools.

Reads LOG_FORMAT env var to select output mode:
  - 'human' (default): Rich-styled log records, integrates with Rich console output
  - 'json': Newline-delimited JSON to stderr; suitable for k8s and Temporal workflows

Usage in tool entry points::

    from slopbox.logging import configure_logging

    def main() -> None:
        log_format = configure_logging()
        ...
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Literal

LogFormat = Literal["human", "json"]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_log_format() -> LogFormat:
    """Read LOG_FORMAT from the environment; default 'human'."""
    return "json" if os.environ.get("LOG_FORMAT", "").lower() == "json" else "human"


def configure_logging(format: LogFormat | None = None) -> LogFormat:
    """Configure the root logger for the given format and return the active format.

    Call once at the top of main() before any logging or build_client() calls.
    Subsequent calls to logging.getLogger() throughout the codebase inherit this
    configuration automatically.

    Idempotent: clears existing root handlers before re-configuring, so calling
    twice (e.g. in tests) does not accumulate duplicate handlers.

    Args:
        format: 'human' or 'json'. If None, reads LOG_FORMAT env var.

    Returns:
        The active LogFormat ('human' or 'json').
    """
    log_format = format if format is not None else get_log_format()

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.WARNING)  # keep elasticsearch-py / urllib3 quiet

    if log_format == "json":
        handler: logging.Handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(_JsonFormatter())
    else:
        from rich.logging import RichHandler
        handler = RichHandler(show_time=False, show_path=False)

    handler.setLevel(logging.INFO)
    root.addHandler(handler)

    # Bring slopbox tool loggers up to INFO without opening the floodgates for
    # third-party libraries that live under different hierarchy roots.
    for name in ("slopbox", "ilm_review"):
        logging.getLogger(name).setLevel(logging.INFO)

    return log_format


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

class _JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        msg = record.getMessage()
        if record.exc_info:
            msg += "\n" + self.formatException(record.exc_info)
        return json.dumps({
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": msg,
        })
