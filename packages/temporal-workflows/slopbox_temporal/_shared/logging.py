"""Minimal JSON-to-stderr logging setup for Temporal worker processes.

Workers run in containers where structured logs are shipped to a log
aggregator (Loki, Datadog, ...).  This module stays lean on purpose: no Rich,
no tool-specific dependencies, just the ``slopbox.logging`` JSON formatter
contract (``timestamp``, ``level``, ``logger``, ``message``) so downstream
log pipelines see one format everywhere.

Idempotent — call ``configure_worker_logging()`` once at the top of
``main()``.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone


def configure_worker_logging() -> None:
    """Configure the root logger for a worker process.

    * Root at ``WARNING`` so third-party libraries (temporalio, elasticsearch,
      urllib3) stay quiet.
    * Handler at ``INFO`` so our own loggers (``slopbox_temporal.*`` and
      per-workflow loggers like ``kibana_export``, ``log_scrub``,
      ``node_drain_reboot``) produce useful output by default.
    * JSON-to-stderr format so log aggregators can parse each line.
    """
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.WARNING)

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_JsonFormatter())
    handler.setLevel(logging.INFO)
    root.addHandler(handler)

    for name in (
        "slopbox_temporal",
        "kibana_export",
        "log_scrub",
        "node_drain_reboot",
    ):
        logging.getLogger(name).setLevel(logging.INFO)


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        msg = record.getMessage()
        if record.exc_info:
            msg += "\n" + self.formatException(record.exc_info)
        return json.dumps(
            {
                "timestamp": datetime.fromtimestamp(
                    record.created, tz=timezone.utc
                ).isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": msg,
            }
        )
