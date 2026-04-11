"""VictoriaMetrics HTTP client.

Thin wrapper around the Prometheus-compatible HTTP API exposed by VictoriaMetrics.
No authentication is needed for internal clusters. All methods raise
requests.RequestException on network or HTTP errors — callers are responsible for
catching these at the tool boundary (main()).

Entry point:
    build_vm_client(config) → VmClient
"""

import logging
from dataclasses import dataclass, field

import requests

from slopbox_domain.metrics.cluster import VictoriaMetricsClusterConfig
from slopbox_domain.metrics.types import (
    RawLabelValuesResponse,
    RawQueryResponse,
    RawTsdbStatus,
)

logger = logging.getLogger("slopbox.vm_client")


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class VmClient:
    """HTTP client for one VictoriaMetrics instance.

    All methods raise requests.HTTPError for non-2xx responses and
    requests.RequestException for network failures.

    Thread-safe: each method creates a fresh request; the session is shared
    for connection keep-alive only and carries no mutable state.
    """
    host: str           # http(s)://host:port, no trailing slash
    name: str           # logical cluster name, used only in log messages
    timeout: float = 30.0
    _session: requests.Session = field(default_factory=requests.Session, compare=False, hash=False)

    def query(self, expr: str, time: float | None = None) -> RawQueryResponse:
        """Execute an instant PromQL query.

        Args:
            expr: PromQL expression, e.g. 'count({__name__=~"m1|m2"}) by (__name__)'
            time: Unix timestamp for the query evaluation point.
                  Defaults to 'now' (VM server time) when None.

        Returns:
            Parsed RawQueryResponse with resultType="vector".
        """
        params: dict[str, str | float] = {"query": expr}
        if time is not None:
            params["time"] = time

        resp = self._session.get(
            f"{self.host}/api/v1/query",
            params=params,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return RawQueryResponse.model_validate(resp.json())

    def label_values(self, label: str) -> list[str]:
        """Return all known values for *label* across all time series.

        Equivalent to GET /api/v1/label/<label>/values.
        """
        resp = self._session.get(
            f"{self.host}/api/v1/label/{label}/values",
            timeout=self.timeout,
        )
        resp.raise_for_status()
        parsed = RawLabelValuesResponse.model_validate(resp.json())
        return parsed.data

    def tsdb_status(self, top_n: int = 100) -> RawTsdbStatus:
        """Return TSDB cardinality statistics.

        topN controls the number of entries returned per cardinality list
        (seriesCountByMetricName, seriesCountByLabelName, etc.).
        """
        resp = self._session.get(
            f"{self.host}/api/v1/status/tsdb",
            params={"topN": top_n},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return RawTsdbStatus.model_validate(resp.json())

    def scrape_metrics(self) -> dict[str, float]:
        """Scrape VM's own Prometheus-format /metrics endpoint.

        Returns a flat dict of {metric_name: float_value} for simple (non-labelled)
        metrics. Labelled metrics (those with '{') are skipped — this is sufficient
        for the internal diagnostic signals we care about.
        """
        resp = self._session.get(
            f"{self.host}/metrics",
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return _parse_prometheus_text(resp.text)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def build_vm_client(config: VictoriaMetricsClusterConfig) -> VmClient:
    """Construct a VmClient from a VictoriaMetricsClusterConfig."""
    return VmClient(host=config.host, name=config.name)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_prometheus_text(text: str) -> dict[str, float]:
    """Parse Prometheus text-format /metrics output into {name: value}.

    Only simple (label-free) metrics are returned. Lines starting with '#'
    are skipped (HELP/TYPE comments). Labelled metrics are skipped.
    """
    result: dict[str, float] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "{" in line:
            # Labelled metric — skip for the internal diagnostic pass.
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        name, raw_value = parts[0], parts[1]
        try:
            result[name] = float(raw_value)
        except ValueError:
            continue
    return result
