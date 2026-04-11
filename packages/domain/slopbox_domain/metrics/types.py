"""Raw Pydantic boundary models for VictoriaMetrics / Prometheus HTTP API responses.

These models validate the wire format returned by the Prometheus-compatible API.
They are consumed by fetch functions in vm_comparison.py and never escape the
fetch/parse layer.
"""

from pydantic import BaseModel, ConfigDict


# ---------------------------------------------------------------------------
# /api/v1/label/<name>/values
# ---------------------------------------------------------------------------

class RawLabelValuesResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    status: str
    data: list[str]


# ---------------------------------------------------------------------------
# /api/v1/status/tsdb
# ---------------------------------------------------------------------------

class RawTsdbEntry(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str
    value: int


class RawTsdbData(BaseModel):
    model_config = ConfigDict(extra="ignore")

    totalSeries: int
    totalSeriesAll: int
    seriesCountByMetricName: list[RawTsdbEntry]


class RawTsdbStatus(BaseModel):
    model_config = ConfigDict(extra="ignore")

    status: str
    data: RawTsdbData


# ---------------------------------------------------------------------------
# /api/v1/query (instant vector result)
# ---------------------------------------------------------------------------

class RawQueryResultVector(BaseModel):
    model_config = ConfigDict(extra="ignore")

    metric: dict[str, str]
    value: list  # [unix_timestamp_float, value_str]


class RawQueryData(BaseModel):
    model_config = ConfigDict(extra="ignore")

    resultType: str
    result: list[RawQueryResultVector]


class RawQueryResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    status: str
    data: RawQueryData
