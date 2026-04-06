"""Raw Pydantic boundary models for Elasticsearch API responses.

These models validate and coerce the ES wire format — _cat APIs return all
numerics as strings, and field names use dots as separators. They own all
string-to-int coercion so that correlate_data() and similar pipeline
functions never need to handle it.

These types never leave the fetch/parse layer.
"""

from pydantic import BaseModel, ConfigDict, Field, field_validator


# ---------------------------------------------------------------------------
# ILM explain response shapes
# ---------------------------------------------------------------------------

class RawPhaseExecution(BaseModel):
    # The ILM explain API returns many extra fields (policy, version, etc.).
    # extra="ignore" keeps this robust to API evolution.
    model_config = ConfigDict(extra="ignore")

    modified_date_in_millis: int | None = None


class RawIlmExplainEntry(BaseModel):
    # The ILM explain API returns many extra fields (managed, action, step, etc.).
    # extra="ignore" keeps this robust to API evolution.
    model_config = ConfigDict(extra="ignore")

    index: str
    policy: str = "unknown"
    phase: str = "unknown"
    is_write_index: bool = False
    phase_execution: RawPhaseExecution | None = None


# ---------------------------------------------------------------------------
# _cat/indices response shapes
# ---------------------------------------------------------------------------

class RawCatIndexEntry(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    index: str
    docs_count: int = Field(alias="docs.count", default=0)
    store_size_bytes: int = Field(alias="store.size", default=0)
    creation_epoch_ms: int = Field(alias="creation.date.epoch", default=0)
    primary_shards: int = Field(alias="pri", default=1)
    health: str = "unknown"
    status: str = "unknown"

    @field_validator("docs_count", "store_size_bytes", "creation_epoch_ms", "primary_shards", mode="before")
    @classmethod
    def coerce_str_to_int(cls, v: str | int | None) -> int:
        return 0 if v is None else int(v)


# ---------------------------------------------------------------------------
# _data_stream response shapes
# ---------------------------------------------------------------------------

class RawDataStreamIndex(BaseModel):
    model_config = ConfigDict(extra="forbid")

    index_name: str
    index_uuid: str


class RawDataStream(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    template: str = "unknown"
    indices: list[RawDataStreamIndex] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# _cat/recovery response shapes
# ---------------------------------------------------------------------------

class RawCatRecoveryEntry(BaseModel):
    # _cat/recovery returns many fields; we only need `index` to cross-reference
    # with the cluster state.  extra="ignore" keeps this robust to API evolution.
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    index: str
    shard: str = "0"
    stage: str = "unknown"
