"""Elasticsearch domain objects.

These are the clean, typed domain objects constructed after raw API responses
have been validated and coerced by domain/es/types.py. Display strings
(size_human, index_age, phase_time, shard_size_human) are computed lazily from
raw numeric fields via @computed_field — never stored, but accessible as normal
attributes.
"""

from pydantic import BaseModel, ConfigDict, computed_field

from slopbox_domain.formatting import format_bytes, format_duration


class IndexProfile(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    policy: str
    phase: str
    index_age_days: float
    phase_age_days: float
    docs: int
    size_bytes: int
    primary_shards: int
    is_write_index: bool
    data_stream: str | None
    creation_epoch_ms: int
    health: str
    status: str

    @computed_field
    @property
    def size_human(self) -> str:
        return format_bytes(self.size_bytes)

    @computed_field
    @property
    def index_age(self) -> str:
        return format_duration(self.index_age_days)

    @computed_field
    @property
    def phase_time(self) -> str:
        return format_duration(self.phase_age_days)

    @computed_field
    @property
    def shard_size_bytes(self) -> int:
        shards = max(self.primary_shards, 1)
        return self.size_bytes // shards

    @computed_field
    @property
    def shard_size_human(self) -> str:
        return format_bytes(self.shard_size_bytes)
