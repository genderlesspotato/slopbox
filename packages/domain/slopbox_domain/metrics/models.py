"""VictoriaMetrics similarity domain objects.

These are the clean, typed domain objects that hold the results of comparing
two VictoriaMetrics clusters. Display strings are computed via @computed_field.
All models are frozen (immutable) and produced by profile_* functions in
vm_comparison.py.
"""

from pydantic import BaseModel, ConfigDict, computed_field


# ---------------------------------------------------------------------------
# Dimension 1: Metric coverage
# ---------------------------------------------------------------------------

class CoverageProfile(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    total_a: int                    # metric names in cluster A
    total_b: int                    # metric names in cluster B
    shared: int                     # names present in both
    only_in_a: int                  # count of names only in A
    only_in_b: int                  # count of names only in B
    jaccard: float                  # |A ∩ B| / |A ∪ B|, 0.0–1.0

    @computed_field
    @property
    def score_pct(self) -> str:
        return f"{self.jaccard * 100:.1f}%"


# ---------------------------------------------------------------------------
# Dimension 2: Series cardinality
# ---------------------------------------------------------------------------

class CardinalityProfile(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    total_series_a: int             # total active series in cluster A
    total_series_b: int             # total active series in cluster B
    metrics_checked: int            # shared metrics with cardinality data
    median_ratio: float             # median min/max series-count ratio, 0.0–1.0

    @computed_field
    @property
    def score_pct(self) -> str:
        return f"{self.median_ratio * 100:.1f}%"

    @computed_field
    @property
    def total_delta_pct(self) -> str:
        """Percentage difference in total series counts."""
        if self.total_series_a == 0 and self.total_series_b == 0:
            return "0.0%"
        denom = max(self.total_series_a, self.total_series_b)
        delta = abs(self.total_series_a - self.total_series_b) / denom
        return f"{delta * 100:.1f}%"


# ---------------------------------------------------------------------------
# Dimension 3: Sample density
# ---------------------------------------------------------------------------

class DensityWindowResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    window: str                     # e.g. "1h", "24h", "7d"
    metrics_checked: int
    median_ratio: float             # median min/max count ratio, 0.0–1.0
    gap_metrics: list[str]          # metrics with ratio < 0.8 (data gaps)

    @computed_field
    @property
    def score_pct(self) -> str:
        return f"{self.median_ratio * 100:.1f}%"


class DensityProfile(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    windows: list[DensityWindowResult]

    @computed_field
    @property
    def overall_ratio(self) -> float:
        if not self.windows:
            return 0.0
        return sum(w.median_ratio for w in self.windows) / len(self.windows)

    @computed_field
    @property
    def score_pct(self) -> str:
        return f"{self.overall_ratio * 100:.1f}%"


# ---------------------------------------------------------------------------
# Dimension 4: Value alignment
# ---------------------------------------------------------------------------

class ValueProfile(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    metrics_checked: int
    timestamps_checked: int
    samples_compared: int           # (metric, timestamp) pairs with data on both sides
    within_tolerance: int           # pairs where |avg_a - avg_b| / max(|avg_a|, |avg_b|) < tol
    tolerance: float                # fractional tolerance used (e.g. 0.02 = 2%)

    @computed_field
    @property
    def match_ratio(self) -> float:
        if self.samples_compared == 0:
            return 0.0
        return self.within_tolerance / self.samples_compared

    @computed_field
    @property
    def score_pct(self) -> str:
        return f"{self.match_ratio * 100:.1f}%"


# ---------------------------------------------------------------------------
# VM-internal /metrics diagnostic
# ---------------------------------------------------------------------------

class VmInternalMetric(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    value_a: float
    value_b: float
    ratio: float                    # value_a / value_b (or inf / nan if value_b == 0)

    @computed_field
    @property
    def ratio_pct(self) -> str:
        if self.ratio != self.ratio:  # NaN
            return "N/A"
        return f"{self.ratio * 100:.1f}%"


class VmInternalProfile(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    metrics: list[VmInternalMetric]


# ---------------------------------------------------------------------------
# Overall report
# ---------------------------------------------------------------------------

_WEIGHTS_WITH_VALUES = {
    "coverage": 0.25,
    "cardinality": 0.25,
    "density": 0.30,
    "values": 0.20,
}

_WEIGHTS_WITHOUT_VALUES = {
    "coverage": 0.27,
    "cardinality": 0.27,
    "density": 0.46,
}


class SimilarityReport(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    cluster_a: str                  # logical name from clusters.yaml
    cluster_b: str
    cluster_a_host: str
    cluster_b_host: str
    evaluated_at: str               # ISO-8601 UTC
    compare_window: str             # e.g. "24h"
    coverage: CoverageProfile
    cardinality: CardinalityProfile
    density: DensityProfile
    values: ValueProfile | None
    vm_internals: VmInternalProfile | None
    overall_score: float            # weighted average, 0.0–1.0

    @computed_field
    @property
    def overall_score_pct(self) -> str:
        return f"{self.overall_score * 100:.1f}%"

    @computed_field
    @property
    def grade(self) -> str:
        s = self.overall_score
        if s >= 0.95:
            return "excellent"
        if s >= 0.85:
            return "good"
        if s >= 0.70:
            return "degraded"
        return "critical"
