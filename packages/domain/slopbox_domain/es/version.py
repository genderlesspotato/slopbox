"""Elasticsearch cluster version and capability predicates.

ClusterVersion is the single authoritative place where ES version numbers are
compared. Everything else — tool scripts, fetch functions, domain logic — must
ask "can this cluster do X?" via capability properties rather than branching on
major/minor integers directly.

Lifecycle rule: when a new ES major is released, add or update predicates here.
No tool code should need to change unless that release introduces a genuinely
new capability the tool wants to exploit.

Usage::

    from slopbox_domain.es.version import ClusterVersion

    version = ClusterVersion.from_info(client.info().body)
    print(version)                          # "8.15.3"
    version.has_primary_shard_rollover      # True
    version.uses_flat_indices_dir           # True
"""

from pydantic import BaseModel, ConfigDict


# ---------------------------------------------------------------------------
# ClusterVersion
# ---------------------------------------------------------------------------

class ClusterVersion(BaseModel):
    """Parsed Elasticsearch cluster version with capability predicates."""

    model_config = ConfigDict(frozen=True)

    major: int
    minor: int
    patch: int

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def from_info(cls, info: dict) -> "ClusterVersion":
        """Parse from the body of a client.info() response.

        Handles pre-release suffixes (``-SNAPSHOT``, ``-beta1``, etc.) and
        missing patch components. Raises ``ValueError`` for strings that
        cannot be parsed at all.
        """
        version_str: str = info.get("version", {}).get("number", "")
        # Strip pre-release suffix: "8.15.0-SNAPSHOT" → "8.15.0"
        clean = version_str.split("-")[0]
        parts = clean.split(".")
        if len(parts) < 2 or not all(p.isdigit() for p in parts[:3] if p):
            raise ValueError(f"cannot parse ES version string: {version_str!r}")
        return cls(
            major=int(parts[0]),
            minor=int(parts[1]),
            patch=int(parts[2]) if len(parts) > 2 else 0,
        )

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"

    # ------------------------------------------------------------------
    # Capability predicates — the ONLY place version numbers are compared
    # ------------------------------------------------------------------

    @property
    def uses_flat_indices_dir(self) -> bool:
        """ES 8+: data directory layout is ``<root>/indices/``.

        ES 7 used ``<root>/nodes/0/indices/``.
        """
        return self.major >= 8

    @property
    def has_primary_shard_rollover(self) -> bool:
        """ES 8+: ``max_primary_shard_size`` ILM rollover criterion is available."""
        return self.major >= 8

    @property
    def has_frozen_tier(self) -> bool:
        """ES 7.12+: frozen phase with searchable snapshots is available."""
        return self.major > 7 or (self.major == 7 and self.minor >= 12)

    @property
    def has_max_primary_shard_docs(self) -> bool:
        """ES 8.2+: ``max_primary_shard_docs`` ILM rollover criterion is available."""
        return self.major > 8 or (self.major == 8 and self.minor >= 2)
