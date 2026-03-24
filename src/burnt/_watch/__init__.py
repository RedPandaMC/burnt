"""burnt.watch() - Cost monitoring."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from pathlib import Path


def run(
    tag_key: str | None = None,
    *,
    drift_threshold: float = 0.25,
    idle_threshold: float = 0.10,
    budget: float | None = None,
    days: int = 30,
    job_id: int | None = None,
    pipeline_id: str | None = None,
) -> Any:
    """Monitor Databricks costs via system tables."""
    raise NotImplementedError(
        "burnt.watch() requires burnt-engine to be installed. "
        "See https://burnt.ai/docs/install for instructions."
    )


__all__ = ["run"]
