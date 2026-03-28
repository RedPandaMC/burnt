"""burnt.watch() - Cost monitoring."""

from __future__ import annotations

from typing import Any


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
    from ..watch.core import watch

    return watch(
        tag_key=tag_key,
        drift_threshold=drift_threshold,
        idle_threshold=idle_threshold,
        budget=budget,
        days=days,
        job_id=job_id,
        pipeline_id=pipeline_id,
    )


__all__ = ["run"]
