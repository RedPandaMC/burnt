"""Watch orchestration."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class WatchResult(BaseModel):
    """Result of cost monitoring."""

    tag_costs: dict[str, float] = {}
    idle_clusters: list[dict[str, Any]] = []
    cost_drift: dict[str, float] = {}
    total_cost_usd: float = 0


def watch(
    tag_key: str | None = None,
    *,
    drift_threshold: float = 0.25,
    idle_threshold: float = 0.10,
    budget: float | None = None,
    days: int = 30,
    job_id: int | None = None,
    pipeline_id: str | None = None,
) -> WatchResult:
    """Monitor Databricks costs via system tables.

    Args:
        tag_key: Databricks tag to group costs by.
        drift_threshold: Alert if cost changes by more than this percentage.
        idle_threshold: Alert if cluster idle time exceeds this percentage.
        budget: Monthly budget for alerts.
        days: Number of days to look back.
        job_id: Filter to specific job.
        pipeline_id: Filter to specific DLT pipeline.

    Returns:
        Watch result with cost metrics and alerts.
    """
    raise NotImplementedError(
        "Watch requires burnt-engine and Databricks connectivity. "
        "Install with: pip install burnt[engine]"
    )
