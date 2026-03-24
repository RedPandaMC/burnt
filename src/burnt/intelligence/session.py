"""Session cost analysis for notebooks."""

from __future__ import annotations

from pydantic import BaseModel


class SessionCost(BaseModel):
    """Cost breakdown for a notebook session."""

    execution_cost_usd: float
    idle_cost_usd: float
    total_cost_usd: float
    execution_time_seconds: float
    total_time_seconds: float
    utilization_pct: float


def analyze_session(
    execution_time_seconds: float,
    total_time_seconds: float,
    cluster_cost_per_hour: float,
    dbu_per_hour: float = 0.75,
) -> SessionCost:
    """Analyze session cost from timing data.

    Args:
        execution_time_seconds: Time spent executing code.
        total_time_seconds: Total session time.
        cluster_cost_per_hour: Cluster cost per hour.
        dbu_per_hour: DBU rate.

    Returns:
        Session cost breakdown.
    """
    if total_time_seconds == 0:
        return SessionCost(
            execution_cost_usd=0,
            idle_cost_usd=0,
            total_cost_usd=0,
            execution_time_seconds=0,
            total_time_seconds=0,
            utilization_pct=0,
        )

    execution_hours = execution_time_seconds / 3600
    total_hours = total_time_seconds / 3600
    idle_hours = total_hours - execution_hours

    execution_cost = execution_hours * cluster_cost_per_hour
    idle_cost = idle_hours * cluster_cost_per_hour

    return SessionCost(
        execution_cost_usd=round(execution_cost, 4),
        idle_cost_usd=round(idle_cost, 4),
        total_cost_usd=round(execution_cost + idle_cost, 4),
        execution_time_seconds=execution_time_seconds,
        total_time_seconds=total_time_seconds,
        utilization_pct=round((execution_time_seconds / total_time_seconds) * 100, 1),
    )
