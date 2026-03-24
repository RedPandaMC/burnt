"""Cluster and configuration recommendations."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class ClusterRecommendation(BaseModel):
    """Three-tier cluster recommendation."""

    economy: dict[str, Any]
    balanced: dict[str, Any]
    performance: dict[str, Any]
    current_cost_usd: float
    rationale: str


def recommend(
    cost_estimate: Any,
    *,
    utilization_threshold: float = 0.3,
) -> ClusterRecommendation:
    """Recommend cluster configurations based on cost estimate.

    Args:
        cost_estimate: The cost estimate to base recommendations on.
        utilization_threshold: Minimum utilization for serverless.

    Returns:
        Cluster recommendation with three tiers.
    """
    raise NotImplementedError(
        "Recommendations require burnt-engine. Install with: pip install burnt[engine]"
    )
