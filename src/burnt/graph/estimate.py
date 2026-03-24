"""Cost estimation from graph."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel

if TYPE_CHECKING:
    from .model import CostGraph


class CostEstimate(BaseModel):
    """Estimated cost for a workload."""

    estimated_dbu: float | None = None
    estimated_cost_usd: float | None = None
    confidence: Literal["low", "medium", "high"] = "low"
    breakdown: dict[str, float] = {}
    warnings: list[str] = []


def estimate_cost(
    graph: CostGraph,
    *,
    dbu_rate: float = 0.75,
    num_workers: int = 2,
) -> CostEstimate:
    """Estimate cost by walking the cost graph.

    Args:
        graph: The cost graph to estimate.
        dbu_per_hour: DBU rate for the cluster.
        num_workers: Number of workers in the cluster.

    Returns:
        Cost estimate with DBU and dollar amounts.
    """
    raise NotImplementedError(
        "Cost estimation requires burnt-engine. Install with: pip install burnt[engine]"
    )
