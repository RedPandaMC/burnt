"""Idle cluster detection."""

from __future__ import annotations

from typing import Any


def find_idle_clusters(
    threshold_pct: float = 0.10,
    *,
    days: int = 7,
    warehouse_id: str | None = None,
) -> list[dict[str, Any]]:
    """Find clusters with excessive idle time.

    Args:
        threshold_pct: Alert if idle time exceeds this percentage.
        days: Number of days to look back.
        warehouse_id: Optional SQL warehouse for queries.

    Returns:
        List of idle clusters with idle time percentages and wasted cost.
    """
    raise NotImplementedError(
        "Idle detection requires burnt-engine and Databricks connectivity. "
        "Install with: pip install burnt[engine]"
    )
