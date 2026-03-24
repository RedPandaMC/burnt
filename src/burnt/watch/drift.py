"""Cost drift detection."""

from __future__ import annotations


def detect_cost_drift(
    *,
    threshold_pct: float = 0.25,
    days: int = 30,
    warehouse_id: str | None = None,
) -> dict[str, float]:
    """Detect significant cost changes over time.

    Args:
        threshold_pct: Alert if cost changes by more than this percentage.
        days: Number of days to look back.
        warehouse_id: Optional SQL warehouse for queries.

    Returns:
        Dictionary mapping resources to cost change percentage.
    """
    raise NotImplementedError(
        "Drift detection requires burnt-engine and Databricks connectivity. "
        "Install with: pip install burnt[engine]"
    )
