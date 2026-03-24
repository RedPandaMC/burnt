"""Tag-based cost attribution."""

from __future__ import annotations


def tag_attribution(
    tag_key: str,
    *,
    days: int = 30,
    warehouse_id: str | None = None,
) -> dict[str, float]:
    """Get cost breakdown by Databricks tag.

    Args:
        tag_key: The tag key to group by (e.g., "team", "project").
        days: Number of days to look back.
        warehouse_id: Optional SQL warehouse for queries.

    Returns:
        Dictionary mapping tag values to total cost in USD.
    """
    raise NotImplementedError(
        "Tag attribution requires burnt-engine and Databricks connectivity. "
        "Install with: pip install burnt[engine]"
    )
