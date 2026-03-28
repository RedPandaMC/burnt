"""Tag-based cost attribution."""

from __future__ import annotations

from ..core.config import Settings
from ..tables.connection import DatabricksClient, _sanitize_id


def tag_attribution(
    tag_key: str,
    *,
    days: int = 30,
    warehouse_id: str | None = None,
) -> dict[str, float]:
    """Get cost breakdown by Databricks tag.

    Queries system.billing.usage grouped by the given tag key.
    Rows without the tag are reported under '__untagged__'.

    Args:
        tag_key: The tag key to group by (e.g., "team", "project").
        days: Number of days to look back.
        warehouse_id: SQL warehouse for queries.

    Returns:
        Dictionary mapping tag values to total cost in USD, ordered by cost descending.
    """
    _, settings = Settings.discover()
    wh_id = warehouse_id or settings.watch.warehouse_id or settings.warehouse_id
    if not wh_id:
        raise ValueError(
            "warehouse_id is required for tag attribution. "
            "Pass it explicitly or set warehouse_id in burnt.toml [watch] "
            "or the BURNT_WAREHOUSE_ID env var."
        )
    safe_key = _sanitize_id(tag_key, "tag_key")
    sql = f"""
        SELECT
            COALESCE(custom_tags['{safe_key}'], '__untagged__') AS tag_value,
            SUM(usage_quantity * COALESCE(p.price_usd, 0.55)) AS total_cost_usd
        FROM system.billing.usage u
        LEFT JOIN (
            SELECT sku_name, pricing.default AS price_usd
            FROM system.billing.list_prices
            WHERE price_end_time IS NULL
        ) p ON u.sku_name = p.sku_name
        WHERE u.usage_start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        GROUP BY 1
        ORDER BY total_cost_usd DESC
    """
    with DatabricksClient(settings) as client:
        rows = client.execute_sql(sql, wh_id)
    return {row["tag_value"]: float(row["total_cost_usd"] or 0) for row in rows if row.get("tag_value")}
