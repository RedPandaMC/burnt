"""Historical DLT pipeline cost reporting."""

from __future__ import annotations

from typing import Any

from ..core.config import Settings
from ..tables.connection import DatabricksClient, _sanitize_id


def get_pipeline_report(
    pipeline_id: str | None = None,
    *,
    limit: int = 30,
    days: int = 30,
    warehouse_id: str | None = None,
) -> list[dict[str, Any]]:
    """Summarize per-table costs for DLT pipeline updates.

    Queries system.billing.usage for DLT SKUs, groups by pipeline update date,
    and identifies the dominant cost table.

    Args:
        pipeline_id: Filter to a specific DLT pipeline. If None, reports all.
        limit: Maximum number of update periods to return (default 30).
        days: Look-back window in days.
        warehouse_id: SQL warehouse for queries.

    Returns:
        List of period dicts with pipeline_id, update_date, total_cost_usd,
        dominant_table hint, and per-table breakdown where available.
    """
    _, settings = Settings.discover()
    wh_id = warehouse_id or settings.watch.warehouse_id or settings.warehouse_id
    if not wh_id:
        raise ValueError(
            "warehouse_id is required for pipeline reporting. "
            "Pass it explicitly or set warehouse_id in burnt.toml [watch]."
        )
    pipeline_filter = ""
    if pipeline_id is not None:
        safe_id = _sanitize_id(pipeline_id, "pipeline_id")
        pipeline_filter = f"AND u.usage_metadata.dlt_pipeline_id = '{safe_id}'"

    sql = f"""
        WITH pipeline_costs AS (
            SELECT
                u.usage_metadata.dlt_pipeline_id AS pipeline_id,
                DATE(u.usage_start_time) AS update_date,
                SUM(u.usage_quantity * COALESCE(p.price_usd, 0.55)) AS total_cost_usd
            FROM system.billing.usage u
            LEFT JOIN (
                SELECT sku_name, pricing.default AS price_usd
                FROM system.billing.list_prices
                WHERE price_end_time IS NULL
            ) p ON u.sku_name = p.sku_name
            WHERE u.usage_start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
              AND u.sku_name LIKE 'DLT%'
              AND u.usage_metadata.dlt_pipeline_id IS NOT NULL
              {pipeline_filter}
            GROUP BY 1, 2
            ORDER BY update_date DESC
            LIMIT {limit}
        ),
        pipeline_event_costs AS (
            SELECT
                e.pipeline_id,
                DATE(e.timestamp) AS update_date,
                e.entity_name AS table_name,
                SUM(e.num_output_rows) AS total_output_rows
            FROM system.lakeflow.pipeline_event_log e
            WHERE e.event_type = 'flow_progress'
              AND e.timestamp >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
              {pipeline_filter.replace("u.usage_metadata.dlt_pipeline_id", "e.pipeline_id")}
            GROUP BY 1, 2, 3
        )
        SELECT
            pc.pipeline_id,
            pc.update_date,
            ROUND(pc.total_cost_usd, 4) AS total_cost_usd,
            FIRST(pe.table_name ORDER BY pe.total_output_rows DESC) AS dominant_table
        FROM pipeline_costs pc
        LEFT JOIN pipeline_event_costs pe
            ON pc.pipeline_id = pe.pipeline_id
           AND pc.update_date = pe.update_date
        GROUP BY pc.pipeline_id, pc.update_date, pc.total_cost_usd
        ORDER BY pc.update_date DESC
    """
    with DatabricksClient(settings) as client:
        rows = client.execute_sql(sql, wh_id)

    return [
        {
            "pipeline_id": row.get("pipeline_id"),
            "update_date": row.get("update_date"),
            "total_cost_usd": float(row.get("total_cost_usd") or 0),
            "dominant_table": row.get("dominant_table"),
        }
        for row in rows
        if row.get("pipeline_id")
    ]
