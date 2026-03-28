"""Idle cluster detection."""

from __future__ import annotations

from typing import Any

from ..core.config import Settings
from ..tables.connection import DatabricksClient


def find_idle_clusters(
    threshold_pct: float = 0.10,
    *,
    days: int = 7,
    warehouse_id: str | None = None,
) -> list[dict[str, Any]]:
    """Find All-Purpose clusters with average CPU utilization below threshold.

    Joins system.compute.node_timeline with system.billing.usage to compute
    both idle percentage and wasted cost.

    Args:
        threshold_pct: Flag clusters with avg CPU below this fraction (e.g. 0.10 = 10%).
        days: Number of days to look back.
        warehouse_id: SQL warehouse for queries.

    Returns:
        List of dicts with cluster_id, avg_cpu_pct, running_hours,
        total_cost_usd, wasted_cost_usd, and recommendation.
    """
    _, settings = Settings.discover()
    wh_id = warehouse_id or settings.watch.warehouse_id or settings.warehouse_id
    if not wh_id:
        raise ValueError(
            "warehouse_id is required for idle cluster detection. "
            "Pass it explicitly or set warehouse_id in burnt.toml [watch] "
            "or the BURNT_WAREHOUSE_ID env var."
        )
    threshold_sql = threshold_pct * 100
    sql = f"""
        WITH node_stats AS (
            SELECT
                cluster_id,
                AVG(avg_cpu) AS avg_cpu_pct,
                SUM(
                    TIMESTAMPDIFF(SECOND, start_time, COALESCE(end_time, CURRENT_TIMESTAMP()))
                ) / 3600.0 AS running_hours
            FROM system.compute.node_timeline
            WHERE start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
              AND driver = false
            GROUP BY cluster_id
        ),
        cluster_costs AS (
            SELECT
                usage_metadata.cluster_id AS cluster_id,
                SUM(usage_quantity * COALESCE(p.price_usd, 0.55)) AS total_cost_usd
            FROM system.billing.usage u
            LEFT JOIN (
                SELECT sku_name, pricing.default AS price_usd
                FROM system.billing.list_prices
                WHERE price_end_time IS NULL
            ) p ON u.sku_name = p.sku_name
            WHERE u.usage_start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
              AND u.sku_name = 'ALL_PURPOSE'
              AND usage_metadata.cluster_id IS NOT NULL
            GROUP BY usage_metadata.cluster_id
        )
        SELECT
            ns.cluster_id,
            ROUND(ns.avg_cpu_pct, 2) AS avg_cpu_pct,
            ROUND(ns.running_hours, 2) AS running_hours,
            ROUND(cc.total_cost_usd, 4) AS total_cost_usd,
            ROUND(cc.total_cost_usd * (1.0 - ns.avg_cpu_pct / 100.0), 4) AS wasted_cost_usd
        FROM node_stats ns
        JOIN cluster_costs cc ON ns.cluster_id = cc.cluster_id
        WHERE ns.avg_cpu_pct < {threshold_sql}
        ORDER BY wasted_cost_usd DESC
    """
    with DatabricksClient(settings) as client:
        rows = client.execute_sql(sql, wh_id)
    return [
        {
            "cluster_id": row["cluster_id"],
            "avg_cpu_pct": float(row["avg_cpu_pct"] or 0),
            "running_hours": float(row["running_hours"] or 0),
            "total_cost_usd": float(row["total_cost_usd"] or 0),
            "wasted_cost_usd": float(row["wasted_cost_usd"] or 0),
            "recommendation": (
                "Enable auto-termination (idle > 10 min) or terminate manually."
            ),
        }
        for row in rows
        if row.get("cluster_id")
    ]
