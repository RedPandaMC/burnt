"""Cost drift detection."""

from __future__ import annotations

from ..core.config import Settings
from ..tables.connection import DatabricksClient


def detect_cost_drift(
    *,
    threshold_pct: float = 0.25,
    days: int = 30,
    warehouse_id: str | None = None,
) -> dict[str, float]:
    """Detect jobs and DLT pipelines whose recent costs deviate from their baseline.

    Computes a 30-day median baseline per resource, compares it to the last
    7-day average, and returns resources where the drift exceeds threshold_pct.

    Args:
        threshold_pct: Minimum absolute drift ratio to include (e.g. 0.25 = 25%).
        days: Total window to look back for baseline computation.
        warehouse_id: SQL warehouse for queries.

    Returns:
        Dictionary mapping resource identifiers (e.g. "job:123", "pipeline:abc")
        to their drift ratio (positive = cost increase, negative = decrease).
    """
    _, settings = Settings.discover()
    wh_id = warehouse_id or settings.watch.warehouse_id or settings.warehouse_id
    if not wh_id:
        raise ValueError(
            "warehouse_id is required for drift detection. "
            "Pass it explicitly or set warehouse_id in burnt.toml [watch] "
            "or the BURNT_WAREHOUSE_ID env var."
        )
    sql = f"""
        WITH daily_costs AS (
            SELECT
                CASE
                    WHEN usage_metadata.job_id IS NOT NULL
                        THEN CONCAT('job:', CAST(usage_metadata.job_id AS STRING))
                    WHEN usage_metadata.dlt_pipeline_id IS NOT NULL
                        THEN CONCAT('pipeline:', usage_metadata.dlt_pipeline_id)
                END AS resource_id,
                DATE(usage_start_time) AS cost_date,
                SUM(usage_quantity * COALESCE(p.price_usd, 0.55)) AS daily_cost
            FROM system.billing.usage u
            LEFT JOIN (
                SELECT sku_name, pricing.default AS price_usd
                FROM system.billing.list_prices
                WHERE price_end_time IS NULL
            ) p ON u.sku_name = p.sku_name
            WHERE u.usage_start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
              AND (
                  usage_metadata.job_id IS NOT NULL
                  OR usage_metadata.dlt_pipeline_id IS NOT NULL
              )
            GROUP BY 1, 2
        ),
        baseline AS (
            SELECT
                resource_id,
                PERCENTILE_APPROX(daily_cost, 0.5) AS median_cost
            FROM daily_costs
            WHERE cost_date < DATEADD(day, -7, CURRENT_TIMESTAMP())
            GROUP BY resource_id
        ),
        recent AS (
            SELECT
                resource_id,
                AVG(daily_cost) AS recent_avg
            FROM daily_costs
            WHERE cost_date >= DATEADD(day, -7, CURRENT_TIMESTAMP())
            GROUP BY resource_id
        )
        SELECT
            r.resource_id,
            b.median_cost,
            r.recent_avg,
            (r.recent_avg - b.median_cost) / b.median_cost AS drift_ratio
        FROM recent r
        JOIN baseline b ON r.resource_id = b.resource_id
        WHERE b.median_cost > 0
          AND ABS((r.recent_avg - b.median_cost) / b.median_cost) >= {threshold_pct}
        ORDER BY ABS(drift_ratio) DESC
    """
    with DatabricksClient(settings) as client:
        rows = client.execute_sql(sql, wh_id)
    return {
        row["resource_id"]: float(row["drift_ratio"] or 0)
        for row in rows
        if row.get("resource_id")
    }
