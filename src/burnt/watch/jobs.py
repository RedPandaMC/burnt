"""Historical job run cost reporting."""

from __future__ import annotations

from typing import Any

from ..core.config import Settings
from ..tables.connection import DatabricksClient, _sanitize_id


def get_job_report(
    job_id: int | None = None,
    *,
    limit: int = 30,
    days: int = 30,
    warehouse_id: str | None = None,
) -> list[dict[str, Any]]:
    """Summarize historical cost and performance for Databricks job runs.

    Returns the last `limit` runs ordered newest-first, with per-run cost,
    trend classification, and >20% jump annotations.

    Args:
        job_id: Filter to a specific job. If None, returns across all jobs.
        limit: Maximum number of runs to return (default 30).
        days: Look-back window in days.
        warehouse_id: SQL warehouse for queries.

    Returns:
        List of run dicts with job_id, run_id, start_time, end_time,
        result_state, total_cost_usd, pct_change_vs_median, and annotation.
    """
    _, settings = Settings.discover()
    wh_id = warehouse_id or settings.watch.warehouse_id or settings.warehouse_id
    if not wh_id:
        raise ValueError(
            "warehouse_id is required for job reporting. "
            "Pass it explicitly or set warehouse_id in burnt.toml [watch]."
        )
    job_filter = ""
    if job_id is not None:
        safe_id = _sanitize_id(str(job_id), "job_id")
        job_filter = f"AND t.job_id = '{safe_id}'"

    sql = f"""
        WITH run_costs AS (
            SELECT
                t.job_id,
                t.run_id,
                t.start_time,
                t.end_time,
                t.result_state,
                SUM(u.usage_quantity * COALESCE(p.price_usd, 0.55)) AS total_cost_usd
            FROM system.lakeflow.job_run_timeline t
            LEFT JOIN system.billing.usage u
                ON u.usage_metadata.job_id = t.job_id
               AND u.usage_metadata.job_run_id = t.run_id
            LEFT JOIN (
                SELECT sku_name, pricing.default AS price_usd
                FROM system.billing.list_prices
                WHERE price_end_time IS NULL
            ) p ON u.sku_name = p.sku_name
            WHERE t.start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
              {job_filter}
            GROUP BY t.job_id, t.run_id, t.start_time, t.end_time, t.result_state
            ORDER BY t.start_time DESC
            LIMIT {limit}
        ),
        medians AS (
            SELECT
                job_id,
                PERCENTILE_APPROX(total_cost_usd, 0.5) AS median_cost
            FROM run_costs
            GROUP BY job_id
        )
        SELECT
            rc.job_id,
            rc.run_id,
            rc.start_time,
            rc.end_time,
            rc.result_state,
            ROUND(rc.total_cost_usd, 4) AS total_cost_usd,
            ROUND(
                (rc.total_cost_usd - m.median_cost) / NULLIF(m.median_cost, 0),
                4
            ) AS pct_change_vs_median
        FROM run_costs rc
        JOIN medians m ON rc.job_id = m.job_id
        ORDER BY rc.start_time DESC
    """
    with DatabricksClient(settings) as client:
        rows = client.execute_sql(sql, wh_id)

    result = []
    for row in rows:
        pct = float(row.get("pct_change_vs_median") or 0)
        annotation = ""
        if pct > 0.20:
            annotation = f"spike (+{pct:.0%})"
        elif pct < -0.20:
            annotation = f"drop ({pct:.0%})"
        result.append(
            {
                "job_id": row.get("job_id"),
                "run_id": row.get("run_id"),
                "start_time": row.get("start_time"),
                "end_time": row.get("end_time"),
                "result_state": row.get("result_state"),
                "total_cost_usd": float(row.get("total_cost_usd") or 0),
                "pct_change_vs_median": pct,
                "annotation": annotation,
            }
        )
    return result
