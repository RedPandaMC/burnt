"""Cost attribution logic for mapping DBU costs to queries and jobs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .connection import DatabricksClient

from .billing import get_historical_usage, get_live_prices


@dataclass
class QueryAttribution:
    """Cost attribution for a specific query."""

    query_fingerprint: str
    total_dbu: float
    total_cost_usd: float
    execution_count: int
    avg_duration_ms: float
    samples: list[dict]


@dataclass
class JobRunAttribution:
    """Cost attribution for a Lakeflow job run."""

    job_id: str
    run_id: str
    total_dbu: float
    total_cost_usd: float
    start_time: datetime
    end_time: datetime


def attribute_costs_to_queries(
    client: DatabricksClient,
    warehouse_id: str,
    days: int = 30,
) -> list[QueryAttribution]:
    """Attribute DBU costs to queries by joining billing usage with query history.

    Args:
        client: DatabricksClient instance
        warehouse_id: SQL warehouse ID
        days: Number of days of history to analyze

    Returns:
        List of QueryAttribution objects with cost breakdown per query fingerprint
    """
    usage_records = get_historical_usage(client, warehouse_id, days)
    sku_names = list({r.sku_name for r in usage_records})
    prices = get_live_prices(client, warehouse_id, sku_names)

    from .queries import get_query_history

    query_history = get_query_history(client, warehouse_id, days)

    attribution: dict[str, QueryAttribution] = {}

    for usage in usage_records:
        if not usage.warehouse_id:
            continue

        matching_queries = [
            q
            for q in query_history
            if q.warehouse_id == usage.warehouse_id
            and q.start_time
            and usage.usage_start_time
            and _time_overlaps(
                q.start_time,
                q.end_time,
                usage.usage_start_time,
                usage.usage_end_time,
            )
        ]

        for query in matching_queries:
            from .queries import fingerprint_sql

            fp = (
                fingerprint_sql(query.statement_text)
                if query.statement_text
                else "unknown"
            )

            if fp not in attribution:
                attribution[fp] = QueryAttribution(
                    query_fingerprint=fp,
                    total_dbu=0.0,
                    total_cost_usd=0.0,
                    execution_count=0,
                    avg_duration_ms=0.0,
                    samples=[],
                )

            dbu = float(usage.usage_quantity)
            price = prices.get(usage.sku_name, Decimal("0.55"))
            cost = dbu * float(price)

            attr = attribution[fp]
            attr.total_dbu += dbu
            attr.total_cost_usd += cost
            attr.execution_count += 1
            if query.execution_duration_ms:
                attr.avg_duration_ms = (
                    attr.avg_duration_ms * (attr.execution_count - 1)
                    + query.execution_duration_ms
                ) / attr.execution_count
            attr.samples.append(
                {
                    "statement_id": query.statement_id,
                    "start_time": query.start_time,
                    "duration_ms": query.execution_duration_ms,
                }
            )

    return list(attribution.values())


def get_historical_cost(
    client: DatabricksClient,
    warehouse_id: str,
    sql_fingerprint: str,
    days: int = 30,
) -> QueryAttribution | None:
    """Get historical cost for a specific SQL fingerprint.

    This function is used by Tier 4 of the EstimationPipeline to look up
    actual costs from previous executions of similar queries.

    Args:
        client: DatabricksClient instance
        warehouse_id: SQL warehouse ID
        sql_fingerprint: The SQL fingerprint to look up
        days: Number of days of history to analyze

    Returns:
        QueryAttribution if found, None otherwise
    """
    from .queries import find_similar_queries

    matching_queries = find_similar_queries(
        client, sql_fingerprint, warehouse_id, limit=100
    )

    if not matching_queries:
        return None

    usage_records = get_historical_usage(client, warehouse_id, days)
    sku_names = list({r.sku_name for r in usage_records})
    prices = get_live_prices(client, warehouse_id, sku_names)

    total_dbu = 0.0
    total_cost_usd = 0.0
    durations = []

    for usage in usage_records:
        if not usage.warehouse_id:
            continue

        for query in matching_queries:
            if query.warehouse_id != usage.warehouse_id:
                continue

            if not (query.start_time and usage.usage_start_time):
                continue

            if not _time_overlaps(
                query.start_time,
                query.end_time,
                usage.usage_start_time,
                usage.usage_end_time,
            ):
                continue

            dbu = float(usage.usage_quantity)
            price = prices.get(usage.sku_name, Decimal("0.55"))
            total_dbu += dbu
            total_cost_usd += dbu * float(price)

            if query.execution_duration_ms:
                durations.append(query.execution_duration_ms)

    if total_dbu == 0:
        return None

    avg_duration = sum(durations) / len(durations) if durations else 0.0

    return QueryAttribution(
        query_fingerprint=sql_fingerprint,
        total_dbu=total_dbu,
        total_cost_usd=total_cost_usd,
        execution_count=len(matching_queries),
        avg_duration_ms=avg_duration,
        samples=[
            {
                "statement_id": q.statement_id,
                "start_time": q.start_time,
                "duration_ms": q.execution_duration_ms,
            }
            for q in matching_queries[:10]
        ],
    )


def attribute_lakeflow_costs(
    client: DatabricksClient,
    warehouse_id: str,
    days: int = 30,
) -> list[JobRunAttribution]:
    """Attribute costs to Lakeflow job runs.

    Note: This requires access to system.lakeflow.job_run_timeline which
    may not be available in all workspaces.

    Args:
        client: DatabricksClient instance
        warehouse_id: SQL warehouse ID
        days: Number of days of history

    Returns:
        List of JobRunAttribution objects
    """
    sql = f"""
        SELECT
            job_id,
            run_id,
            start_time,
            end_time,
            dbu_total,
            cost_usd
        FROM system.lakeflow.job_run_timeline
        WHERE start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        ORDER BY start_time DESC
    """

    try:
        rows = client.execute_sql(sql, warehouse_id)
    except Exception:
        return []

    attributions = []
    for row in rows:
        try:
            attributions.append(
                JobRunAttribution(
                    job_id=str(row.get("job_id", "")),
                    run_id=str(row.get("run_id", "")),
                    total_dbu=float(row.get("dbu_total", 0)),
                    total_cost_usd=float(row.get("cost_usd", 0)),
                    start_time=datetime.fromisoformat(
                        row.get("start_time", "").replace("Z", "+00:00")
                    )
                    if row.get("start_time")
                    else datetime.min,
                    end_time=datetime.fromisoformat(
                        row.get("end_time", "").replace("Z", "+00:00")
                    )
                    if row.get("end_time")
                    else datetime.min,
                )
            )
        except (ValueError, TypeError):
            continue

    return attributions


def _time_overlaps(
    start1: str | None,
    end1: str | None,
    start2: str | None,
    end2: str | None,
) -> bool:
    """Check if two time ranges overlap."""
    if not all([start1, start2]):
        return False

    try:
        s1 = _parse_datetime(start1)
        e1 = _parse_datetime(end1) if end1 else s1
        s2 = _parse_datetime(start2)
        e2 = _parse_datetime(end2) if end2 else s2
    except (ValueError, TypeError):
        return False

    return s1 <= e2 and s2 <= e1


def _parse_datetime(dt_str: str | None) -> datetime:
    """Parse various datetime formats from Databricks."""
    import re

    if dt_str is None:
        raise ValueError("Cannot parse None datetime")
    dt_str = dt_str.replace("Z", "+00:00")
    dt_str = re.sub(r"(\.\d{6})\d*", r"\1", dt_str)
    return datetime.fromisoformat(dt_str)
