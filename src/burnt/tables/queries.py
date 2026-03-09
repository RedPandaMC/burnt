"""Query history system table integration and SQL fingerprinting."""

from __future__ import annotations

import hashlib
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .connection import DatabricksClient

from ..core.models import QueryRecord
from .connection import _sanitize_id

_HISTORY_COLUMNS = """
    statement_id, statement_text, statement_type,
    start_time, end_time,
    execution_duration_ms, compilation_duration_ms,
    read_bytes, read_rows, produced_rows, written_bytes,
    total_task_duration_ms,
    compute.warehouse_id AS warehouse_id,
    compute.cluster_id AS cluster_id,
    status, error_message
""".strip()

_INT_FIELDS = [
    "execution_duration_ms",
    "compilation_duration_ms",
    "read_bytes",
    "read_rows",
    "produced_rows",
    "written_bytes",
    "total_task_duration_ms",
]


def normalize_sql(sql: str) -> str:
    """Normalize SQL for fingerprinting: strip comments, normalize whitespace, replace literals.

    Strips -- line comments and /* block comments */, uppercases keywords,
    replaces string literals with '?', numeric literals with '?', and
    collapses IN-lists to a single placeholder.
    """
    # Strip -- line comments
    sql = re.sub(r"--[^\n]*", "", sql)
    # Strip /* block comments */
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    # Normalize whitespace
    sql = re.sub(r"\s+", " ", sql).strip().upper()
    # Replace string literals
    sql = re.sub(r"'[^']*'", "?", sql)
    # Replace numeric literals (not inside identifiers)
    sql = re.sub(r"\b\d+(\.\d+)?\b", "?", sql)
    # Collapse IN-lists: IN (?, ?, ?) -> IN (?)
    sql = re.sub(r"IN\s*\(\s*\?(?:\s*,\s*\?)*\s*\)", "IN (?)", sql)
    return sql


def fingerprint_sql(sql: str) -> str:
    """Return SHA-256 hex digest of the normalized SQL string."""
    normalized = normalize_sql(sql)
    return hashlib.sha256(normalized.encode()).hexdigest()


def get_query_history(
    client: DatabricksClient, warehouse_id: str, days: int = 30
) -> list[QueryRecord]:
    """Fetch query execution history for the past N days from system.query.history.

    Returns up to 10,000 most recent records ordered by start_time descending.
    """
    safe_warehouse_id = _sanitize_id(warehouse_id, "warehouse_id")
    sql = f"""
        SELECT {_HISTORY_COLUMNS}
        FROM system.query.history
        WHERE start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
          AND compute.warehouse_id = '{safe_warehouse_id}'
        ORDER BY start_time DESC
        LIMIT 10000
    """
    rows = client.execute_sql(sql, warehouse_id)
    return [_row_to_record(row) for row in rows]


def find_similar_queries(
    client: DatabricksClient,
    sql_fingerprint: str,
    warehouse_id: str,
    limit: int = 10,
) -> list[QueryRecord]:
    """Find historical queries matching a SQL fingerprint from system.query.history.

    Fetches recent FINISHED queries for the warehouse and filters in-memory by
    comparing each statement's fingerprint against the provided sql_fingerprint.
    Returns up to limit matching records.
    """
    safe_warehouse_id = _sanitize_id(warehouse_id, "warehouse_id")
    sql = f"""
        SELECT {_HISTORY_COLUMNS}
        FROM system.query.history
        WHERE compute.warehouse_id = '{safe_warehouse_id}'
          AND status = 'FINISHED'
        ORDER BY start_time DESC
        LIMIT {limit * 100}
    """
    rows = client.execute_sql(sql, warehouse_id)
    records = [_row_to_record(row) for row in rows]
    return [r for r in records if fingerprint_sql(r.statement_text) == sql_fingerprint][
        :limit
    ]


def _row_to_record(row: dict[str, Any]) -> QueryRecord:
    """Convert raw API row dict to a QueryRecord, coercing numeric fields to int."""
    coerced: dict[str, Any] = dict(row)
    for field in _INT_FIELDS:
        value = coerced.get(field)
        coerced[field] = int(value) if value is not None else None
    return QueryRecord(**coerced)
