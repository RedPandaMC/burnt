"""SQLite-based mock backend for Databricks.

Implements the Backend protocol using SQLite, allowing tests to run
without requiring a real Databricks connection.
"""

from __future__ import annotations

import contextlib
import sqlite3
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from burnt.core.models import ClusterConfig, DeltaTableInfo, QueryRecord

from burnt.core.models import ClusterConfig, DeltaTableInfo, QueryRecord
from burnt.core.table_registry import TableRegistry

from .mock_data import init_mock_database


class SQLiteBackend:
    """Mock Databricks backend using SQLite.

    Implements the Backend protocol for testing. Supports:
    - SQL execution with dialect translation
    - Query history retrieval
    - Table metadata (DESCRIBE DETAIL)
    - Cluster configuration

    Usage:
        >>> backend = SQLiteBackend()
        >>> results = backend.execute_sql("SELECT * FROM system.billing.usage")
        >>> queries = backend.get_recent_queries(limit=10)
    """

    def __init__(
        self,
        db_path: str = ":memory:",
        xlsx_path: str | None = None,
        scale_factor: int = 10,
        table_registry: TableRegistry | None = None,
    ):
        """Initialize SQLiteBackend.

        Args:
            db_path: Path to SQLite database (default: :memory:)
            xlsx_path: Path to xlsx file with real billing data
            scale_factor: Multiplier for benchmark table sizes
            table_registry: TableRegistry for table path mapping
        """
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row
        self._registry = table_registry or TableRegistry()

        # Initialize with mock data
        self._stats = init_mock_database(
            self._conn,
            xlsx_path=xlsx_path,
            scale_factor=scale_factor,
        )

    def _translate_sql(self, sql: str) -> str:
        """Translate Databricks SQL dialect to SQLite.

        Performs simple string replacements for common patterns.

        Args:
            sql: Databricks SQL

        Returns:
            SQLite-compatible SQL
        """
        # Apply table registry mappings
        sql = self._registry.format_sql(sql)

        # Convert table paths to SQLite-safe names
        # system.billing.usage -> system_billing_usage
        for default_path in [
            "system.billing.usage",
            "system.billing.list_prices",
            "system.query.history",
            "system.compute.node_types",
            "system.compute.clusters",
            "system.compute.node_timeline",
            "system.lakeflow.jobs",
            "system.lakeflow.job_run_timeline",
        ]:
            sqlite_name = default_path.replace(".", "_")
            sql = sql.replace(default_path, sqlite_name)

        # Also handle registry-configured paths
        for configured_path in [
            self._registry.billing_usage,
            self._registry.billing_list_prices,
            self._registry.query_history,
            self._registry.compute_node_types,
        ]:
            if "." in configured_path:
                sqlite_name = configured_path.replace(".", "_")
                sql = sql.replace(configured_path, sqlite_name)

        # Databricks -> SQLite function translations
        translations = {
            "DATEADD(day, -": "DATE('now', '-',",
            "DATEADD(day, ": "DATE('now', '+',",
            "DATEADD(hour, -": "DATETIME('now', '-',",
            "DATEADD(hour, ": "DATETIME('now', '+',",
            "CURRENT_TIMESTAMP()": "CURRENT_TIMESTAMP",
            "CURRENT_DATE()": "CURRENT_DATE",
            "GETDATE()": "CURRENT_DATE",
            "NOW()": "CURRENT_TIMESTAMP",
            "TRY_CAST": "CAST",
            "DECIMAL": "REAL",
        }

        for databricks, sqlite in translations.items():
            sql = sql.replace(databricks, sqlite)

        return sql

    def execute_sql(
        self, sql: str, warehouse_id: str | None = None
    ) -> list[dict[str, Any]]:
        """Execute SQL and return results.

        Args:
            sql: SQL statement to execute
            warehouse_id: Ignored (for API compatibility)

        Returns:
            List of row dicts
        """
        translated_sql = self._translate_sql(sql)

        cursor = self._conn.cursor()
        try:
            cursor.execute(translated_sql)

            # Fetch column names
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
            else:
                columns = []

            # Fetch all rows
            rows = cursor.fetchall()

            # Convert to list of dicts
            return [dict(zip(columns, row, strict=False)) for row in rows]

        except sqlite3.Error:
            # Return empty list on error (graceful degradation)
            return []

    def get_cluster_config(self, cluster_id: str) -> ClusterConfig:
        """Get cluster configuration.

        Returns a default cluster configuration since SQLite
        doesn't have access to real cluster metadata.

        Args:
            cluster_id: Cluster ID (ignored for mock)

        Returns:
            Default ClusterConfig
        """
        return ClusterConfig(
            instance_type="Standard_DS3_v2",
            num_workers=2,
            dbu_per_hour=0.75,
            photon_enabled=False,
            sku="ALL_PURPOSE",
        )

    def get_recent_queries(self, limit: int = 100) -> list[QueryRecord]:
        """Get recent query history.

        Args:
            limit: Maximum number of queries to return

        Returns:
            List of QueryRecord objects
        """
        sql = f"""
            SELECT
                statement_id,
                statement_text,
                statement_type,
                start_time,
                end_time,
                execution_duration_ms,
                compilation_duration_ms,
                read_bytes,
                read_rows,
                produced_rows,
                written_bytes,
                total_task_duration_ms,
                warehouse_id,
                cluster_id,
                status,
                error_message
            FROM system_query_history
            ORDER BY start_time DESC
            LIMIT {limit}
        """

        rows = self.execute_sql(sql)

        return [
            QueryRecord(
                statement_id=row.get("statement_id", ""),
                statement_text=row.get("statement_text", ""),
                statement_type=row.get("statement_type"),
                start_time=row.get("start_time", ""),
                end_time=row.get("end_time"),
                execution_duration_ms=row.get("execution_duration_ms"),
                compilation_duration_ms=row.get("compilation_duration_ms"),
                read_bytes=row.get("read_bytes"),
                read_rows=row.get("read_rows"),
                produced_rows=row.get("produced_rows"),
                written_bytes=row.get("written_bytes"),
                total_task_duration_ms=row.get("total_task_duration_ms"),
                warehouse_id=row.get("warehouse_id"),
                cluster_id=row.get("cluster_id"),
                status=row.get("status", ""),
                error_message=row.get("error_message"),
            )
            for row in rows
        ]

    def describe_table(self, table_name: str) -> DeltaTableInfo:
        """Get Delta table metadata.

        Args:
            table_name: Fully qualified table name

        Returns:
            DeltaTableInfo with table metadata

        Raises:
            ValueError: If table not found in mock_tables
        """
        # Check mock_tables for the table
        sql = """
            SELECT table_name, location, size_in_bytes, num_files,
                   num_records, partition_columns
            FROM mock_tables
            WHERE table_name = ?
        """

        cursor = self._conn.cursor()
        cursor.execute(sql, (table_name,))
        row = cursor.fetchone()

        if not row:
            # Try to get size from actual table
            try:
                count_sql = (
                    f"SELECT COUNT(*) as count FROM {table_name.replace('.', '_')}"
                )
                cursor.execute(count_sql)
                count_row = cursor.fetchone()
                num_records = count_row[0] if count_row else 0

                # Estimate size (100 bytes per row for simplicity)
                size_in_bytes = num_records * 100

                return DeltaTableInfo(
                    location=f"dbfs:/mock/{table_name}",
                    total_size_bytes=size_in_bytes,
                    num_files=max(1, num_records // 1000),
                    num_records=num_records,
                    partition_columns=[],
                )
            except sqlite3.Error:
                raise ValueError(f"Table not found: {table_name}") from None

        import json

        partition_columns = []
        if row["partition_columns"]:
            with contextlib.suppress(json.JSONDecodeError):
                partition_columns = json.loads(row["partition_columns"])

        return DeltaTableInfo(
            location=row["location"],
            total_size_bytes=row["size_in_bytes"],
            num_files=row["num_files"],
            num_records=row["num_records"],
            partition_columns=partition_columns,
        )

    def get_session_metrics(self) -> dict[str, Any]:
        """Get mock session metrics.

        Returns:
            Dict with mock session metrics
        """
        return {
            "active_stages": 0,
            "active_jobs": 0,
            "completed_stages": 0,
            "completed_tasks": 0,
            "failed_stages": 0,
            "failed_tasks": 0,
            "executor_count": 2,
        }

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

    def get_stats(self) -> dict[str, Any]:
        """Get statistics about the mock database.

        Returns:
            Dict with stats about loaded data
        """
        return self._stats.copy()


def create_mock_backend(
    xlsx_path: str | None = None,
    scale_factor: int = 10,
    table_registry: TableRegistry | None = None,
) -> SQLiteBackend:
    """Create a SQLiteBackend with mock data.

    Convenience function for creating a mock backend.

    Args:
        xlsx_path: Path to xlsx file with real billing data
        scale_factor: Multiplier for benchmark table sizes
        table_registry: TableRegistry for table path mapping

    Returns:
        Initialized SQLiteBackend
    """
    return SQLiteBackend(
        db_path=":memory:",
        xlsx_path=xlsx_path,
        scale_factor=scale_factor,
        table_registry=table_registry,
    )
