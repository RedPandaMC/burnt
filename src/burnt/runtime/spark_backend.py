"""Spark backend for in-cluster execution via SparkSession."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..core.models import ClusterConfig, DeltaTableInfo, QueryRecord

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class SparkBackend:
    """In-cluster execution backend using SparkSession.

    Provides direct access to SparkSQL for executing queries
    and retrieving session metrics inside Databricks notebooks.
    """

    def __init__(self, spark: SparkSession) -> None:
        """Initialize SparkBackend with active SparkSession.

        Args:
            spark: Active SparkSession instance

        Raises:
            ImportError: If pyspark is not installed
        """
        try:
            from pyspark.sql import SparkSession
        except ImportError as err:
            raise ImportError(
                "pyspark is required for SparkBackend. "
                "Install with: pip install pyspark"
            ) from err

        if not isinstance(spark, SparkSession):
            raise TypeError(f"Expected SparkSession, got {type(spark).__name__}")

        self._spark = spark

    def execute_sql(self, sql: str, warehouse_id: str | None = None) -> list[dict[str, object]]:
        """Execute SQL using SparkSession and collect results.

        Args:
            sql: SQL statement to execute
            warehouse_id: Ignored in SparkBackend (not applicable)

        Returns:
            List of row dicts with column names as keys
        """
        df = self._spark.sql(sql)
        rows = df.collect()

        if not rows:
            return []

        col_names = df.columns
        return [dict(zip(col_names, row, strict=False)) for row in rows]

    def get_cluster_config(self, cluster_id: str) -> ClusterConfig:
        """Get cluster configuration from Spark context.

        Note: cluster_id is ignored in SparkBackend since we can
        get cluster info directly from the SparkSession.

        Args:
            cluster_id: Ignored (kept for protocol compatibility)

        Returns:
            ClusterConfig with current cluster details
        """
        conf = self._spark.conf

        instance_type = conf.get("spark.databricks.cluster.nodeType", "Standard_DS3_v2")
        num_workers = int(conf.get("spark.databricks.cluster.numWorkers", "2"))
        photon = conf.get("spark.databricks.photon.enabled", "false").lower() == "true"

        dbu_rate = self._get_dbu_rate(instance_type)

        return ClusterConfig(
            instance_type=instance_type,
            num_workers=num_workers,
            dbu_per_hour=dbu_rate,
            photon_enabled=photon,
            sku="ALL_PURPOSE",
        )

    def get_recent_queries(self, limit: int = 100) -> list[QueryRecord]:
        """Get recent queries from system.query.history.

        Queries system.query.history table directly via SparkSQL.

        Args:
            limit: Maximum number of queries to return

        Returns:
            List of QueryRecord objects
        """
        query = f"""
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
            FROM system.query.history
            ORDER BY start_time DESC
            LIMIT {limit}
        """

        try:
            df = self._spark.sql(query)
            rows = df.collect()
        except Exception:
            return []

        return [
            QueryRecord(
                statement_id=row["statement_id"],
                statement_text=row["statement_text"],
                statement_type=row.get("statement_type"),
                start_time=row["start_time"],
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
        """Get Delta table metadata using DESCRIBE DETAIL.

        Args:
            table_name: Fully qualified table name

        Returns:
            DeltaTableInfo with table metadata
        """
        df = self._spark.sql(f"DESCRIBE DETAIL {table_name}")
        row = df.first()

        if not row:
            raise ValueError(f"No results for DESCRIBE DETAIL {table_name}")

        return DeltaTableInfo(
            location=row["location"],
            total_size_bytes=row["sizeInBytes"],
            num_files=row["numFiles"],
            num_records=row.get("numRecords"),
            partition_columns=row.get("partitionColumns", []),
        )

    def get_session_metrics(self) -> dict[str, object]:
        """Get current session metrics from SparkContext.

        Returns:
            Dict with active stages, completed tasks, etc.
        """
        sc = self._spark.sparkContext

        status_tracker = sc.statusTracker()
        job_status = status_tracker.getJobStatus()

        return {
            "active_stages": status_tracker.getActiveStageIds(),
            "active_jobs": status_tracker.getActiveJobIds(),
            "completed_stages": job_status.get("numCompletedStages", 0),
            "completed_tasks": job_status.get("numCompletedTasks", 0),
            "failed_stages": job_status.get("numFailedStages", 0),
            "failed_tasks": job_status.get("numFailedTasks", 0),
            "executor_count": len(sc._jsc.getExecutorMemoryStatus()),
        }

    @staticmethod
    def _get_dbu_rate(instance_type: str) -> float:
        """Get approximate DBU rate for instance type."""
        dbu_rates = {
            "Standard_DS3_v2": 0.75,
            "Standard_DS4_v2": 1.5,
            "Standard_DS5_v2": 3.0,
            "Standard_E4s_v3": 1.0,
            "Standard_E8s_v3": 2.0,
            "Standard_E16s_v3": 4.0,
            "Standard_E32s_v3": 8.0,
        }
        return dbu_rates.get(instance_type, 0.75)
