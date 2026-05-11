"""REST backend using Databricks SDK for external execution."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..core.exceptions import NotAvailableError
from ..core.models import ClusterConfig, DeltaTableInfo, QueryRecord

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


class RestBackend:
    """External execution backend using Databricks SDK.

    Uses Databricks unified authentication (OAuth, PAT, or config profile).
    Requires databricks-sdk package.
    """

    def __init__(self, workspace_client: WorkspaceClient | None = None) -> None:
        """Initialize RestBackend with Databricks SDK client.

        Args:
            workspace_client: Optional pre-configured WorkspaceClient.
                              If None, creates one using unified auth env vars.
        """
        if workspace_client is not None:
            self._client = workspace_client
        else:
            try:
                from databricks.sdk import WorkspaceClient

                self._client = WorkspaceClient()
            except ImportError as err:
                raise ImportError(
                    "databricks-sdk is required for REST backend. "
                    "Install with: pip install databricks-sdk"
                ) from err

    def execute_sql(self, sql: str, warehouse_id: str | None = None) -> list[dict[str, object]]:
        """Execute a SQL statement via Statement Execution API.

        Args:
            sql: SQL statement to execute
            warehouse_id: SQL warehouse ID (required)

        Returns:
            List of row dicts with column names as keys

        Raises:
            ValueError: If warehouse_id is not provided
        """
        if not warehouse_id:
            raise ValueError("warehouse_id is required for REST backend")

        from databricks.sdk.service.sql import ExecuteStatementRequest

        request = ExecuteStatementRequest(
            statement=sql,
            warehouse_id=warehouse_id,
            wait_timeout="30s",
            disposition="INLINE",
            format="JSON_ARRAY",
        )

        response = self._client.statements.execute_statement(request)

        if response.result and response.result.data_array:
            schema = response.manifest.schema if response.manifest else None
            col_names = [col.name for col in schema.columns] if schema else []
            return [
                dict(zip(col_names, row_values, strict=False))
                for row_values in response.result.data_array
            ]
        return []

    def get_cluster_config(self, cluster_id: str) -> ClusterConfig:
        """Get cluster configuration via Clusters API.

        Args:
            cluster_id: Cluster ID to query

        Returns:
            ClusterConfig with cluster details
        """

        cluster = self._client.clusters.get(cluster_id)
        if not cluster:
            raise ValueError(f"Cluster {cluster_id} not found")

        num_workers = cluster.num_workers or 0
        instance_type = cluster.node_type_id or "Standard_DS3_v2"

        dbu_per_hour = self._get_dbu_rate(instance_type)

        return ClusterConfig(
            instance_type=instance_type,
            num_workers=num_workers,
            dbu_per_hour=dbu_per_hour,
            photon_enabled=cluster.enable_photon or False,
            sku="ALL_PURPOSE",
        )

    def get_recent_queries(self, limit: int = 100) -> list[QueryRecord]:
        """Get recent query history from system.query.history.

        Args:
            limit: Maximum number of queries to return

        Returns:
            List of QueryRecord objects
        """
        queries = self._client.queries.list(max_results=limit)

        return [
            QueryRecord(
                statement_id=q.statement_id or "",
                statement_text=q.statement_text or "",
                statement_type=q.statement_type,
                start_time=q.start_time or "",
                end_time=q.end_time,
                execution_duration_ms=q.execution_duration_ms,
                compilation_duration_ms=q.compilation_duration_ms,
                read_bytes=q.read_bytes,
                read_rows=q.read_rows,
                produced_rows=q.produced_rows,
                written_bytes=q.written_bytes,
                total_task_duration_ms=q.total_task_duration_ms,
                warehouse_id=q.warehouse_id,
                cluster_id=q.cluster_id,
                status=q.status or "",
                error_message=q.error_message,
            )
            for q in queries
        ]

    def describe_table(self, table_name: str) -> DeltaTableInfo:
        """Get Delta table metadata using DESCRIBE DETAIL.

        Args:
            table_name: Fully qualified table name

        Returns:
            DeltaTableInfo with table metadata
        """
        rows = self.execute_sql(f"DESCRIBE DETAIL {table_name}", None)
        if not rows:
            raise ValueError(f"No results for DESCRIBE DETAIL {table_name}")

        row = rows[0]
        return DeltaTableInfo(
            location=row.get("location", ""),
            total_size_bytes=row.get("sizeInBytes", 0),
            num_files=row.get("numFiles", 0),
            num_records=row.get("numRecords"),
            partition_columns=row.get("partitionColumns", "").split(",")
            if row.get("partitionColumns")
            else [],
        )

    def get_session_metrics(self) -> dict[str, object]:
        """Get session metrics (not available in REST context).

        Raises:
            NotAvailableError: Always, since REST has no session context
        """
        raise NotAvailableError(
            "Session metrics require in-cluster execution (SparkBackend)"
        )

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
