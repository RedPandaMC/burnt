"""Backend protocol for runtime execution contexts."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from ..core.models import ClusterConfig, DeltaTableInfo, QueryRecord


@runtime_checkable
class Backend(Protocol):
    """Protocol for runtime backends that can execute queries in different contexts.

    Implementations:
    - SparkBackend: In-cluster execution via SparkSession
    - RestBackend: External execution via Databricks SDK
    """

    def execute_sql(self, sql: str, warehouse_id: str | None = None) -> list[dict[str, object]]:
        """Execute a SQL statement and return results as list of dicts.

        Args:
            sql: SQL statement to execute
            warehouse_id: Optional SQL warehouse ID (required for REST backend)

        Returns:
            List of row dicts with column names as keys
        """
        ...

    def get_cluster_config(self, cluster_id: str) -> ClusterConfig:
        """Get cluster configuration by ID.

        Args:
            cluster_id: Cluster ID to query

        Returns:
            ClusterConfig with cluster details
        """
        ...

    def get_recent_queries(self, limit: int = 100) -> list[QueryRecord]:
        """Get recent query history.

        Args:
            limit: Maximum number of queries to return

        Returns:
            List of QueryRecord objects
        """
        ...

    def describe_table(self, table_name: str) -> DeltaTableInfo:
        """Get Delta table metadata.

        Args:
            table_name: Fully qualified table name

        Returns:
            DeltaTableInfo with table metadata
        """
        ...

    def get_session_metrics(self) -> dict[str, object]:
        """Get current session metrics.

        Returns:
            Dict with session-level metrics (stages, tasks, etc.)
            Raises NotAvailableError if not available in current context.
        """
        ...
