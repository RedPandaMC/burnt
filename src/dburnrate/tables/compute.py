"""Compute system table queries for node types, clusters, and timelines."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .connection import DatabricksClient

from ..core.exceptions import DatabricksQueryError
from ..core.models import ClusterConfig
from .connection import _sanitize_id

_NODE_TYPE_COLUMNS = (
    "node_type_id, num_cores, memory_mb, instance_type_id, dbu_per_hour"
)
_CLUSTER_COLUMNS = (
    "cluster_id, cluster_name, node_type_id, driver_node_type_id, "
    "num_workers, autoscale_min_workers, autoscale_max_workers, "
    "spark_version, cluster_source"
)
_TIMELINE_COLUMNS = "cluster_id, node_type, start_time, end_time, driver, num_nodes"


def get_node_types(client: DatabricksClient, warehouse_id: str) -> dict[str, float]:
    """Return mapping of node_type_id to dbu_per_hour from system.compute.node_types."""
    sql = f"SELECT {_NODE_TYPE_COLUMNS} FROM system.compute.node_types"
    rows = client.execute_sql(sql, warehouse_id)
    return {
        row["node_type_id"]: float(row["dbu_per_hour"])
        for row in rows
        if row.get("dbu_per_hour") is not None
    }


def get_cluster_config(
    client: DatabricksClient, cluster_id: str, warehouse_id: str
) -> ClusterConfig:
    """Fetch cluster configuration from system.compute.clusters and map to ClusterConfig.

    Raises DatabricksQueryError if the cluster is not found.
    """
    safe_cluster_id = _sanitize_id(cluster_id, "cluster_id")
    sql = f"""
        SELECT {_CLUSTER_COLUMNS}
        FROM system.compute.clusters
        WHERE cluster_id = '{safe_cluster_id}'
        LIMIT 1
    """
    rows = client.execute_sql(sql, warehouse_id)
    if not rows:
        raise DatabricksQueryError(f"Cluster not found: {cluster_id}")

    node_types = get_node_types(client, warehouse_id)
    row = rows[0]
    node_type_id = row.get("node_type_id", "")
    dbu_per_hour = node_types.get(node_type_id, 0.0)
    num_workers = _parse_int(row.get("num_workers")) or 1

    return ClusterConfig(
        instance_type=node_type_id,
        num_workers=num_workers,
        dbu_per_hour=dbu_per_hour,
    )


def get_node_timeline(
    client: DatabricksClient,
    cluster_id: str,
    start_time: str,
    end_time: str,
    warehouse_id: str,
) -> list[dict[str, Any]]:
    """Fetch node utilization timeline for a cluster within a time range."""
    safe_cluster_id = _sanitize_id(cluster_id, "cluster_id")
    sql = f"""
        SELECT {_TIMELINE_COLUMNS}
        FROM system.compute.node_timeline
        WHERE cluster_id = '{safe_cluster_id}'
          AND start_time >= '{start_time}'
          AND end_time <= '{end_time}'
        ORDER BY start_time
    """
    return client.execute_sql(sql, warehouse_id)


def _parse_int(value: Any) -> int | None:
    """Parse a value to int, returning None if not possible."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None
