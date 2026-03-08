"""TableRegistry for enterprise governance view support.

This module provides a configurable registry of system table paths,
allowing enterprises to use custom governance views instead of the
default system tables.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class TableRegistry:
    """Registry of Databricks system table paths.

    This class provides a central configuration for all system table
    paths used by dburnrate. In enterprise environments, these can be
    overridden to use curated governance views.

    Default paths follow the standard system table schema:
    - system.billing.usage
    - system.billing.list_prices
    - system.query.history
    - system.compute.node_types
    - system.compute.clusters
    - system.compute.node_timeline
    - system.lakeflow.jobs
    - system.lakeflow.job_run_timeline
    - system.storage.predictive_optimization_operations_history

    Environment variable overrides:
    - DBURNRATE_TABLE_BILLING_USAGE
    - DBURNRATE_TABLE_BILLING_LIST_PRICES
    - DBURNRATE_TABLE_QUERY_HISTORY
    - DBURNRATE_TABLE_COMPUTE_NODE_TYPES
    - DBURNRATE_TABLE_COMPUTE_CLUSTERS
    - DBURNRATE_TABLE_COMPUTE_NODE_TIMELINE
    - DBURNRATE_TABLE_LACEFLOW_JOBS
    - DBURNRATE_TABLE_LAKEFLOW_JOB_RUN_TIMELINE
    - DBURNRATE_TABLE_PREDICTIVE_OPTIMIZATION

    Example:
        >>> registry = TableRegistry()
        >>> registry.billing_usage
        'system.billing.usage'
        >>> registry = TableRegistry.from_env()
        >>> # Or with custom paths:
        >>> registry = TableRegistry(billing_usage="governance.cost.v_billing")
    """

    billing_usage: str = "system.billing.usage"
    billing_list_prices: str = "system.billing.list_prices"
    query_history: str = "system.query.history"
    compute_node_types: str = "system.compute.node_types"
    compute_clusters: str = "system.compute.clusters"
    compute_node_timeline: str = "system.compute.node_timeline"
    lakeflow_jobs: str = "system.lakeflow.jobs"
    lakeflow_job_run_timeline: str = "system.lakeflow.job_run_timeline"
    predictive_optimization: str = (
        "system.storage.predictive_optimization_operations_history"
    )
    column_overrides: dict[str, dict[str, str]] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> TableRegistry:
        """Load table paths from environment variables.

        Environment variables with prefix DBURNRATE_TABLE_ are used to
        override default table paths.

        Returns:
            TableRegistry with overrides from environment

        Example:
            >>> # In shell:
            >>> export DBURNRATE_TABLE_BILLING_USAGE=governance.cost.v_billing
            >>> # In Python:
            >>> registry = TableRegistry.from_env()
            >>> registry.billing_usage
            'governance.cost.v_billing'
        """
        overrides = {}

        env_mapping = {
            "DBURNRATE_TABLE_BILLING_USAGE": "billing_usage",
            "DBURNRATE_TABLE_BILLING_LIST_PRICES": "billing_list_prices",
            "DBURNRATE_TABLE_QUERY_HISTORY": "query_history",
            "DBURNRATE_TABLE_COMPUTE_NODE_TYPES": "compute_node_types",
            "DBURNRATE_TABLE_COMPUTE_CLUSTERS": "compute_clusters",
            "DBURNRATE_TABLE_COMPUTE_NODE_TIMELINE": "compute_node_timeline",
            "DBURNRATE_TABLE_LAKEFLOW_JOBS": "lakeflow_jobs",
            "DBURNRATE_TABLE_LAKEFLOW_JOB_RUN_TIMELINE": "lakeflow_job_run_timeline",
            "DBURNRATE_TABLE_PREDICTIVE_OPTIMIZATION": "predictive_optimization",
        }

        for env_key, attr_name in env_mapping.items():
            if value := os.environ.get(env_key):
                overrides[attr_name] = value

        return cls(**overrides)

    def with_overrides(
        self,
        column_overrides: dict[str, dict[str, str]],
    ) -> TableRegistry:
        """Create a new registry with additional column overrides.

        Column overrides allow mapping system table columns to custom
        column names used in governance views.

        Args:
            column_overrides: Dict mapping table names to column mappings

        Returns:
            New TableRegistry instance with overrides

        Example:
            >>> registry = TableRegistry()
            >>> registry = registry.with_overrides(
            ...     {"billing_usage": {"custom_sku": "sku_name"}}
            ... )
        """
        new_overrides = self.column_overrides.copy()
        for table, cols in column_overrides.items():
            if table in new_overrides:
                new_overrides[table].update(cols)
            else:
                new_overrides[table] = cols
        return TableRegistry(
            billing_usage=self.billing_usage,
            billing_list_prices=self.billing_list_prices,
            query_history=self.query_history,
            compute_node_types=self.compute_node_types,
            compute_clusters=self.compute_clusters,
            compute_node_timeline=self.compute_node_timeline,
            lakeflow_jobs=self.lakeflow_jobs,
            lakeflow_job_run_timeline=self.lakeflow_job_run_timeline,
            predictive_optimization=self.predictive_optimization,
            column_overrides=new_overrides,
        )
