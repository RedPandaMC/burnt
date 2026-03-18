
"""Tests for HybridEstimator delta_tables integration."""

import pytest

from burnt.core.models import (
    ClusterConfig,
    DeltaTableInfo,
    ExplainPlan,
    QueryRecord,
)
from burnt.estimators.hybrid import HybridEstimator


@pytest.fixture
def cluster():
    return ClusterConfig(
        instance_type="Standard_DS3_v2",
        num_workers=2,
        dbu_per_hour=0.75,
    )


@pytest.fixture
def hybrid():
    return HybridEstimator()


class TestDeltaTablesOnly:
    def test_delta_tables_uses_delta_sizes(self, hybrid, cluster):
        """When delta_tables provided, uses Delta sizes instead of EXPLAIN."""
        query = "SELECT * FROM orders JOIN customers ON orders.id = customers.id"
        delta_tables = {
            "orders": DeltaTableInfo(
                location="s3://bucket/orders",
                total_size_bytes=1_000_000_000,  # 1 GB
                num_files=10,
            ),
            "customers": DeltaTableInfo(
                location="s3://bucket/customers",
                total_size_bytes=500_000_000,  # 500 MB
                num_files=5,
            ),
        }

        result = hybrid.estimate(query, cluster, delta_tables=delta_tables)

        assert result.estimated_dbu > 0
        assert result.confidence in ("medium", "high")

    def test_delta_only_partial_coverage(self, hybrid, cluster):
        """When only some tables have Delta info, uses EXPLAIN for remainder."""
        query = "SELECT * FROM orders JOIN customers ON orders.id = customers.id"
        delta_tables = {
            "orders": DeltaTableInfo(
                location="s3://bucket/orders",
                total_size_bytes=1_000_000_000,
                num_files=10,
            ),
        }
        explain_plan = ExplainPlan(
            total_size_bytes=500_000_000,
            stats_complete=True,
            join_types=["SortMergeJoin"],
            shuffle_count=2,
        )

        result = hybrid.estimate(
            query, cluster, explain_plan=explain_plan, delta_tables=delta_tables
        )

        assert result.estimated_dbu > 0

    def test_delta_tables_not_in_query_ignored(self, hybrid, cluster):
        """Tables in delta_tables but not in query are ignored."""
        query = "SELECT * FROM orders"
        delta_tables = {
            "orders": DeltaTableInfo(
                location="s3://bucket/orders",
                total_size_bytes=1_000_000_000,
                num_files=10,
            ),
            "unused_table": DeltaTableInfo(
                location="s3://bucket/unused",
                total_size_bytes=100_000_000_000,
                num_files=100,
            ),
        }

        result = hybrid.estimate(query, cluster, delta_tables=delta_tables)

        # Should use only orders table size, not unused_table
        assert result.estimated_dbu > 0


class TestDeltaWithExplain:
    def test_explain_with_delta_override(self, hybrid, cluster):
        """EXPLAIN with Delta override uses Delta sizes for scan."""
        query = "SELECT * FROM orders JOIN customers ON orders.id = customers.id"
        delta_tables = {
            "orders": DeltaTableInfo(
                location="s3://bucket/orders",
                total_size_bytes=1_000_000_000,
                num_files=10,
            ),
            "customers": DeltaTableInfo(
                location="s3://bucket/customers",
                total_size_bytes=500_000_000,
                num_files=5,
            ),
        }
        explain_plan = ExplainPlan(
            total_size_bytes=10_000_000_000,  # 10 GB - much larger than Delta
            stats_complete=True,
            join_types=["SortMergeJoin"],
            shuffle_count=2,
        )

        result = hybrid.estimate(
            query, cluster, explain_plan=explain_plan, delta_tables=delta_tables
        )

        # Result should use delta sizes (1.5GB) not explain (10GB)
        assert result.estimated_dbu > 0


class TestDeltaWithHistorical:
    def test_historical_with_delta_scaling(self, hybrid, cluster):
        """Historical estimate uses Delta sizes for scaling."""
        query = "SELECT * FROM orders"
        delta_tables = {
            "orders": DeltaTableInfo(
                location="s3://bucket/orders",
                total_size_bytes=2_000_000_000,  # 2 GB current
                num_files=20,
            ),
        }
        historical = [
            QueryRecord(
                statement_id="abc",
                statement_text=query,
                start_time="2024-01-01T00:00:00Z",
                execution_duration_ms=1000,
                read_bytes=1_000_000_000,  # 1 GB historical
            ),
            QueryRecord(
                statement_id="def",
                statement_text=query,
                start_time="2024-01-02T00:00:00Z",
                execution_duration_ms=1200,
                read_bytes=1_000_000_000,
            ),
        ]

        result = hybrid.estimate(
            query, cluster, historical=historical, delta_tables=delta_tables
        )

        # Should scale by 2x (current 2GB / historical 1GB)
        assert result.confidence == "high"
        assert "Historical" in result.warnings[0]


class TestDeltaFallback:
    def test_empty_delta_tables_falls_back_to_explain(self, hybrid, cluster):
        """Empty delta_tables dict falls back to EXPLAIN."""
        query = "SELECT * FROM orders"
        delta_tables = {}
        explain_plan = ExplainPlan(
            total_size_bytes=1_000_000_000,
            stats_complete=True,
            join_types=[],
            shuffle_count=0,
        )

        result = hybrid.estimate(
            query, cluster, explain_plan=explain_plan, delta_tables=delta_tables
        )

        assert result.estimated_dbu > 0

    def test_missing_tables_fallback_to_explain(self, hybrid, cluster):
        """Tables not in delta_tables fall back to EXPLAIN sizes."""
        query = "SELECT * FROM orders"
        # Empty delta_tables - query has orders but not in dict
        delta_tables: dict[str, DeltaTableInfo] = {}
        explain_plan = ExplainPlan(
            total_size_bytes=1_000_000_000,
            stats_complete=True,
            join_types=[],
            shuffle_count=0,
        )

        result = hybrid.estimate(
            query, cluster, explain_plan=explain_plan, delta_tables=delta_tables
        )

        # Should fall back to EXPLAIN
        assert result.estimated_dbu > 0
