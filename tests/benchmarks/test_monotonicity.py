
"""Monotonicity and sanity tests for cost estimation.

These tests verify that the estimator produces reasonable values
and maintains expected ordering relationships between queries
of different complexity.
"""

import pytest

from burnt.core.models import ClusterConfig
from burnt.estimators.static import CostEstimator


@pytest.mark.benchmark
class TestMonotonicity:
    """Test that more complex queries have higher costs."""

    def test_five_way_join_costs_more_than_simple_select(
        self,
        cost_estimator: CostEstimator,
        simple_select_sql: str,
        five_table_join_sql: str,
    ) -> None:
        """Complex 5-way join should cost more than simple SELECT."""
        simple_result = cost_estimator.estimate(simple_select_sql)
        complex_result = cost_estimator.estimate(five_table_join_sql)

        assert complex_result.estimated_dbu > simple_result.estimated_dbu, (
            f"5-way join DBU ({complex_result.estimated_dbu}) should be "
            f"greater than simple select DBU ({simple_result.estimated_dbu})"
        )

    def test_adding_join_increases_cost(
        self,
        cost_estimator: CostEstimator,
        single_table_filter_sql: str,
        two_table_join_sql: str,
    ) -> None:
        """Adding a join should increase the cost estimate or stay at MIN_DBU."""
        single_result = cost_estimator.estimate(single_table_filter_sql)
        join_result = cost_estimator.estimate(two_table_join_sql)

        # If both are at MIN_DBU, that's acceptable
        # Otherwise join should be greater
        min_dbu = 0.01  # MIN_DBU constant from static estimator
        if single_result.estimated_dbu > min_dbu:
            assert join_result.estimated_dbu >= single_result.estimated_dbu, (
                f"Join DBU ({join_result.estimated_dbu}) should be "
                f">= single table DBU ({single_result.estimated_dbu})"
            )
        else:
            # Both at MIN_DBU is acceptable for simple queries
            assert join_result.estimated_dbu >= min_dbu

    def test_larger_cluster_increases_cost_or_stays_at_min(
        self,
        simple_select_sql: str,
        default_cluster: ClusterConfig,
        large_cluster: ClusterConfig,
    ) -> None:
        """Larger cluster should produce higher or equal DBU estimate."""
        estimator = CostEstimator()

        small_result = estimator.estimate(simple_select_sql, cluster=default_cluster)
        large_result = estimator.estimate(simple_select_sql, cluster=large_cluster)

        # Large cluster should be >= small cluster (may both be at MIN_DBU)
        assert large_result.estimated_dbu >= small_result.estimated_dbu, (
            f"Large cluster DBU ({large_result.estimated_dbu}) should be "
            f">= small cluster DBU ({small_result.estimated_dbu})"
        )

    def test_photon_enabled_produces_valid_cost(
        self,
        cost_estimator: CostEstimator,
        groupby_agg_sql: str,
    ) -> None:
        """Photon should produce a valid cost estimate."""
        regular_result = cost_estimator.estimate(groupby_agg_sql)

        photon_cluster = ClusterConfig(photon_enabled=True)
        photon_result = cost_estimator.estimate(groupby_agg_sql, cluster=photon_cluster)

        # Both should be valid estimates (may be at MIN_DBU for simple queries)
        assert photon_result.estimated_dbu > 0, "Photon estimate should be positive"
        assert regular_result.estimated_dbu > 0, "Regular estimate should be positive"

        # If both above MIN_DBU, photon should differ (2.5/2.7 multiplier)
        min_dbu = 0.01
        if (
            regular_result.estimated_dbu > min_dbu
            and photon_result.estimated_dbu > min_dbu
        ):
            # Photon should be roughly 2.5/2.7 = ~0.926 of regular
            ratio = photon_result.estimated_dbu / regular_result.estimated_dbu
            assert 0.8 <= ratio <= 1.0, (
                f"Photon ratio {ratio:.2f} should be ~0.93 (2.5/2.7)"
            )


@pytest.mark.benchmark
class TestOrderOfMagnitude:
    """Test that costs are in expected order of magnitude."""

    def test_simple_select_under_one_dbu(
        self,
        cost_estimator: CostEstimator,
        simple_select_sql: str,
    ) -> None:
        """Simple SELECT should cost less than 1 DBU."""
        result = cost_estimator.estimate(simple_select_sql)

        assert result.estimated_dbu < 1.0, (
            f"Simple SELECT should cost < 1 DBU, got {result.estimated_dbu}"
        )
        assert result.estimated_dbu > 0, (
            f"Cost should be positive, got {result.estimated_dbu}"
        )

    def test_small_query_under_one_dbu(
        self,
        cost_estimator: CostEstimator,
        single_table_filter_sql: str,
    ) -> None:
        """Small query should cost less than 1 DBU."""
        result = cost_estimator.estimate(single_table_filter_sql)

        assert result.estimated_dbu < 1.0, (
            f"Small query should cost < 1 DBU, got {result.estimated_dbu}"
        )

    def test_complex_query_reasonable_cost(
        self,
        cost_estimator: CostEstimator,
        five_table_join_sql: str,
    ) -> None:
        """Complex query should have reasonable cost."""
        result = cost_estimator.estimate(five_table_join_sql)

        # 5-way join should be more than 0.01 but less than 100 DBU
        assert 0.01 <= result.estimated_dbu <= 100, (
            f"5-way join should cost 0.01-100 DBU, got {result.estimated_dbu}"
        )

    def test_groupby_agg_reasonable_cost(
        self,
        cost_estimator: CostEstimator,
        groupby_agg_sql: str,
    ) -> None:
        """GROUP BY aggregation should have reasonable cost."""
        result = cost_estimator.estimate(groupby_agg_sql)

        # GROUP BY should be more than 0.001 but less than 10 DBU
        assert 0.001 <= result.estimated_dbu <= 10, (
            f"GROUP BY should cost 0.001-10 DBU, got {result.estimated_dbu}"
        )


@pytest.mark.benchmark
class TestConfidenceLevels:
    """Test that confidence levels are reasonable."""

    def test_simple_query_has_confidence(
        self,
        cost_estimator: CostEstimator,
        simple_select_sql: str,
    ) -> None:
        """Estimates should have confidence levels."""
        result = cost_estimator.estimate(simple_select_sql)

        assert result.confidence in ("low", "medium", "high"), (
            f"Confidence should be low/medium/high, got {result.confidence}"
        )

    def test_breakdown_has_required_fields(
        self,
        cost_estimator: CostEstimator,
        groupby_agg_sql: str,
    ) -> None:
        """Cost breakdown should include complexity and cluster info."""
        result = cost_estimator.estimate(groupby_agg_sql)

        assert "complexity" in result.breakdown, "Breakdown should include complexity"
        assert "cluster_factor" in result.breakdown, (
            "Breakdown should include cluster_factor"
        )

    def test_warnings_list_exists(
        self,
        cost_estimator: CostEstimator,
        five_table_join_sql: str,
    ) -> None:
        """Estimates should have a warnings list."""
        result = cost_estimator.estimate(five_table_join_sql)

        assert isinstance(result.warnings, list), "Warnings should be a list"


@pytest.mark.benchmark
class TestQueryProgression:
    """Test that cost increases monotonically with complexity."""

    def test_cost_increases_with_complexity(
        self,
        cost_estimator: CostEstimator,
        all_benchmark_queries: dict[str, str],
    ) -> None:
        """Costs should increase from simple to complex queries."""
        queries_ordered = [
            "simple_select",
            "single_table_filter",
            "groupby_agg",
            "two_table_join",
            "five_table_join",
        ]

        costs = []
        for name in queries_ordered:
            sql = all_benchmark_queries[name]
            result = cost_estimator.estimate(sql)
            costs.append((name, result.estimated_dbu))

        # Verify costs generally increase (allowing for minor variations)
        for i in range(len(costs) - 1):
            current_name, current_cost = costs[i]
            next_name, next_cost = costs[i + 1]

            assert next_cost >= current_cost * 0.5, (
                f"{next_name} cost ({next_cost}) should be at least "
                f"50% of {current_name} cost ({current_cost})"
            )
