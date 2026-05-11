import warnings

import pytest

from burnt import CostBudgetExceeded
from burnt.core.exchange import FixedRateProvider
from burnt.core.models import (
    ClusterConfig,
    ClusterRecommendation,
    CostEstimate,
    OperationInfo,
    PricingInfo,
    QueryProfile,
)


class TestOperationInfo:
    def test_operation_info_creation(self):
        op = OperationInfo(name="Join", kind="INNER", weight=10.0)
        assert op.name == "Join"
        assert op.kind == "INNER"
        assert op.weight == 10.0

    def test_operation_info_default_values(self):
        op = OperationInfo(name="Join", kind="", weight=10.0)
        assert op.name == "Join"
        assert op.kind == ""


class TestQueryProfile:
    def test_query_profile_creation(self):
        profile = QueryProfile(
            sql="SELECT * FROM users",
            dialect="databricks",
            operations=[],
            tables=["users"],
            complexity_score=10.0,
        )
        assert profile.sql == "SELECT * FROM users"
        assert profile.dialect == "databricks"
        assert profile.tables == ["users"]

    def test_query_profile_default_values(self):
        profile = QueryProfile(sql="SELECT 1")
        assert profile.dialect == "databricks"
        assert profile.operations == []
        assert profile.tables == []
        assert profile.complexity_score == 0.0


class TestClusterConfig:
    def test_cluster_config_creation(self):
        cluster = ClusterConfig(
            instance_type="Standard_DS3_v2",
            num_workers=4,
            dbu_per_hour=0.75,
            photon_enabled=True,
        )
        assert cluster.instance_type == "Standard_DS3_v2"
        assert cluster.num_workers == 4
        assert cluster.dbu_per_hour == 0.75
        assert cluster.photon_enabled is True

    def test_cluster_config_default_values(self):
        cluster = ClusterConfig()
        assert cluster.instance_type == "Standard_DS3_v2"
        assert cluster.num_workers == 2
        assert cluster.dbu_per_hour == 0.75
        assert cluster.photon_enabled is False

    def test_cluster_config_is_frozen(self):
        from pydantic_core import ValidationError

        cluster = ClusterConfig(num_workers=4)
        with pytest.raises(ValidationError):
            cluster.num_workers = 8


class TestPricingInfo:
    def test_pricing_info_creation(self):
        pricing = PricingInfo(sku_name="ALL_PURPOSE", dbu_rate=0.55)
        assert pricing.sku_name == "ALL_PURPOSE"
        assert pricing.dbu_rate == 0.55

    def test_pricing_info_default_values(self):
        pricing = PricingInfo(sku_name="JOBS_COMPUTE", dbu_rate=0.30)
        assert pricing.cloud == "AZURE"
        assert pricing.region == "EAST_US"


class TestCostEstimate:
    def test_cost_estimate_creation(self):
        estimate = CostEstimate(
            estimated_dbu=100.0,
            costs={"USD": 55.0},
            confidence="high",
            breakdown={"complexity": 50.0},
            warnings=[],
        )
        assert estimate.estimated_dbu == 100.0
        assert estimate.costs["USD"] == 55.0
        assert estimate.confidence == "high"

    def test_cost_estimate_default_confidence(self):
        estimate = CostEstimate(estimated_dbu=10.0)
        assert estimate.confidence == "low"

    def test_cost_estimate_costs_empty_by_default(self):
        estimate = CostEstimate(estimated_dbu=100.0)
        assert estimate.costs == {}

    def test_cost_estimate_confidence_values(self):
        for conf in ["low", "medium", "high"]:
            estimate = CostEstimate(estimated_dbu=10.0, confidence=conf)
            assert estimate.confidence == conf

        with pytest.raises(ValueError):
            CostEstimate(estimated_dbu=10.0, confidence="invalid")

    # ------------------------------------------------------------------
    # cost_in / convert_to / primary_cost
    # ------------------------------------------------------------------

    def test_cost_in_returns_value_from_costs(self):
        estimate = CostEstimate(costs={"USD": 50.0, "EUR": 46.0})
        assert estimate.cost_in("USD") == 50.0
        assert estimate.cost_in("EUR") == 46.0

    def test_cost_in_is_case_insensitive(self):
        estimate = CostEstimate(costs={"GBP": 42.0})
        assert estimate.cost_in("gbp") == 42.0
        assert estimate.cost_in("GBP") == 42.0

    def test_cost_in_returns_none_for_missing_currency(self):
        estimate = CostEstimate(costs={"USD": 50.0})
        assert estimate.cost_in("JPY") is None

    def test_cost_in_returns_none_on_empty_costs(self):
        estimate = CostEstimate()
        assert estimate.cost_in("USD") is None

    def test_convert_to_direct_hit(self):
        estimate = CostEstimate(costs={"USD": 50.0, "EUR": 46.0})
        assert estimate.convert_to("EUR") == 46.0

    def test_convert_to_uses_injected_exchange_provider(self):
        from decimal import Decimal

        estimate = CostEstimate(costs={"USD": 100.0})
        provider = FixedRateProvider(Decimal("0.85"))
        result = estimate.convert_to("EUR", exchange=provider)
        assert abs(result - 85.0) < 0.01

    def test_convert_to_raises_on_empty_costs(self):
        estimate = CostEstimate()
        with pytest.raises(ValueError, match="No cost data"):
            estimate.convert_to("USD")

    def test_primary_cost_prefers_usd(self):
        estimate = CostEstimate(costs={"EUR": 46.0, "USD": 50.0, "GBP": 42.0})
        assert estimate.primary_cost == 50.0
        assert estimate.primary_currency == "USD"

    def test_primary_cost_falls_back_to_eur(self):
        estimate = CostEstimate(costs={"EUR": 46.0, "GBP": 42.0})
        assert estimate.primary_cost == 46.0
        assert estimate.primary_currency == "EUR"

    def test_primary_cost_none_when_empty(self):
        estimate = CostEstimate()
        assert estimate.primary_cost is None
        assert estimate.primary_currency is None

    def test_comparison_table_shows_all_costs(self):
        estimate = CostEstimate(costs={"USD": 50.0, "EUR": 46.0, "GBP": 42.0})
        table = estimate.comparison_table()
        assert "USD" in table
        assert "EUR" in table
        assert "GBP" in table

    def test_comparison_table_no_costs(self):
        estimate = CostEstimate(estimated_dbu=10.0)
        table = estimate.comparison_table()
        assert "Cost Estimate" in table
        assert "Confidence" in table


class TestClusterRecommendation:
    def test_cluster_recommendation_creation(self):
        economy = ClusterConfig(num_workers=2, instance_type="Standard_DS3_v2")
        balanced = ClusterConfig(num_workers=4, instance_type="Standard_DS4_v2")
        performance = ClusterConfig(num_workers=8, instance_type="Standard_DS5_v2")
        recommendation = ClusterRecommendation(
            economy=economy,
            balanced=balanced,
            performance=performance,
            current_cost_usd=10.0,
            rationale="Underutilized CPU",
        )
        assert recommendation.economy.num_workers == 2
        assert recommendation.balanced.num_workers == 4
        assert recommendation.performance.num_workers == 8
        assert recommendation.current_cost_usd == 10.0

    def test_cluster_recommendation_comparison_table(self):
        economy = ClusterConfig(num_workers=2, instance_type="Standard_DS3_v2")
        balanced = ClusterConfig(num_workers=4, instance_type="Standard_DS4_v2")
        performance = ClusterConfig(num_workers=8, instance_type="Standard_DS5_v2")
        recommendation = ClusterRecommendation(
            economy=economy,
            balanced=balanced,
            performance=performance,
            current_cost_usd=10.0,
            rationale="Test workload",
        )
        table = recommendation.comparison_table()
        assert "Economy" in table
        assert "Balanced" in table
        assert "Performance" in table
        assert "Standard_DS3_v2" in table


class TestCostBudgetExceeded:
    def test_raise_if_exceeds_under_budget_returns_self(self):
        estimate = CostEstimate(estimated_dbu=10.0, costs={"USD": 5.0})
        result = estimate.raise_if_exceeds(50.0)
        assert result is estimate

    def test_raise_if_exceeds_over_budget_raises(self):
        estimate = CostEstimate(estimated_dbu=100.0, costs={"USD": 50.0})
        with pytest.raises(CostBudgetExceeded):
            estimate.raise_if_exceeds(10.0)

    def test_raise_if_exceeds_over_budget_exception_attributes(self):
        estimate = CostEstimate(estimated_dbu=100.0, costs={"USD": 50.0})
        with pytest.raises(CostBudgetExceeded) as exc_info:
            estimate.raise_if_exceeds(10.0)
        assert exc_info.value.estimate is estimate
        assert exc_info.value.budget == 10.0
        assert exc_info.value.currency == "USD"

    def test_raise_if_exceeds_label_in_message(self):
        estimate = CostEstimate(estimated_dbu=100.0, costs={"USD": 50.0})
        with pytest.raises(CostBudgetExceeded) as exc_info:
            estimate.raise_if_exceeds(10.0, label="daily_agg")
        assert "daily_agg" in str(exc_info.value)

    def test_raise_if_exceeds_no_costs_warns_and_returns_self(self):
        estimate = CostEstimate(estimated_dbu=10.0)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = estimate.raise_if_exceeds(50.0)
            assert result is estimate
            assert len(w) == 1
            assert "no cost data" in str(w[0].message)

    def test_raise_if_exceeds_no_costs_with_label_warns(self):
        estimate = CostEstimate(estimated_dbu=10.0)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            estimate.raise_if_exceeds(50.0, label="test_query")
            assert len(w) == 1
            assert "test_query" in str(w[0].message)

    def test_raise_if_exceeds_non_usd_currency(self):
        estimate = CostEstimate(costs={"GBP": 42.0})
        with pytest.raises(CostBudgetExceeded) as exc_info:
            estimate.raise_if_exceeds(10.0, currency="GBP")
        assert exc_info.value.currency == "GBP"

    def test_raise_if_exceeds_with_multiple_currencies(self):
        estimate = CostEstimate(costs={"USD": 100.0, "EUR": 85.0})
        with pytest.raises(CostBudgetExceeded):
            estimate.raise_if_exceeds(50.0, currency="EUR")

    def test_raise_if_exceeds_chaining(self):
        estimate = CostEstimate(estimated_dbu=10.0, costs={"USD": 5.0})
        result = estimate.raise_if_exceeds(50.0)
        assert result is estimate
        assert result.costs["USD"] == 5.0
