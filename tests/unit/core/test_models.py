import warnings

import pytest

from burnt import CostBudgetExceeded
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
            estimated_cost_usd=55.0,
            confidence="high",
            breakdown={"complexity": 50.0},
            warnings=[],
        )
        assert estimate.estimated_dbu == 100.0
        assert estimate.estimated_cost_usd == 55.0
        assert estimate.confidence == "high"

    def test_cost_estimate_default_confidence(self):
        estimate = CostEstimate(estimated_dbu=10.0)
        assert estimate.confidence == "low"

    def test_cost_estimate_optional_fields(self):
        estimate = CostEstimate(estimated_dbu=100.0)
        assert estimate.estimated_cost_usd is None
        assert estimate.estimated_cost_eur is None
        assert estimate.warnings == []

    def test_cost_estimate_confidence_values(self):
        for conf in ["low", "medium", "high"]:
            estimate = CostEstimate(estimated_dbu=10.0, confidence=conf)
            assert estimate.confidence == conf

        with pytest.raises(ValueError):
            CostEstimate(estimated_dbu=10.0, confidence="invalid")


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
        estimate = CostEstimate(estimated_dbu=10.0, estimated_cost_usd=5.0)
        result = estimate.raise_if_exceeds(50.0)
        assert result is estimate

    def test_raise_if_exceeds_over_budget_raises(self):
        estimate = CostEstimate(estimated_dbu=100.0, estimated_cost_usd=50.0)
        with pytest.raises(CostBudgetExceeded):
            estimate.raise_if_exceeds(10.0)

    def test_raise_if_exceeds_over_budget_exception_attributes(self):
        estimate = CostEstimate(estimated_dbu=100.0, estimated_cost_usd=50.0)
        with pytest.raises(CostBudgetExceeded) as exc_info:
            estimate.raise_if_exceeds(10.0)
        assert exc_info.value.estimate is estimate
        assert exc_info.value.budget == 10.0
        assert exc_info.value.currency == "USD"

    def test_raise_if_exceeds_label_in_message(self):
        estimate = CostEstimate(estimated_dbu=100.0, estimated_cost_usd=50.0)
        with pytest.raises(CostBudgetExceeded) as exc_info:
            estimate.raise_if_exceeds(10.0, label="daily_agg")
        assert "daily_agg" in str(exc_info.value)

    def test_raise_if_exceeds_none_cost_warns_and_returns_self(self):
        estimate = CostEstimate(estimated_dbu=10.0)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = estimate.raise_if_exceeds(50.0)
            assert result is estimate
            assert len(w) == 1
            assert "estimated_cost_usd is None" in str(w[0].message)

    def test_raise_if_exceeds_none_cost_with_label_warns(self):
        estimate = CostEstimate(estimated_dbu=10.0)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            estimate.raise_if_exceeds(50.0, label="test_query")
            assert len(w) == 1
            assert "test_query" in str(w[0].message)

    def test_raise_if_exceeds_chaining(self):
        estimate = CostEstimate(estimated_dbu=10.0, estimated_cost_usd=5.0)
        result = estimate.raise_if_exceeds(50.0)
        assert result is estimate
        assert result.estimated_cost_usd == 5.0
