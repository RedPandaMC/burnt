import pytest

from burnt.core.models import ClusterConfig, CostEstimate, PricingInfo


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
