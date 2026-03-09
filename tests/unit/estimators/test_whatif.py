from burnt.core.models import ClusterConfig, CostEstimate
from burnt.estimators.whatif import (
    SPEEDUP_FACTORS,
    apply_cluster_resize,
    apply_photon_scenario,
    apply_serverless_migration,
)


class TestApplyPhotonScenario:
    def test_apply_photon_scenario_complex_join(self):
        estimate = CostEstimate(
            estimated_dbu=100.0,
            estimated_cost_usd=55.0,
            confidence="high",
        )
        result = apply_photon_scenario(estimate, "complex_join")

        assert result.estimated_dbu != estimate.estimated_dbu
        assert result.breakdown.get("photon")
        assert result.breakdown.get("speedup") == SPEEDUP_FACTORS["complex_join"]

    def test_apply_photon_scenario_aggregation(self):
        estimate = CostEstimate(
            estimated_dbu=100.0,
            estimated_cost_usd=30.0,
            confidence="high",
        )
        result = apply_photon_scenario(estimate, "aggregation")

        assert result.breakdown.get("speedup") == SPEEDUP_FACTORS["aggregation"]

    def test_apply_photon_scenario_unknown_type(self):
        estimate = CostEstimate(
            estimated_dbu=100.0,
            estimated_cost_usd=55.0,
            confidence="high",
        )
        result = apply_photon_scenario(estimate, "unknown_type")

        assert result.breakdown.get("speedup") == 2.0

    def test_apply_photon_warning_when_cost_increases(self):
        estimate = CostEstimate(
            estimated_dbu=10.0,
            estimated_cost_usd=5.5,
            confidence="high",
        )
        result = apply_photon_scenario(estimate, "simple_insert")

        assert len(result.warnings) > 0


class TestApplyClusterResize:
    def test_apply_cluster_resize_increase_workers(self):
        estimate = CostEstimate(
            estimated_dbu=100.0,
            estimated_cost_usd=55.0,
            confidence="medium",
        )
        current = ClusterConfig(num_workers=2, dbu_per_hour=0.75)
        new = ClusterConfig(num_workers=4, dbu_per_hour=0.75)

        result = apply_cluster_resize(estimate, current, new)

        assert result.estimated_cost_usd > estimate.estimated_cost_usd
        assert result.breakdown.get("cluster_resize_ratio") == 2.0

    def test_apply_cluster_resize_decrease_workers(self):
        estimate = CostEstimate(
            estimated_dbu=100.0,
            estimated_cost_usd=110.0,
            confidence="medium",
        )
        current = ClusterConfig(num_workers=4, dbu_per_hour=0.75)
        new = ClusterConfig(num_workers=2, dbu_per_hour=0.75)

        result = apply_cluster_resize(estimate, current, new)

        assert result.estimated_cost_usd < estimate.estimated_cost_usd
        assert "Estimated savings" in result.warnings[0]

    def test_apply_cluster_resize_warning_message(self):
        estimate = CostEstimate(
            estimated_dbu=100.0,
            estimated_cost_usd=55.0,
            confidence="medium",
        )
        current = ClusterConfig(num_workers=2, dbu_per_hour=0.75)
        new = ClusterConfig(num_workers=4, dbu_per_hour=0.75)

        result = apply_cluster_resize(estimate, current, new)

        assert len(result.warnings) > 0


class TestApplyServerlessMigration:
    def test_apply_serverless_migration_cheaper(self):
        estimate = CostEstimate(
            estimated_dbu=100.0,
            estimated_cost_usd=55.0,
            confidence="low",
        )
        result = apply_serverless_migration(estimate, "ALL_PURPOSE", 20.0)

        assert result.estimated_cost_usd != estimate.estimated_cost_usd
        assert "serverless" in result.breakdown

    def test_apply_serverless_migration_expensive(self):
        estimate = CostEstimate(
            estimated_dbu=100.0,
            estimated_cost_usd=55.0,
            confidence="low",
        )
        result = apply_serverless_migration(estimate, "ALL_PURPOSE", 80.0)

        assert "Serverless is" in result.warnings[0]

    def test_apply_serverless_migration_unknown_sku(self):
        estimate = CostEstimate(
            estimated_dbu=100.0,
            estimated_cost_usd=55.0,
            confidence="low",
        )
        result = apply_serverless_migration(estimate, "UNKNOWN_SKU", 50.0)

        assert result.estimated_cost_usd != estimate.estimated_cost_usd
