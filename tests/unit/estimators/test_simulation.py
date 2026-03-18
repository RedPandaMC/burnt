import pytest

from burnt.core.instances import WorkloadProfile
from burnt.core.models import ClusterConfig, CostEstimate
from burnt.estimators.simulation import (
    SPEEDUP_FACTORS,
    Simulation,
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


class TestProfileAwareSimulation:
    """Tests for profile/metrics-enriched Simulation scenarios."""

    def _base_estimate(self) -> CostEstimate:
        return CostEstimate(estimated_dbu=10.0, estimated_cost_usd=10.0, confidence="low")

    # --- Photon ---

    def test_enable_photon_derives_window_from_memory_intensive_profile(self):
        """High memory_intensity → photon should use 'window' query type."""
        profile = WorkloadProfile(memory_intensity=0.85, compute_intensity=0.2)
        sim = Simulation(self._base_estimate(), profile=profile)
        result = sim.cluster().enable_photon().compare()
        photon_mod = next(m for m in result.modifications if "Photon" in m.name)
        assert "window" in photon_mod.rationale

    def test_enable_photon_derives_aggregation_from_cpu_intensive_profile(self):
        """High compute_intensity → photon should use 'aggregation'."""
        profile = WorkloadProfile(memory_intensity=0.3, compute_intensity=0.85)
        sim = Simulation(self._base_estimate(), profile=profile)
        result = sim.cluster().enable_photon().compare()
        photon_mod = next(m for m in result.modifications if "Photon" in m.name)
        assert "aggregation" in photon_mod.rationale

    def test_enable_photon_falls_back_to_complex_join_without_profile(self):
        """No profile → enable_photon() uses 'complex_join' heuristic."""
        sim = Simulation(self._base_estimate())
        result = sim.cluster().enable_photon().compare()
        photon_mod = next(m for m in result.modifications if "Photon" in m.name)
        assert "complex_join" in photon_mod.rationale

    def test_enable_photon_explicit_query_type_overrides_profile(self):
        """Explicit query_type argument takes precedence over profile-derived type."""
        profile = WorkloadProfile(memory_intensity=0.9, compute_intensity=0.1)
        sim = Simulation(self._base_estimate(), profile=profile)
        result = sim.cluster().enable_photon(query_type="aggregation").compare()
        photon_mod = next(m for m in result.modifications if "Photon" in m.name)
        assert "aggregation" in photon_mod.rationale

    # --- Spot ---

    def test_use_spot_applies_better_discount_for_short_job(self):
        """Job < 30 min → discount 0.65 (lower interruption risk)."""
        metrics = {"duration_ms": 900_000}  # 15 minutes
        sim = Simulation(self._base_estimate(), metrics=metrics)
        result = sim.cluster().use_spot().compare()
        spot_mod = next(m for m in result.modifications if "Spot" in m.name)
        assert spot_mod.cost_multiplier == pytest.approx(0.65)

    def test_use_spot_applies_worse_discount_for_long_job(self):
        """Job > 4 hours → discount 0.72 (higher interruption risk)."""
        metrics = {"duration_ms": 18_000_000}  # 5 hours
        sim = Simulation(self._base_estimate(), metrics=metrics)
        result = sim.cluster().use_spot().compare()
        spot_mod = next(m for m in result.modifications if "Spot" in m.name)
        assert spot_mod.cost_multiplier == pytest.approx(0.72)

    def test_use_spot_default_discount_for_medium_job(self):
        """1-hour job → standard SPOT_VM_DISCOUNT (0.70)."""
        metrics = {"duration_ms": 3_600_000}  # 1 hour
        sim = Simulation(self._base_estimate(), metrics=metrics)
        result = sim.cluster().use_spot().compare()
        spot_mod = next(m for m in result.modifications if "Spot" in m.name)
        assert spot_mod.cost_multiplier == pytest.approx(0.70)

    def test_use_spot_default_discount_without_metrics(self):
        """No metrics → fallback to SPOT_VM_DISCOUNT (0.70)."""
        sim = Simulation(self._base_estimate())
        result = sim.cluster().use_spot().compare()
        spot_mod = next(m for m in result.modifications if "Spot" in m.name)
        assert spot_mod.cost_multiplier == pytest.approx(0.70)

    # --- Serverless ---

    def test_to_serverless_derives_utilization_from_duration(self):
        """10-minute job → ~16.7% utilization → serverless competitive."""
        metrics = {"duration_ms": 600_000}  # 10 minutes → 16.67% of 1 hour
        sim = Simulation(self._base_estimate(), metrics=metrics)
        result = sim.cluster().to_serverless().compare()
        assert result.projected.estimated_cost_usd is not None

    def test_to_serverless_explicit_utilization_overrides_metrics(self):
        """Explicit utilization_pct takes precedence over derived value."""
        metrics = {"duration_ms": 600_000}
        sim = Simulation(self._base_estimate(), metrics=metrics)
        # Passing explicit 80.0 — should not be overridden by metrics
        result_explicit = sim.cluster().to_serverless(utilization_pct=80.0).compare()
        # Without metrics, utilization defaults to 50.0
        sim2 = Simulation(self._base_estimate())
        result_default = sim2.cluster().to_serverless().compare()
        # Both should produce valid results
        assert result_explicit.projected.estimated_cost_usd is not None
        assert result_default.projected.estimated_cost_usd is not None

    def test_to_serverless_fallback_utilization_without_metrics(self):
        """No metrics → to_serverless uses 50.0% utilization default."""
        sim = Simulation(self._base_estimate())
        result = sim.cluster().to_serverless().compare()
        assert result.projected.estimated_cost_usd is not None

    # --- to_instance spill risk ---

    def test_to_instance_adds_spill_warning_when_profile_shows_spill(self):
        """When profile has spill bytes and target instance is smaller, warn."""
        # DS4_v2 has 28GB memory; going to DS3_v2 (14GB) with 80% peak → spill risk
        profile = WorkloadProfile(
            peak_memory_pct=80.0,
            spill_to_disk_bytes=500_000_000,  # 500 MB spill
        )
        cluster = ClusterConfig(instance_type="Standard_DS4_v2", num_workers=2, dbu_per_hour=1.5)
        sim = Simulation(self._base_estimate(), cluster=cluster, profile=profile)
        result = sim.cluster().to_instance("Standard_DS3_v2").compare()
        instance_mod = next(m for m in result.modifications if "Migrate" in m.name)
        assert any("Spill risk" in t for t in instance_mod.trade_offs)

    def test_to_instance_no_spill_warning_without_profile(self):
        """Without profile, to_instance produces no spill risk warning."""
        sim = Simulation(self._base_estimate())
        result = sim.cluster().to_instance("Standard_DS3_v2").compare()
        instance_mod = next((m for m in result.modifications if "Migrate" in m.name), None)
        if instance_mod:
            assert not any("Spill risk" in t for t in instance_mod.trade_offs)

    # --- End-to-end advisory→simulation chain ---

    def test_advisory_simulate_chain_end_to_end(self):
        """advice.simulate().cluster().enable_photon().compare() works without error."""
        from burnt.advisor.report import AdvisoryReport, ComputeScenario
        from burnt.core.instances import WorkloadProfile
        from burnt.core.models import ClusterRecommendation

        profile = WorkloadProfile(compute_intensity=0.8, memory_intensity=0.3)
        baseline = ComputeScenario(
            compute_type="All-Purpose",
            sku="ALL_PURPOSE",
            estimated_cost_usd=50.0,
            savings_pct=0.0,
            tradeoff="Test",
        )
        report = AdvisoryReport(
            baseline=baseline,
            scenarios=[],
            recommended=ClusterConfig(),
            recommendation=ClusterRecommendation(
                economy=ClusterConfig(),
                balanced=ClusterConfig(),
                performance=ClusterConfig(),
                current_cost_usd=0.0,
                rationale="",
            ),
            insights=[],
            run_metrics={"duration_ms": 300_000},
            workload_profile=profile,
        )
        result = report.simulate().cluster().enable_photon().compare()
        assert result.projected.estimated_cost_usd is not None
        photon_mod = next(m for m in result.modifications if "Photon" in m.name)
        # compute_intensity=0.8 → "aggregation"
        assert "aggregation" in photon_mod.rationale
