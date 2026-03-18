"""Unit tests for ClusterConfig.from_databricks_json(), ClusterProfile, and related features."""

from __future__ import annotations

import logging

import pytest

import burnt
from burnt.core.models import ClusterConfig, ClusterProfile

# ---------------------------------------------------------------------------
# ClusterConfig.from_databricks_json
# ---------------------------------------------------------------------------


class TestClusterConfigFactory:
    @pytest.mark.unit
    def test_from_full_jobs_payload(self):
        payload = {
            "new_cluster": {
                "node_type_id": "Standard_DS4_v2",
                "num_workers": 4,
                "spark_version": "15.4.x-scala2.12",
            }
        }
        c = ClusterConfig.from_databricks_json(payload)
        assert c.instance_type == "Standard_DS4_v2"
        assert c.num_workers == 4
        assert c.dbu_per_hour == 1.5
        assert c.photon_enabled is False

    @pytest.mark.unit
    def test_from_bare_dict(self):
        payload = {"node_type_id": "Standard_DS3_v2", "num_workers": 2}
        c = ClusterConfig.from_databricks_json(payload)
        assert c.instance_type == "Standard_DS3_v2"
        assert c.num_workers == 2
        assert c.dbu_per_hour == 0.75

    @pytest.mark.unit
    def test_photon_from_spark_version(self):
        payload = {"node_type_id": "Standard_DS3_v2", "spark_version": "15.4.x-photon-scala2.12"}
        c = ClusterConfig.from_databricks_json(payload)
        assert c.photon_enabled is True

    @pytest.mark.unit
    def test_photon_case_insensitive(self):
        payload = {"node_type_id": "Standard_DS3_v2", "spark_version": "15.4.x-PHOTON-scala2.12"}
        c = ClusterConfig.from_databricks_json(payload)
        assert c.photon_enabled is True

    @pytest.mark.unit
    def test_photon_from_runtime_engine(self):
        payload = {
            "node_type_id": "Standard_DS3_v2",
            "spark_version": "15.4.x-scala2.12",
            "runtime_engine": "PHOTON",
        }
        c = ClusterConfig.from_databricks_json(payload)
        assert c.photon_enabled is True

    @pytest.mark.unit
    def test_no_photon_standard_engine(self):
        payload = {
            "node_type_id": "Standard_DS3_v2",
            "spark_version": "15.4.x-scala2.12",
            "runtime_engine": "STANDARD",
        }
        c = ClusterConfig.from_databricks_json(payload)
        assert c.photon_enabled is False

    @pytest.mark.unit
    def test_spot_mapping_on_demand(self):
        payload = {
            "node_type_id": "Standard_DS3_v2",
            "azure_attributes": {"availability": "ON_DEMAND"},
        }
        c = ClusterConfig.from_databricks_json(payload)
        assert c.spot_policy == "ON_DEMAND"

    @pytest.mark.unit
    def test_spot_mapping_spot_with_fallback(self):
        payload = {
            "node_type_id": "Standard_DS3_v2",
            "azure_attributes": {"availability": "SPOT_WITH_ON_DEMAND_FALLBACK"},
        }
        c = ClusterConfig.from_databricks_json(payload)
        assert c.spot_policy == "SPOT_WITH_ON_DEMAND_FALLBACK"

    @pytest.mark.unit
    def test_spot_mapping_spot(self):
        payload = {
            "node_type_id": "Standard_DS3_v2",
            "azure_attributes": {"availability": "SPOT"},
        }
        c = ClusterConfig.from_databricks_json(payload)
        assert c.spot_policy == "SPOT"

    @pytest.mark.unit
    def test_spot_defaults_to_on_demand_when_missing(self):
        payload = {"node_type_id": "Standard_DS3_v2"}
        c = ClusterConfig.from_databricks_json(payload)
        assert c.spot_policy == "ON_DEMAND"

    @pytest.mark.unit
    def test_autoscale_fields(self):
        payload = {
            "node_type_id": "Standard_DS3_v2",
            "autoscale": {"min_workers": 2, "max_workers": 8},
        }
        c = ClusterConfig.from_databricks_json(payload)
        assert c.autoscale_min_workers == 2
        assert c.autoscale_max_workers == 8
        assert c.num_workers == 8  # falls back to max_workers

    @pytest.mark.unit
    def test_num_workers_explicit_overrides_autoscale(self):
        payload = {
            "node_type_id": "Standard_DS3_v2",
            "num_workers": 6,
            "autoscale": {"min_workers": 2, "max_workers": 8},
        }
        c = ClusterConfig.from_databricks_json(payload)
        assert c.num_workers == 6

    @pytest.mark.unit
    def test_unknown_node_type_logs_warning_and_uses_default(self, caplog):
        with caplog.at_level(logging.WARNING):
            c = ClusterConfig.from_databricks_json({"node_type_id": "Unknown_XYZ_v99"})
        assert c.dbu_per_hour == 0.75
        assert "Unknown_XYZ_v99" in caplog.text
        assert "0.75" in caplog.text

    @pytest.mark.unit
    def test_missing_node_type_uses_default(self):
        c = ClusterConfig.from_databricks_json({})
        assert c.instance_type == "Standard_DS3_v2"
        assert c.dbu_per_hour == 0.75

    @pytest.mark.unit
    def test_round_trip(self):
        original = ClusterConfig(
            instance_type="Standard_DS4_v2",
            num_workers=4,
            dbu_per_hour=1.5,
            photon_enabled=False,
            spot_policy="SPOT_WITH_ON_DEMAND_FALLBACK",
        )
        roundtripped = ClusterConfig.from_databricks_json(original.to_json())
        assert roundtripped.instance_type == original.instance_type
        assert roundtripped.num_workers == original.num_workers
        assert roundtripped.dbu_per_hour == original.dbu_per_hour
        assert roundtripped.photon_enabled == original.photon_enabled
        assert roundtripped.spot_policy == original.spot_policy

    @pytest.mark.unit
    def test_integration_check_ds4_v2(self):
        """Spec integration check: DS4_v2 with 4 workers → dbu_per_hour == 1.5."""
        c = ClusterConfig.from_databricks_json({"node_type_id": "Standard_DS4_v2", "num_workers": 4})
        assert c.dbu_per_hour == 1.5


# ---------------------------------------------------------------------------
# ClusterProfile
# ---------------------------------------------------------------------------


class TestClusterProfile:
    @pytest.mark.unit
    def test_direct_construction(self):
        config = ClusterConfig(instance_type="Standard_DS3_v2", num_workers=2, dbu_per_hour=0.75)
        profile = ClusterProfile(config=config, spark_version="15.4.x-scala2.12")
        assert profile.config is config
        assert profile.spark_version == "15.4.x-scala2.12"
        assert profile.cloud_provider == "AZURE"

    @pytest.mark.unit
    def test_default_fields(self):
        config = ClusterConfig()
        profile = ClusterProfile(config=config)
        assert profile.driver_node_type is None
        assert profile.spark_version is None
        assert profile.custom_spark_conf == {}
        assert profile.cluster_tags == {}
        assert profile.instance_pool_id is None
        assert profile.instance_pool_max_capacity is None

    @pytest.mark.unit
    def test_from_databricks_json_basic(self):
        payload = {
            "node_type_id": "Standard_DS4_v2",
            "spark_version": "15.4.x-photon-scala2.12",
            "num_workers": 4,
        }
        p = ClusterProfile.from_databricks_json(payload)
        assert p.spark_version == "15.4.x-photon-scala2.12"
        assert p.config.photon_enabled is True
        assert p.config.instance_type == "Standard_DS4_v2"
        assert p.config.dbu_per_hour == 1.5

    @pytest.mark.unit
    def test_from_databricks_json_full_payload(self):
        payload = {
            "new_cluster": {
                "node_type_id": "Standard_DS4_v2",
                "num_workers": 4,
                "spark_version": "15.4.x-scala2.12",
                "spark_conf": {"spark.sql.shuffle.partitions": "400"},
                "custom_tags": {"team": "data-engineering"},
                "driver_node_type_id": "Standard_DS5_v2",
                "instance_pool_id": "pool-abc123",
                "instance_pool_max_capacity": 10,
            }
        }
        p = ClusterProfile.from_databricks_json(payload)
        assert p.spark_version == "15.4.x-scala2.12"
        assert p.custom_spark_conf == {"spark.sql.shuffle.partitions": "400"}
        assert p.cluster_tags == {"team": "data-engineering"}
        assert p.driver_node_type == "Standard_DS5_v2"
        assert p.instance_pool_id == "pool-abc123"
        assert p.instance_pool_max_capacity == 10

    @pytest.mark.unit
    def test_integration_check(self):
        """Spec integration check: photon detected from spark_version."""
        p = ClusterProfile.from_databricks_json(
            {
                "node_type_id": "Standard_DS4_v2",
                "spark_version": "15.4.x-photon-scala2.12",
                "num_workers": 4,
            }
        )
        assert p.spark_version == "15.4.x-photon-scala2.12"
        assert p.config.photon_enabled is True


# ---------------------------------------------------------------------------
# Simulation accepts ClusterProfile
# ---------------------------------------------------------------------------


class TestSimulationWithClusterProfile:
    @pytest.mark.unit
    def test_simulation_accepts_cluster_profile(self):
        from burnt.estimators.simulation import Simulation

        config = ClusterConfig(instance_type="Standard_DS3_v2", num_workers=2, dbu_per_hour=0.75)
        profile = ClusterProfile(config=config, spark_version="15.4.x-scala2.12")
        estimate = burnt.CostEstimate(estimated_dbu=100.0, estimated_cost_usd=55.0, confidence="medium")

        sim = Simulation(estimate, cluster=profile)
        assert sim._cluster == config
        assert sim._profile is profile

    @pytest.mark.unit
    def test_simulation_uses_config_for_cost_calculations(self):
        from burnt.estimators.simulation import Simulation

        config = ClusterConfig(instance_type="Standard_DS3_v2", num_workers=2, dbu_per_hour=0.75)
        profile = ClusterProfile(config=config)
        estimate = burnt.CostEstimate(estimated_dbu=100.0, estimated_cost_usd=55.0, confidence="medium")

        sim = Simulation(estimate, cluster=profile)
        # cluster config is correctly extracted from profile
        assert sim._cluster.instance_type == "Standard_DS3_v2"
        assert sim._cluster.dbu_per_hour == 0.75

    @pytest.mark.unit
    def test_simulation_plain_cluster_config_still_works(self):
        from burnt.estimators.simulation import Simulation

        config = ClusterConfig(instance_type="Standard_DS3_v2", num_workers=2, dbu_per_hour=0.75)
        estimate = burnt.CostEstimate(estimated_dbu=100.0, estimated_cost_usd=55.0, confidence="medium")

        sim = Simulation(estimate, cluster=config)
        assert sim._cluster is config
        assert sim._profile is None


# ---------------------------------------------------------------------------
# Default Currency
# ---------------------------------------------------------------------------


class TestDefaultCurrency:
    @pytest.mark.unit
    def test_default_is_usd(self):
        burnt.set_default_currency("USD")
        assert burnt.get_default_currency() == "USD"

    @pytest.mark.unit
    def test_set_valid_currency(self):
        burnt.set_default_currency("EUR")
        assert burnt.get_default_currency() == "EUR"
        burnt.set_default_currency("USD")  # restore

    @pytest.mark.unit
    def test_set_currency_case_insensitive(self):
        burnt.set_default_currency("eur")
        assert burnt.get_default_currency() == "EUR"
        burnt.set_default_currency("USD")  # restore

    @pytest.mark.unit
    def test_set_unsupported_currency_raises(self):
        with pytest.raises(ValueError, match="Unsupported currency"):
            burnt.set_default_currency("XYZ")

    @pytest.mark.unit
    def test_raise_if_exceeds_uses_default_currency(self):
        burnt.set_default_currency("USD")
        estimate = burnt.CostEstimate(
            estimated_dbu=100.0,
            estimated_cost_usd=10.0,
            confidence="high",
        )
        # Should not raise — 10.0 USD < 50.0 USD
        result = estimate.raise_if_exceeds(50.0)
        assert result is estimate
        burnt.set_default_currency("USD")  # restore

    @pytest.mark.unit
    def test_raise_if_exceeds_override_currency(self):
        burnt.set_default_currency("EUR")
        estimate = burnt.CostEstimate(
            estimated_dbu=100.0,
            estimated_cost_usd=10.0,
            confidence="high",
        )
        # Even though default is EUR, explicit USD override should work fine
        result = estimate.raise_if_exceeds(50.0, currency="USD")
        assert result is estimate
        burnt.set_default_currency("USD")  # restore
