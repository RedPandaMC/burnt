"""Tests for the pricing utilities and provider backends."""

import pytest


class TestAzureDbuRates:
    def test_azure_dbu_rates_contains_expected_skus(self):
        from burnt.providers.azure_databricks.rates import DBU_RATES

        expected_skus = [
            "JOBS_COMPUTE",
            "ALL_PURPOSE",
            "SERVERLESS_JOBS",
            "SERVERLESS_NOTEBOOKS",
            "SQL_CLASSIC",
            "SQL_PRO",
            "SQL_SERVERLESS",
            "DLT_CORE",
            "DLT_PRO",
            "DLT_ADVANCED",
        ]
        for sku in expected_skus:
            assert sku in DBU_RATES

    def test_azure_dbu_rates_are_floats(self):
        from burnt.providers.azure_databricks.rates import DBU_RATES

        for rate in DBU_RATES.values():
            assert isinstance(rate, (float, int))


class TestComputeUnits:
    def test_compute_components_vcpu_hours(self):
        from burnt.providers.base import InstanceSpec
        from burnt.providers.compute_units import ComputeComponents

        spec = InstanceSpec(instance_type="test", vcpus=4, memory_gb=16.0)
        components = ComputeComponents.from_raw(
            compute_seconds=3600.0,
            instance_spec=spec,
            num_workers=2,
            shuffle_bytes=0,
        )
        assert components.vcpu_hours() == pytest.approx(8.0)
        assert components.memory_gb_hours() == pytest.approx(32.0)

    def test_compute_components_executor_hours(self):
        from burnt.providers.base import InstanceSpec
        from burnt.providers.compute_units import ComputeComponents

        spec = InstanceSpec(instance_type="test", vcpus=4, memory_gb=16.0)
        components = ComputeComponents.from_raw(
            compute_seconds=3600.0,
            instance_spec=spec,
            num_workers=2,
            shuffle_bytes=0,
        )
        assert components.executor_hours() == pytest.approx(2.0)


class TestOnPremSparkBackend:
    def test_onprem_estimate(self):
        from burnt.providers.onprem_spark import OnPremSparkBackend

        backend = OnPremSparkBackend(total_vcpus=8, total_memory_gb=32.0)
        result = backend.estimate(3600.0, num_workers=2, currency="USD")
        assert result.cost_in("USD") is not None
        assert result.cost_in("USD") > 0

    def test_onprem_estimate_with_custom_config(self):
        from burnt.providers.onprem_spark import OnPremConfig, OnPremSparkBackend

        cfg = OnPremConfig(cost_per_vcpu_hour=0.10, datacenter_overhead_pct=0.0)
        backend = OnPremSparkBackend(total_vcpus=4, total_memory_gb=16.0, config=cfg)
        result = backend.estimate(3600.0, num_workers=1)
        # 4 vCPUs * 1h * $0.10 + 16 GB * 1h * $0.006 = $0.4 + $0.096 = $0.496
        assert result.cost_in("USD") == pytest.approx(0.496, rel=0.01)


class TestAzureDatabricksBackend:
    def test_azure_estimate(self):
        from burnt.providers.azure_databricks import AzureDatabricksBackend

        backend = AzureDatabricksBackend()
        result = backend.estimate(
            3600.0,
            instance_type="Standard_DS3_v2",
            num_workers=2,
            sku="ALL_PURPOSE",
            currency="USD",
        )
        assert result.cost_in("USD") is not None
        assert result.cost_in("USD") > 0

    def test_azure_estimate_unknown_instance(self):
        from burnt.providers.azure_databricks import AzureDatabricksBackend

        backend = AzureDatabricksBackend()
        result = backend.estimate(
            3600.0,
            instance_type="CompletelyUnknownType_XL",
            num_workers=1,
        )
        assert result.cost_in("USD") is None
        assert "Unknown instance type" in result.warnings[0]

    def test_azure_estimate_photon(self):
        from burnt.providers.azure_databricks import AzureDatabricksBackend

        backend = AzureDatabricksBackend()
        normal = backend.estimate(
            3600.0, instance_type="Standard_DS3_v2", num_workers=1
        )
        photon = backend.estimate(
            3600.0, instance_type="Standard_DS3_v2", num_workers=1, photon_enabled=True
        )
        assert normal.estimated_dbu is not None
        assert photon.estimated_dbu is not None
        assert photon.estimated_dbu > normal.estimated_dbu

    def test_azure_estimate_with_currency_conversion(self):
        from burnt.providers.azure_databricks import AzureDatabricksBackend

        backend = AzureDatabricksBackend()
        result = backend.estimate(
            3600.0,
            instance_type="Standard_DS3_v2",
            num_workers=1,
            currency="EUR",
        )
        assert result.cost_in("USD") is not None
        assert result.cost_in("EUR") is not None
        assert result.cost_in("EUR") > 0

    def test_azure_is_available(self):
        from burnt.providers.azure_databricks import AzureDatabricksBackend

        assert AzureDatabricksBackend().is_available() is True


class TestExchangeRateProvider:
    def test_fixed_rate_provider(self):
        from decimal import Decimal

        from burnt.providers.exchange import FixedRateProvider

        provider = FixedRateProvider(Decimal("0.85"))
        rate = provider.get_rate(provider._rate, "USD", "EUR")
        assert rate == Decimal("0.85")

    def test_same_currency_rate(self):
        from decimal import Decimal

        from burnt.providers.exchange import FixedRateProvider

        provider = FixedRateProvider(Decimal("1.0"))
        rate = provider.get_rate(provider._rate, "USD", "USD")
        assert rate == Decimal("1")


class TestInstanceSpec:
    def test_instance_spec_total_vcpus(self):
        from burnt.providers.base import InstanceSpec

        spec = InstanceSpec(instance_type="test", vcpus=4, memory_gb=16.0)
        assert spec.total_vcpus(2) == 8
        assert spec.total_memory_gb(2) == 32.0
