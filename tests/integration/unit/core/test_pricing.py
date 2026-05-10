from decimal import Decimal

import pytest

from burnt.core.exceptions import PricingError
from burnt.core.pricing import (
    AZURE_DBU_RATES,
    AZURE_INSTANCE_DBU,
    apply_photon,
    compute_cost_usd,
    get_dbu_rate,
    usd_to_eur,
)


class TestAzureDbuRates:
    def test_azure_dbu_rates_contains_expected_skus(self):
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
            assert sku in AZURE_DBU_RATES

    def test_azure_dbu_rates_are_decimals(self):
        for rate in AZURE_DBU_RATES.values():
            assert isinstance(rate, Decimal)


class TestAzureInstanceDbu:
    def test_azure_instance_dbu_contains_expected_types(self):
        expected_types = [
            "Standard_DS3_v2",
            "Standard_DS4_v2",
            "Standard_D8s_v3",
            "Standard_D16s_v3",
            "Standard_D32s_v3",
            "Standard_D64s_v3",
        ]
        for inst_type in expected_types:
            assert inst_type in AZURE_INSTANCE_DBU

    def test_azure_instance_dbu_values(self):
        assert AZURE_INSTANCE_DBU["Standard_DS3_v2"] == 0.75
        assert AZURE_INSTANCE_DBU["Standard_DS4_v2"] == 1.50
        assert AZURE_INSTANCE_DBU["Standard_D8s_v3"] == 2.00


class TestGetDbuRate:
    def test_get_dbu_rate_valid_sku(self):
        rate = get_dbu_rate("ALL_PURPOSE")
        assert rate == Decimal("0.55")

    def test_get_dbu_rate_case_insensitive(self):
        rate = get_dbu_rate("all_purpose")
        assert rate == Decimal("0.55")

        rate = get_dbu_rate("jobs_compute")
        assert rate == Decimal("0.30")

    def test_get_dbu_rate_invalid_sku(self):
        with pytest.raises(PricingError) as exc_info:
            get_dbu_rate("INVALID_SKU")
        assert "Unknown SKU" in str(exc_info.value)


class TestComputeCostUsd:
    def test_compute_cost_usd(self):
        cost = compute_cost_usd(100, "ALL_PURPOSE")
        assert cost == Decimal("55.00")

    def test_compute_cost_usd_with_different_sku(self):
        cost = compute_cost_usd(100, "JOBS_COMPUTE")
        assert cost == Decimal("30.00")

    def test_compute_cost_usd_invalid_sku(self):
        with pytest.raises(PricingError):
            compute_cost_usd(100, "INVALID")


class TestApplyPhoton:
    def test_apply_photon_disabled(self):
        result = apply_photon(Decimal("100"), False)
        assert result == Decimal("100")

    def test_apply_photon_enabled(self):
        result = apply_photon(Decimal("100"), True)
        assert result == Decimal("250")


class TestUsdToEur:
    def test_usd_to_eur_default_rate(self):
        result = usd_to_eur(Decimal("100"))
        assert result == Decimal("92.00")

    def test_usd_to_eur_custom_rate(self):
        result = usd_to_eur(Decimal("100"), Decimal("0.85"))
        assert result == Decimal("85.00")

    def test_usd_to_eur_zero(self):
        result = usd_to_eur(Decimal("0"))
        assert result == Decimal("0")
