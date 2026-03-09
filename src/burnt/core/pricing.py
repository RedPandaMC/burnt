"""Pricing utilities for Azure Databricks."""

from decimal import Decimal

from .exceptions import PricingError

AZURE_DBU_RATES = {
    "JOBS_COMPUTE": Decimal("0.30"),
    "ALL_PURPOSE": Decimal("0.55"),
    "SERVERLESS_JOBS": Decimal("0.45"),
    "SERVERLESS_NOTEBOOKS": Decimal("0.95"),
    "SQL_CLASSIC": Decimal("0.22"),
    "SQL_PRO": Decimal("0.55"),
    "SQL_SERVERLESS": Decimal("0.70"),
    "DLT_CORE": Decimal("0.30"),
    "DLT_PRO": Decimal("0.38"),
    "DLT_ADVANCED": Decimal("0.54"),
}


AZURE_INSTANCE_DBU = {
    "Standard_DS3_v2": 0.75,
    "Standard_DS4_v2": 1.50,
    "Standard_D8s_v3": 2.00,
    "Standard_D16s_v3": 4.00,
    "Standard_D32s_v3": 8.00,
    "Standard_D64s_v3": 12.00,
}


PHOTON_MULTIPLIER_AZURE = Decimal("2.5")


def get_dbu_rate(sku_name: str) -> Decimal:
    """Get DBU rate for a SKU."""
    rate = AZURE_DBU_RATES.get(sku_name.upper())
    if rate is None:
        raise PricingError(f"Unknown SKU: {sku_name}")
    return rate


def compute_cost_usd(dbu: float, sku_name: str) -> Decimal:
    """Compute cost in USD from DBU and SKU."""
    rate = get_dbu_rate(sku_name)
    return Decimal(str(dbu)) * rate


def apply_photon(dbu: Decimal, enabled: bool) -> Decimal:
    """Apply Photon multiplier to DBU."""
    if not enabled:
        return dbu
    return dbu * PHOTON_MULTIPLIER_AZURE


def usd_to_eur(usd_amount: Decimal, rate: Decimal = Decimal("0.92")) -> Decimal:
    """Convert USD to EUR."""
    return usd_amount * rate
