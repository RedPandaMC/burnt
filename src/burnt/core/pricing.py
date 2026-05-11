"""Pricing protocol, currency constants, and Azure Databricks pricing utilities."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Protocol

from .exceptions import PricingError

if TYPE_CHECKING:
    from burnt.core.models import CostEstimate
    from burnt.graph.model import CostGraph


class PricingBackend(Protocol):
    """Protocol every cloud pricing backend must satisfy.

    Core ships no implementation — without a pricing extra, cost_estimate.cost_in()
    returns None and only compute-seconds are reported.
    """

    name: str

    def map(self, graph: CostGraph) -> CostEstimate:
        """Map a CostGraph (compute-seconds) to a CostEstimate (dollars)."""
        ...


# ISO 4217 billing currency for each major cloud region identifier.
CLOUD_REGION_CURRENCIES: dict[str, str] = {
    # Azure
    "eastus": "USD",
    "eastus2": "USD",
    "westus": "USD",
    "westus2": "USD",
    "westus3": "USD",
    "centralus": "USD",
    "northcentralus": "USD",
    "southcentralus": "USD",
    "northeurope": "EUR",
    "westeurope": "EUR",
    "uksouth": "GBP",
    "ukwest": "GBP",
    "japaneast": "JPY",
    "japanwest": "JPY",
    "canadacentral": "CAD",
    "canadaeast": "CAD",
    "australiaeast": "AUD",
    "australiasoutheast": "AUD",
    "australiacentral": "AUD",
    "switzerlandnorth": "CHF",
    "switzerlandwest": "CHF",
    "brazilsouth": "BRL",
    "brazilsoutheast": "BRL",
    "koreacentral": "KRW",
    "koreasouth": "KRW",
    "southeastasia": "SGD",
    "eastasia": "HKD",
    "centralindia": "INR",
    "southindia": "INR",
    "westindia": "INR",
    "swedencentral": "SEK",
    "norwayeast": "NOK",
    "norwaywest": "NOK",
    "francesouth": "EUR",
    "francecentral": "EUR",
    "germanywestcentral": "EUR",
    "polandcentral": "EUR",
    "italynorth": "EUR",
    "spaincentral": "EUR",
    "mexicocentral": "MXN",
    "newzealandnorth": "NZD",
    "southafricanorth": "ZAR",
    "uaenorth": "AED",
    "israelcentral": "ILS",
    # AWS
    "us-east-1": "USD",
    "us-east-2": "USD",
    "us-west-1": "USD",
    "us-west-2": "USD",
    "eu-west-1": "EUR",
    "eu-west-2": "GBP",
    "eu-west-3": "EUR",
    "eu-central-1": "EUR",
    "eu-central-2": "CHF",
    "eu-north-1": "SEK",
    "eu-south-1": "EUR",
    "eu-south-2": "EUR",
    "ap-northeast-1": "JPY",
    "ap-northeast-2": "KRW",
    "ap-northeast-3": "JPY",
    "ap-southeast-1": "SGD",
    "ap-southeast-2": "AUD",
    "ap-southeast-3": "IDR",
    "ap-south-1": "INR",
    "ap-south-2": "INR",
    "ap-east-1": "HKD",
    "ca-central-1": "CAD",
    "ca-west-1": "CAD",
    "sa-east-1": "BRL",
    "me-south-1": "USD",
    "me-central-1": "USD",
    "af-south-1": "ZAR",
    "il-central-1": "ILS",
    # GCP
    "us-central1": "USD",
    "us-east1": "USD",
    "us-east4": "USD",
    "us-east5": "USD",
    "us-west1": "USD",
    "us-west2": "USD",
    "us-west3": "USD",
    "us-west4": "USD",
    "us-south1": "USD",
    "northamerica-northeast1": "CAD",
    "northamerica-northeast2": "CAD",
    "southamerica-east1": "BRL",
    "southamerica-west1": "CLP",
    "europe-west1": "EUR",
    "europe-west2": "GBP",
    "europe-west3": "EUR",
    "europe-west4": "EUR",
    "europe-west6": "CHF",
    "europe-west8": "EUR",
    "europe-west9": "EUR",
    "europe-west10": "EUR",
    "europe-west12": "EUR",
    "europe-north1": "EUR",
    "europe-central2": "PLN",
    "europe-southwest1": "EUR",
    "asia-east1": "TWD",
    "asia-east2": "HKD",
    "asia-northeast1": "JPY",
    "asia-northeast2": "JPY",
    "asia-northeast3": "KRW",
    "asia-southeast1": "SGD",
    "asia-southeast2": "IDR",
    "asia-south1": "INR",
    "asia-south2": "INR",
    "australia-southeast1": "AUD",
    "australia-southeast2": "AUD",
    "me-west1": "USD",
    "me-central1": "USD",
    "me-central2": "SAR",
    "africa-south1": "ZAR",
}

# All ISO 4217 currency codes that burnt pricing backends may report.
SUPPORTED_CURRENCIES: frozenset[str] = frozenset(
    {
        "AED",
        "AUD",
        "BRL",
        "CAD",
        "CHF",
        "CLP",
        "CNY",
        "DKK",
        "EUR",
        "GBP",
        "HKD",
        "IDR",
        "ILS",
        "INR",
        "JPY",
        "KRW",
        "MXN",
        "NOK",
        "NZD",
        "PLN",
        "SAR",
        "SEK",
        "SGD",
        "TWD",
        "USD",
        "ZAR",
    }
)


# ---------------------------------------------------------------------------
# Azure Databricks pricing utilities (used until azure-databricks extra ships)
# ---------------------------------------------------------------------------

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
