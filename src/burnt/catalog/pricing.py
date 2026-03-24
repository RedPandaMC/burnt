"""Pricing information for DBU rates."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class Pricing(BaseModel):
    """Pricing for a SKU."""

    sku: str
    dbu_rate: float
    cloud: Literal["AZURE", "AWS", "GCP"] = "AZURE"
    region: str = "EAST_US"


DBU_PRICING: dict[str, Pricing] = {
    "ALL_PURPOSE": Pricing(sku="ALL_PURPOSE", dbu_rate=0.55, cloud="AZURE"),
    "JOBS_COMPUTE": Pricing(sku="JOBS_COMPUTE", dbu_rate=0.07, cloud="AZURE"),
    "SERVERLESS_JOBS": Pricing(sku="SERVERLESS_JOBS", dbu_rate=0.14, cloud="AZURE"),
    "SERVERLESS_NOTEBOOKS": Pricing(
        sku="SERVERLESS_NOTEBOOKS", dbu_rate=0.22, cloud="AZURE"
    ),
    "DLT_CORE": Pricing(sku="DLT_CORE", dbu_rate=0.07, cloud="AZURE"),
    "DLT_PRO": Pricing(sku="DLT_PRO", dbu_rate=0.14, cloud="AZURE"),
    "DLT_ADVANCED": Pricing(sku="DLT_ADVANCED", dbu_rate=0.22, cloud="AZURE"),
}


def get_pricing(
    sku: str,
    cloud: Literal["AZURE", "AWS", "GCP"] = "AZURE",
) -> Pricing:
    """Get pricing for a SKU.

    Args:
        sku: The SKU name (e.g., "ALL_PURPOSE", "DLT_PRO").
        cloud: Cloud provider.

    Returns:
        Pricing information.
    """
    key = f"{sku}_{cloud}" if cloud != "AZURE" else sku
    return DBU_PRICING.get(key, Pricing(sku=sku, dbu_rate=0.55, cloud=cloud))
