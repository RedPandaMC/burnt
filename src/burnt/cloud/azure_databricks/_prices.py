"""Azure Retail Prices API client — no auth required, 24 h TTL cache."""

from __future__ import annotations

from decimal import Decimal

from burnt.cloud._pricing_api import PriceCache

_PRICES_URL = "https://prices.azure.com/api/retail/prices"
_API_VERSION = "2023-01-01-preview"

# Module-level cache: (instance_type_lower, region_lower) -> Decimal price
_vm_cache: PriceCache[Decimal] = PriceCache(ttl_seconds=86_400)


def get_vm_price_usd(instance_type: str, region: str) -> Decimal:
    """Return on-demand Linux VM hourly price (USD) from Azure Retail Prices API.

    Results are cached for 24 hours per (instance_type, region) pair.
    Raises PricingError if the instance type is not found in the region.
    """
    key = (instance_type.lower(), region.lower())
    cached = _vm_cache.get(key)
    if cached is not None:
        return cached

    import requests

    from burnt.core.exceptions import PricingError

    filt = (
        f"serviceName eq 'Virtual Machines'"
        f" and armSkuName eq '{instance_type}'"
        f" and armRegionName eq '{region}'"
        f" and priceType eq 'Consumption'"
    )
    try:
        resp = requests.get(
            _PRICES_URL,
            params={
                "api-version": _API_VERSION,
                "$filter": filt,
                "currencyCode": "USD",
            },
            timeout=10,
        )
        resp.raise_for_status()
    except Exception as exc:
        raise PricingError(f"Azure pricing API error for {instance_type!r}: {exc}") from exc

    items = resp.json().get("Items", [])
    for item in items:
        sku = item.get("skuName", "")
        if "Windows" in sku or "Spot" in sku or "Low Priority" in sku:
            continue
        price = Decimal(str(item["retailPrice"]))
        _vm_cache.set(key, price)
        return price

    raise PricingError(
        f"No on-demand Linux price found for {instance_type!r} in region {region!r}. "
        "Check that the instance type name matches Azure ARM naming."
    )
