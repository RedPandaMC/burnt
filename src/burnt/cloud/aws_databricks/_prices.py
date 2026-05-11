"""AWS EC2 pricing client — public pricing JSON API, no auth, 24 h TTL cache."""

from __future__ import annotations

from decimal import Decimal

from burnt.cloud._pricing_api import PriceCache

# AWS public pricing index URL template — no credentials required
_PRICING_BASE = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current"

# Module-level cache: (instance_type_lower, region_lower) -> Decimal price
_ec2_cache: PriceCache[Decimal] = PriceCache(ttl_seconds=86_400)

# AWS region code → pricing region name mapping (used in the pricing JSON URL)
_REGION_INDEX: dict[str, str] = {
    "us-east-1": "us-east-1",
    "us-east-2": "us-east-2",
    "us-west-1": "us-west-1",
    "us-west-2": "us-west-2",
    "eu-west-1": "eu-west-1",
    "eu-west-2": "eu-west-2",
    "eu-west-3": "eu-west-3",
    "eu-central-1": "eu-central-1",
    "eu-north-1": "eu-north-1",
    "eu-south-1": "eu-south-1",
    "ap-northeast-1": "ap-northeast-1",
    "ap-northeast-2": "ap-northeast-2",
    "ap-northeast-3": "ap-northeast-3",
    "ap-southeast-1": "ap-southeast-1",
    "ap-southeast-2": "ap-southeast-2",
    "ap-south-1": "ap-south-1",
    "ca-central-1": "ca-central-1",
    "sa-east-1": "sa-east-1",
}


def get_ec2_price_usd(instance_type: str, region: str) -> Decimal:
    """Return the on-demand Linux EC2 hourly price (USD) from the AWS Pricing API.

    Uses the public bulk pricing JSON index — no API key required.
    Results are cached for 24 hours per (instance_type, region) pair.
    Raises PricingError if pricing data is unavailable.
    """
    key = (instance_type.lower(), region.lower())
    cached = _ec2_cache.get(key)
    if cached is not None:
        return cached

    import requests

    from burnt.core.exceptions import PricingError

    pricing_region = _REGION_INDEX.get(region.lower(), region.lower())
    url = f"{_PRICING_BASE}/{pricing_region}/index.json"

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        raise PricingError(
            f"AWS pricing API error for {instance_type!r} in {region!r}: {exc}"
        ) from exc

    products = data.get("products", {})
    terms = data.get("terms", {}).get("OnDemand", {})

    for sku, product in products.items():
        attrs = product.get("attributes", {})
        if (
            attrs.get("instanceType") == instance_type
            and attrs.get("operatingSystem") == "Linux"
            and attrs.get("tenancy") == "Shared"
            and attrs.get("preInstalledSw") == "NA"
            and attrs.get("capacitystatus") == "Used"
        ):
            sku_terms = terms.get(sku, {})
            for offer in sku_terms.values():
                for dim in offer.get("priceDimensions", {}).values():
                    price_str = dim.get("pricePerUnit", {}).get("USD", "0")
                    price = Decimal(price_str)
                    if price > 0:
                        _ec2_cache.set(key, price)
                        return price

    raise PricingError(
        f"No on-demand Linux price found for {instance_type!r} in region {region!r}. "
        "Verify instance type name matches AWS naming (e.g. 'm5.xlarge')."
    )
