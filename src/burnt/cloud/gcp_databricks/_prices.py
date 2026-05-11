"""GCP Compute Engine pricing client — Cloud Billing Catalog API, 24 h TTL cache."""

from __future__ import annotations

from decimal import Decimal

from burnt.cloud._pricing_api import PriceCache

# GCP Cloud Billing Catalog API — public endpoint; service ID for Compute Engine
_BILLING_API = "https://cloudbilling.googleapis.com/v1/services"
_COMPUTE_SERVICE_ID = "6F81-5844-456A"

# Module-level cache: (machine_type_lower, region_lower) -> Decimal price
_gce_cache: PriceCache[Decimal] = PriceCache(ttl_seconds=86_400)

# GCP machine type prefix → SKU description fragment used to match billing SKUs
_MACHINE_FAMILY_SKU: dict[str, str] = {
    "n1": "N1 Predefined Instance",
    "n2": "N2 Instance",
    "n2d": "N2D AMD Instance",
    "e2": "E2 Instance",
    "c2": "C2 Instance",
    "m1": "Memory-optimized Instance",
    "m2": "Memory-optimized Instance",
}


def get_gce_price_usd(machine_type: str, region: str) -> Decimal:
    """Return the on-demand GCE hourly price (USD) from the Cloud Billing Catalog API.

    The GCP Billing Catalog API is public and requires no authentication for listing
    SKUs and their prices. Results are cached for 24 hours.

    Note: The Catalog API returns prices per resource unit (vCPU-hour, GB-hour).
    This function returns a composite estimate based on the machine type's vCPU count
    and memory (resolved from GCP's published machine type specs), summing both costs.

    Raises PricingError if the machine type is not recognised.
    """
    key = (machine_type.lower(), region.lower())
    cached = _gce_cache.get(key)
    if cached is not None:
        return cached

    import requests

    from burnt.core.exceptions import PricingError

    # Resolve vCPU and memory for the machine type from GCP metadata
    vcpus, memory_gb = _resolve_machine_specs(machine_type)

    family = machine_type.split("-")[0].lower()
    sku_fragment = _MACHINE_FAMILY_SKU.get(family)
    if not sku_fragment:
        raise PricingError(
            f"Unknown GCP machine family for {machine_type!r}. "
            "Supported families: " + ", ".join(_MACHINE_FAMILY_SKU)
        )

    url = f"{_BILLING_API}/{_COMPUTE_SERVICE_ID}/skus"
    try:
        resp = requests.get(
            url,
            params={"currencyCode": "USD", "pageSize": 2000},
            timeout=15,
        )
        resp.raise_for_status()
        skus = resp.json().get("skus", [])
    except Exception as exc:
        raise PricingError(
            f"GCP billing catalog API error for {machine_type!r}: {exc}"
        ) from exc

    cpu_price = Decimal("0")
    ram_price = Decimal("0")

    for sku in skus:
        desc: str = sku.get("description", "")
        regions: list[str] = sku.get("serviceRegions", [])
        if region.lower() not in [r.lower() for r in regions]:
            continue
        if sku_fragment.lower() not in desc.lower():
            continue

        pricing_info = sku.get("pricingInfo", [{}])
        expr = pricing_info[0].get("pricingExpression", {}) if pricing_info else {}
        tiers = expr.get("tieredRates", [])
        if not tiers:
            continue
        unit_price = tiers[-1].get("unitPrice", {})
        nanos = int(unit_price.get("nanos", 0))
        units = int(unit_price.get("units", 0))
        rate = Decimal(units) + Decimal(nanos) / Decimal("1e9")

        if "Core" in desc or "vCPU" in desc or "CPU" in desc:
            cpu_price = rate * vcpus
        elif "Ram" in desc or "RAM" in desc or "Memory" in desc:
            ram_price = rate * memory_gb

    total = cpu_price + ram_price
    if total <= 0:
        raise PricingError(
            f"Could not determine price for {machine_type!r} in region {region!r}. "
            "Check machine type and region spelling."
        )

    _gce_cache.set(key, total)
    return total


def _resolve_machine_specs(machine_type: str) -> tuple[Decimal, Decimal]:
    """Return (vcpus, memory_gb) for a GCP machine type.

    Uses a static lookup for common predefined types and parses custom types
    (e.g. custom-8-32768 → 8 vCPUs, 32 GB).
    """
    _SPECS: dict[str, tuple[int, float]] = {
        # N1
        "n1-standard-1": (1, 3.75),
        "n1-standard-2": (2, 7.5),
        "n1-standard-4": (4, 15.0),
        "n1-standard-8": (8, 30.0),
        "n1-standard-16": (16, 60.0),
        "n1-standard-32": (32, 120.0),
        "n1-standard-64": (64, 240.0),
        "n1-standard-96": (96, 360.0),
        # N2
        "n2-standard-2": (2, 8.0),
        "n2-standard-4": (4, 16.0),
        "n2-standard-8": (8, 32.0),
        "n2-standard-16": (16, 64.0),
        "n2-standard-32": (32, 128.0),
        "n2-standard-64": (64, 256.0),
        # E2
        "e2-standard-2": (2, 8.0),
        "e2-standard-4": (4, 16.0),
        "e2-standard-8": (8, 32.0),
        "e2-standard-16": (16, 64.0),
        "e2-standard-32": (32, 128.0),
    }
    normalized = machine_type.lower()
    if normalized in _SPECS:
        vcpus, mem = _SPECS[normalized]
        return Decimal(vcpus), Decimal(str(mem))

    # Parse custom machine types: custom-<vcpu>-<mem_mb>
    parts = normalized.split("-")
    if len(parts) >= 3 and "custom" in parts:
        try:
            idx = parts.index("custom")
            vcpus = Decimal(parts[idx + 1])
            mem_gb = Decimal(parts[idx + 2]) / Decimal("1024")
            return vcpus, mem_gb
        except (IndexError, ValueError):
            pass

    # Default conservative estimate: 4 vCPU, 16 GB
    return Decimal("4"), Decimal("16")
