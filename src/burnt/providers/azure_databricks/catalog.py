"""Azure Databricks VM instance catalog — fallback data and loading."""

from __future__ import annotations

import logging
from typing import Any

from ..base import InstanceSpec
from ..cache import PricingCache
from .rates import infer_dbu_rate

logger = logging.getLogger(__name__)

FALLBACK_CATALOG: dict[str, InstanceSpec] = {
    "Standard_DS3_v2": InstanceSpec(
        instance_type="Standard_DS3_v2",
        vcpus=4,
        memory_gb=14.0,
        local_storage_gb=28.0,
        dbu_rate=0.75,
        vm_cost_per_hour=0.293,
        category="general",
    ),
    "Standard_DS4_v2": InstanceSpec(
        instance_type="Standard_DS4_v2",
        vcpus=8,
        memory_gb=28.0,
        local_storage_gb=56.0,
        dbu_rate=1.50,
        vm_cost_per_hour=0.585,
        category="general",
    ),
    "Standard_D8s_v3": InstanceSpec(
        instance_type="Standard_D8s_v3",
        vcpus=8,
        memory_gb=32.0,
        local_storage_gb=64.0,
        dbu_rate=2.00,
        vm_cost_per_hour=0.384,
        category="general",
    ),
    "Standard_D16s_v3": InstanceSpec(
        instance_type="Standard_D16s_v3",
        vcpus=16,
        memory_gb=64.0,
        local_storage_gb=128.0,
        dbu_rate=4.00,
        vm_cost_per_hour=0.768,
        category="general",
    ),
    "Standard_D32s_v3": InstanceSpec(
        instance_type="Standard_D32s_v3",
        vcpus=32,
        memory_gb=128.0,
        local_storage_gb=256.0,
        dbu_rate=8.00,
        vm_cost_per_hour=1.536,
        category="general",
    ),
    "Standard_D64s_v3": InstanceSpec(
        instance_type="Standard_D64s_v3",
        vcpus=64,
        memory_gb=256.0,
        local_storage_gb=512.0,
        dbu_rate=12.00,
        vm_cost_per_hour=3.072,
        category="general",
    ),
    "Standard_E8s_v3": InstanceSpec(
        instance_type="Standard_E8s_v3",
        vcpus=8,
        memory_gb=64.0,
        local_storage_gb=64.0,
        dbu_rate=1.50,
        vm_cost_per_hour=0.504,
        category="memory",
    ),
    "Standard_E16s_v3": InstanceSpec(
        instance_type="Standard_E16s_v3",
        vcpus=16,
        memory_gb=128.0,
        local_storage_gb=128.0,
        dbu_rate=3.00,
        vm_cost_per_hour=1.008,
        category="memory",
    ),
    "Standard_F8s_v2": InstanceSpec(
        instance_type="Standard_F8s_v2",
        vcpus=8,
        memory_gb=16.0,
        local_storage_gb=32.0,
        dbu_rate=1.50,
        vm_cost_per_hour=0.338,
        category="compute",
    ),
}

_CATALOG: dict[str, InstanceSpec] = {}
_CACHE: PricingCache | None = None


def _cache() -> PricingCache:
    global _CACHE
    if _CACHE is None:
        _CACHE = PricingCache(ttl_seconds=86400.0)
    return _CACHE


def load_catalog(force_refresh: bool = False) -> dict[str, InstanceSpec]:
    global _CATALOG

    if _CATALOG and not force_refresh:
        return _CATALOG

    cache = _cache()
    cached = cache.get_disk("azure_vm_catalog")
    if cached and not force_refresh:
        _CATALOG = {k: InstanceSpec(**v) for k, v in cached.items()}
        return _CATALOG

    try:
        items = _fetch_retail_prices()
        catalog: dict[str, InstanceSpec] = {}
        for item in items:
            spec = _parse_retail_item(item)
            if spec is None:
                continue
            if spec.instance_type not in catalog:
                catalog[spec.instance_type] = spec

        if not catalog:
            raise ValueError("Azure API returned 0 valid instance types")

        _CATALOG = catalog
        try:
            cache.set_disk(
                "azure_vm_catalog",
                {k: _spec_to_dict(v) for k, v in catalog.items()},
            )
        except Exception as cache_exc:
            logger.warning("Failed to cache Azure catalog: %s", cache_exc)
        logger.info("Azure VM catalog refreshed: %d instance types", len(catalog))
        return _CATALOG
    except Exception as e:
        logger.warning(
            "Failed to fetch Azure pricing API, using fallback catalog: %s", e
        )
        _CATALOG = FALLBACK_CATALOG
    return _CATALOG


def _spec_to_dict(spec: InstanceSpec) -> dict[str, Any]:
    return {
        "instance_type": spec.instance_type,
        "vcpus": spec.vcpus,
        "memory_gb": spec.memory_gb,
        "local_storage_gb": spec.local_storage_gb,
        "vm_cost_per_hour": spec.vm_cost_per_hour,
        "category": spec.category,
        "dbu_rate": spec.dbu_rate,
        "photon_dbu_rate": spec.photon_dbu_rate,
    }


def _fetch_retail_prices(
    location: str = "East US",
    service_family: str = "Compute",
    timeout: float = 10.0,
) -> list[dict[str, Any]]:
    import requests

    RETAIL_PRICES_API = "https://prices.azure.com/api/retail/prices"
    items: list[dict[str, Any]] = []
    skip = 0
    page_size = 1000

    while True:
        params = {
            "$filter": (
                f"serviceFamily eq '{service_family}' and location eq '{location}'"
            ),
            "$top": page_size,
            "$skip": skip,
        }
        resp = requests.get(RETAIL_PRICES_API, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("Items", [])
        items.extend(batch)
        next_link = data.get("NextPageLink")
        if not next_link or len(batch) == 0:
            break
        skip += page_size

    return items


def _parse_retail_item(item: dict[str, Any]) -> InstanceSpec | None:
    sku = item.get("skuName", "")
    meter = item.get("meterName", "")
    if not sku or not meter:
        return None
    if "vCPU" not in meter or "Linux" not in meter:
        return None

    try:
        vcpus = int(item.get("vCPUs", 0))
        memory_gb = float(item.get("vCPUs", 0) * 4)
        unit_price = float(item.get("unitPrice", 0))
        product_name = item.get("productName", "")
        arm_sku = item.get("armSkuName", sku)

        if vcpus == 0:
            return None

        category = "general"
        if "memory" in product_name.lower() or "E-" in arm_sku:
            category = "memory"
        elif "compute" in product_name.lower() or "F-" in arm_sku:
            category = "compute"
        elif "storage" in product_name.lower() or "L-" in arm_sku:
            category = "storage"

        return InstanceSpec(
            instance_type=arm_sku,
            vcpus=vcpus,
            memory_gb=memory_gb,
            local_storage_gb=0.0,
            vm_cost_per_hour=unit_price,
            category=category,
            dbu_rate=infer_dbu_rate(arm_sku, vcpus),
        )
    except (ValueError, TypeError):
        return None
