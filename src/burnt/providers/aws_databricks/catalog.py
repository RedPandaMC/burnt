"""AWS EC2 instance catalog — fallback data and loading."""

from __future__ import annotations

import logging
from typing import Any

import requests

from ..base import InstanceSpec
from ..cache import PricingCache
from .rates import infer_dbu_rate

logger = logging.getLogger(__name__)

FALLBACK_CATALOG: dict[str, InstanceSpec] = {
    "r5.xlarge": InstanceSpec(
        instance_type="r5.xlarge",
        vcpus=4,
        memory_gb=32.0,
        local_storage_gb=0.0,
        dbu_rate=0.72,
        vm_cost_per_hour=0.252,
        category="memory",
    ),
    "r5.2xlarge": InstanceSpec(
        instance_type="r5.2xlarge",
        vcpus=8,
        memory_gb=64.0,
        local_storage_gb=0.0,
        dbu_rate=1.44,
        vm_cost_per_hour=0.504,
        category="memory",
    ),
    "m5.xlarge": InstanceSpec(
        instance_type="m5.xlarge",
        vcpus=4,
        memory_gb=16.0,
        local_storage_gb=0.0,
        dbu_rate=0.72,
        vm_cost_per_hour=0.192,
        category="general",
    ),
    "m5.2xlarge": InstanceSpec(
        instance_type="m5.2xlarge",
        vcpus=8,
        memory_gb=32.0,
        local_storage_gb=0.0,
        dbu_rate=1.44,
        vm_cost_per_hour=0.384,
        category="general",
    ),
    "c5.xlarge": InstanceSpec(
        instance_type="c5.xlarge",
        vcpus=4,
        memory_gb=8.0,
        local_storage_gb=0.0,
        dbu_rate=0.64,
        vm_cost_per_hour=0.17,
        category="compute",
    ),
    "c5.2xlarge": InstanceSpec(
        instance_type="c5.2xlarge",
        vcpus=8,
        memory_gb=16.0,
        local_storage_gb=0.0,
        dbu_rate=1.28,
        vm_cost_per_hour=0.34,
        category="compute",
    ),
}

_CATALOG: dict[str, InstanceSpec] = {}
_REGION_INDEX: dict[str, str] = {}
_CACHE: PricingCache | None = None


def _cache() -> PricingCache:
    global _CACHE
    if _CACHE is None:
        _CACHE = PricingCache(ttl_seconds=86400.0)
    return _CACHE


def load_catalog(
    region: str = "us-east-1", force_refresh: bool = False
) -> dict[str, InstanceSpec]:
    global _CATALOG, _REGION_INDEX

    cache_key = f"aws_ec2_catalog_{region}"
    cache = _cache()

    if not force_refresh:
        cached = cache.get_disk(cache_key)
        if cached:
            _CATALOG = {k: InstanceSpec(**v) for k, v in cached.items()}
            return _CATALOG

    try:
        if not _REGION_INDEX:
            _REGION_INDEX.update(_fetch_region_index())

        url_suffix = _REGION_INDEX.get(region)
        if not url_suffix:
            logger.warning("AWS region %s not found in pricing index", region)
            _CATALOG = FALLBACK_CATALOG
            return FALLBACK_CATALOG

        products = _fetch_region_pricing(url_suffix)
        catalog: dict[str, InstanceSpec] = {}
        for product in products:
            spec = _parse_ec2_product(product)
            if spec is None:
                continue
            if spec.instance_type not in catalog:
                catalog[spec.instance_type] = spec

        _CATALOG = catalog
        cache.set_disk(
            cache_key,
            {k: _spec_to_dict(v) for k, v in catalog.items()},
        )
        logger.info(
            "AWS EC2 catalog refreshed for %s: %d instance types",
            region,
            len(catalog),
        )
    except Exception as e:
        logger.warning("Failed to fetch AWS pricing API, using fallback catalog: %s", e)
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


def _fetch_region_index(timeout: float = 10.0) -> dict[str, str]:
    url = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/region_index.json"
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    regions = data.get("regions", {})
    return {code: info["currentVersionUrl"] for code, info in regions.items()}


def _fetch_region_pricing(
    region_url_suffix: str, timeout: float = 30.0
) -> list[dict[str, Any]]:
    url = f"https://pricing.us-east-1.amazonaws.com{region_url_suffix}"
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return list(resp.json().get("products", {}).values())


def _parse_ec2_product(product: dict[str, Any]) -> InstanceSpec | None:
    try:
        attrs = product.get("attributes", {})
        inst = attrs.get("instanceType", "")
        vcpus = int(attrs.get("vCPU", 0))
        memory_gb = float(attrs.get("memory", "0").replace(" GiB", "").split()[0])
        usd = attrs.get("usd", "0")
        if isinstance(usd, str):
            usd = usd.replace(",", "")
        price = float(usd) if usd else 0.0
        family = attrs.get("instanceFamily", "")

        if not inst or vcpus == 0:
            return None

        category = "general"
        if "memory" in family.lower():
            category = "memory"
        elif "compute" in family.lower():
            category = "compute"
        elif "storage" in family.lower():
            category = "storage"

        return InstanceSpec(
            instance_type=inst,
            vcpus=vcpus,
            memory_gb=memory_gb,
            local_storage_gb=0.0,
            vm_cost_per_hour=price,
            category=category,
            dbu_rate=infer_dbu_rate(inst, vcpus),
        )
    except (ValueError, TypeError, KeyError):
        return None
