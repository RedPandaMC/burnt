"""GCP Compute Engine instance catalog — fallback data and loading."""

from __future__ import annotations

import logging
import os
import re
from typing import Any

import requests

from ..base import InstanceSpec
from ..cache import PricingCache
from .rates import infer_dbu_rate

logger = logging.getLogger(__name__)

FALLBACK_CATALOG: dict[str, InstanceSpec] = {
    "n1-standard-4": InstanceSpec(
        instance_type="n1-standard-4",
        vcpus=4,
        memory_gb=15.0,
        local_storage_gb=0.0,
        dbu_rate=0.72,
        vm_cost_per_hour=0.21,
        category="general",
    ),
    "n1-standard-8": InstanceSpec(
        instance_type="n1-standard-8",
        vcpus=8,
        memory_gb=30.0,
        local_storage_gb=0.0,
        dbu_rate=1.44,
        vm_cost_per_hour=0.42,
        category="general",
    ),
    "n1-standard-16": InstanceSpec(
        instance_type="n1-standard-16",
        vcpus=16,
        memory_gb=60.0,
        local_storage_gb=0.0,
        dbu_rate=2.88,
        vm_cost_per_hour=0.84,
        category="general",
    ),
    "n1-highmem-8": InstanceSpec(
        instance_type="n1-highmem-8",
        vcpus=8,
        memory_gb=52.0,
        local_storage_gb=0.0,
        dbu_rate=1.44,
        vm_cost_per_hour=0.49,
        category="memory",
    ),
    "n1-highmem-16": InstanceSpec(
        instance_type="n1-highmem-16",
        vcpus=16,
        memory_gb=104.0,
        local_storage_gb=0.0,
        dbu_rate=2.88,
        vm_cost_per_hour=0.98,
        category="memory",
    ),
}

_CATALOG: dict[str, InstanceSpec] = {}
_CACHE: PricingCache | None = None


def _cache() -> PricingCache:
    global _CACHE
    if _CACHE is None:
        _CACHE = PricingCache(ttl_seconds=86400.0)
    return _CACHE


def _get_api_key() -> str | None:
    return os.environ.get("GCP_BILLING_API_KEY") or os.environ.get("BURNT_GCP_API_KEY")


def load_catalog(force_refresh: bool = False) -> dict[str, InstanceSpec]:
    global _CATALOG

    if _CATALOG and not force_refresh:
        return _CATALOG

    api_key = _get_api_key()
    if not api_key:
        logger.warning("GCP_BILLING_API_KEY not set — using fallback catalog")
        _CATALOG = FALLBACK_CATALOG
        return FALLBACK_CATALOG

    cache = _cache()
    cached = cache.get_disk("gcp_compute_catalog")
    if cached and not force_refresh:
        _CATALOG = {k: InstanceSpec(**v) for k, v in cached.items()}
        return _CATALOG

    try:
        skus = _fetch_gcp_skus(api_key)
        catalog: dict[str, InstanceSpec] = {}
        for sku in skus:
            spec = _parse_gcp_sku(sku)
            if spec is None:
                continue
            if spec.instance_type not in catalog:
                catalog[spec.instance_type] = spec

        _CATALOG = catalog
        cache.set_disk(
            "gcp_compute_catalog",
            {k: _spec_to_dict(v) for k, v in catalog.items()},
        )
        logger.info("GCP compute catalog refreshed: %d SKUs", len(catalog))
    except Exception as e:
        logger.warning("Failed to fetch GCP pricing API, using fallback catalog: %s", e)
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


def _fetch_gcp_skus(api_key: str, timeout: float = 10.0) -> list[dict[str, Any]]:
    billing_api = "https://cloudbilling.googleapis.com/v1"
    service_url = f"{billing_api}/services?key={api_key}"
    resp = requests.get(service_url, timeout=timeout)
    resp.raise_for_status()
    services = resp.json().get("services", [])
    compute_service = next(
        (s for s in services if "COMPUTE" in s.get("serviceId", "").upper()), None
    )
    if not compute_service:
        return []

    service_id = compute_service["serviceId"]
    skus_url = f"{billing_api}/services/{service_id}/skus?key={api_key}"
    resp2 = requests.get(skus_url, timeout=timeout)
    resp2.raise_for_status()
    return resp2.json().get("skus", [])


def _parse_gcp_sku(sku: dict[str, Any]) -> InstanceSpec | None:
    try:
        category = sku.get("category", {})
        usage_type = category.get("usageType", "")
        if "OnDemand" not in usage_type:
            return None

        description = sku.get("description", "")
        pricing_expr = sku.get("pricingInfo", [{}])[0].get("pricingExpression", {})
        tiered_rates = pricing_expr.get("tieredRates", [])
        if not tiered_rates:
            return None

        unit_price = tiered_rates[0].get("unitPrice", {})
        nanos = int(unit_price.get("nanos", 0))
        units = int(unit_price.get("units", 0))
        price = units + nanos / 1e9

        vcpu_match = re.search(r"(\d+)\s*vCPU", description)
        mem_match = re.search(r"(\d+)\s*GB", description)
        inst_match = re.search(r"(n1|n2|e2|c2)-[a-z0-9]+", description)

        if not vcpu_match or not mem_match:
            return None

        vcpus = int(vcpu_match.group(1))
        memory_gb = float(mem_match.group(1))
        instance_type = inst_match.group(0) if inst_match else f"custom-{vcpus}"

        family = description.lower()
        cat = "general"
        if "memory" in family or "extended memory" in family:
            cat = "memory"
        elif "compute" in family:
            cat = "compute"

        return InstanceSpec(
            instance_type=instance_type,
            vcpus=vcpus,
            memory_gb=memory_gb,
            local_storage_gb=0.0,
            vm_cost_per_hour=price,
            category=cat,
            dbu_rate=infer_dbu_rate(instance_type, vcpus),
        )
    except (ValueError, TypeError, KeyError, IndexError):
        return None
