"""Azure VM instance catalog and cluster right-sizing."""

import logging
from typing import Literal

import requests
from pydantic import BaseModel

from .cache import TTLCache
from .config import Settings
from .models import ClusterConfig
from .pricing import PHOTON_MULTIPLIER_AZURE

logger = logging.getLogger(__name__)

AZURE_PRICING_API = "https://prices.azure.com/api/retail/prices"


class InstanceSpec(BaseModel):
    """Specification for an Azure VM instance type."""

    instance_type: str
    vcpus: int
    memory_gb: float
    local_storage_gb: float
    dbu_rate: float
    photon_dbu_rate: float
    vm_cost_per_hour: float
    category: Literal["general", "memory", "compute", "storage"]

    def __init__(self, **data):
        if "photon_dbu_rate" not in data:
            data["photon_dbu_rate"] = float(data["dbu_rate"]) * float(
                PHOTON_MULTIPLIER_AZURE
            )
        super().__init__(**data)


class WorkloadProfile(BaseModel):
    """Profile of a workload for cluster right-sizing."""

    peak_memory_pct: float = 0.0
    peak_cpu_pct: float = 0.0
    spill_to_disk_bytes: int = 0
    data_gb: float = 0.0
    shuffle_bytes: int = 0
    compute_intensity: float = 0.5
    memory_intensity: float = 0.5


AZURE_INSTANCE_CATALOG: dict[str, InstanceSpec] = {
    # General Purpose - D-series (DS = Premium Storage)
    # Source: Azure pricing API (prices.azure.com/api/retail/prices), East US, Pay-As-You-Go
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
    "Standard_DS5_v2": InstanceSpec(
        instance_type="Standard_DS5_v2",
        vcpus=16,
        memory_gb=56.0,
        local_storage_gb=112.0,
        dbu_rate=3.00,
        vm_cost_per_hour=1.170,
        category="general",
    ),
    # General Purpose - Dv3-series
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
    # Memory Optimized - E-series
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
    "Standard_E32s_v3": InstanceSpec(
        instance_type="Standard_E32s_v3",
        vcpus=32,
        memory_gb=256.0,
        local_storage_gb=256.0,
        dbu_rate=6.00,
        vm_cost_per_hour=2.016,
        category="memory",
    ),
    "Standard_E64s_v3": InstanceSpec(
        instance_type="Standard_E64s_v3",
        vcpus=64,
        memory_gb=432.0,
        local_storage_gb=432.0,
        dbu_rate=12.00,
        vm_cost_per_hour=4.032,
        category="memory",
    ),
    # Compute Optimized - F-series
    "Standard_F8s_v2": InstanceSpec(
        instance_type="Standard_F8s_v2",
        vcpus=8,
        memory_gb=16.0,
        local_storage_gb=32.0,
        dbu_rate=1.50,
        vm_cost_per_hour=0.338,
        category="compute",
    ),
    "Standard_F16s_v2": InstanceSpec(
        instance_type="Standard_F16s_v2",
        vcpus=16,
        memory_gb=32.0,
        local_storage_gb=64.0,
        dbu_rate=3.00,
        vm_cost_per_hour=0.677,
        category="compute",
    ),
    "Standard_F32s_v2": InstanceSpec(
        instance_type="Standard_F32s_v2",
        vcpus=32,
        memory_gb=64.0,
        local_storage_gb=128.0,
        dbu_rate=6.00,
        vm_cost_per_hour=1.354,
        category="compute",
    ),
    "Standard_F64s_v2": InstanceSpec(
        instance_type="Standard_F64s_v2",
        vcpus=64,
        memory_gb=128.0,
        local_storage_gb=256.0,
        dbu_rate=12.00,
        vm_cost_per_hour=2.708,
        category="compute",
    ),
    # Storage Optimized - L-series
    "Standard_L8s_v3": InstanceSpec(
        instance_type="Standard_L8s_v3",
        vcpus=8,
        memory_gb=64.0,
        local_storage_gb=160.0,
        dbu_rate=1.50,
        vm_cost_per_hour=0.624,
        category="storage",
    ),
    "Standard_L16s_v3": InstanceSpec(
        instance_type="Standard_L16s_v3",
        vcpus=16,
        memory_gb=128.0,
        local_storage_gb=320.0,
        dbu_rate=3.00,
        vm_cost_per_hour=1.248,
        category="storage",
    ),
    "Standard_L32s_v3": InstanceSpec(
        instance_type="Standard_L32s_v3",
        vcpus=32,
        memory_gb=256.0,
        local_storage_gb=640.0,
        dbu_rate=6.00,
        vm_cost_per_hour=2.496,
        category="storage",
    ),
    "Standard_L64s_v3": InstanceSpec(
        instance_type="Standard_L64s_v3",
        vcpus=64,
        memory_gb=512.0,
        local_storage_gb=1280.0,
        dbu_rate=12.00,
        vm_cost_per_hour=4.992,
        category="storage",
    ),
    # Additional General Purpose - Ddv4
    "Standard_D2ds_v4": InstanceSpec(
        instance_type="Standard_D2ds_v4",
        vcpus=2,
        memory_gb=8.0,
        local_storage_gb=75.0,
        dbu_rate=0.55,
        vm_cost_per_hour=0.138,
        category="general",
    ),
    "Standard_D4ds_v4": InstanceSpec(
        instance_type="Standard_D4ds_v4",
        vcpus=4,
        memory_gb=16.0,
        local_storage_gb=150.0,
        dbu_rate=1.10,
        vm_cost_per_hour=0.276,
        category="general",
    ),
    # Additional Memory - Eav4
    "Standard_E4a_v4": InstanceSpec(
        instance_type="Standard_E4a_v4",
        vcpus=4,
        memory_gb=32.0,
        local_storage_gb=68.0,
        dbu_rate=0.85,
        vm_cost_per_hour=0.23,
        category="memory",
    ),
    # Additional Compute - FX
    "Standard_F4s": InstanceSpec(
        instance_type="Standard_F4s",
        vcpus=4,
        memory_gb=8.0,
        local_storage_gb=32.0,
        dbu_rate=0.90,
        vm_cost_per_hour=0.19,
        category="compute",
    ),
}


class AzureInstanceCatalog:
    """Catalog of Azure VM instance types for Databricks clusters."""

    def __init__(self, instances: dict[str, InstanceSpec] | None = None):
        self._instances = instances or AZURE_INSTANCE_CATALOG

    def filter_by(
        self,
        category: str | None = None,
        min_memory_gb: float | None = None,
        min_vcpus: int | None = None,
    ) -> list[InstanceSpec]:
        """Filter instances by category, memory, and vCPU requirements."""
        results = list(self._instances.values())
        if category:
            results = [i for i in results if i.category == category]
        if min_memory_gb is not None:
            results = [i for i in results if i.memory_gb >= min_memory_gb]
        if min_vcpus is not None:
            results = [i for i in results if i.vcpus >= min_vcpus]
        return sorted(results, key=lambda x: (x.vcpus, x.memory_gb))

    def get(self, instance_type: str) -> InstanceSpec:
        """Get instance specification by type."""
        if instance_type not in self._instances:
            raise KeyError(f"Unknown instance type: {instance_type}")
        return self._instances[instance_type]

    def find_smaller(self, current: str) -> InstanceSpec | None:
        """Find the next smaller instance in the same family."""
        current_spec = self.get(current)
        same_category = self.filter_by(category=current_spec.category)
        smaller = [i for i in same_category if i.vcpus < current_spec.vcpus]
        if smaller:
            return max(smaller, key=lambda x: x.vcpus)
        return None

    def find_larger(self, current: str) -> InstanceSpec | None:
        """Find the next larger instance in the same family."""
        current_spec = self.get(current)
        same_category = self.filter_by(category=current_spec.category)
        larger = [i for i in same_category if i.vcpus > current_spec.vcpus]
        if larger:
            return min(larger, key=lambda x: x.vcpus)
        return None

    def recommend_for_workload(self, profile: WorkloadProfile) -> InstanceSpec:
        """Recommend an instance type based on workload profile."""
        if profile.spill_to_disk_bytes > 0:
            candidates = self.filter_by(category="memory", min_memory_gb=64.0)
            if candidates:
                return candidates[0]
        if profile.compute_intensity > 0.7:
            candidates = self.filter_by(category="compute", min_memory_gb=16.0)
            if candidates:
                return candidates[0]
        if profile.memory_intensity > 0.7:
            candidates = self.filter_by(category="memory", min_memory_gb=32.0)
            if candidates:
                return candidates[0]
        candidates = self.filter_by(category="general", min_memory_gb=14.0)
        if candidates:
            return candidates[0]
        return self._instances["Standard_DS3_v2"]


_CATALOG = AzureInstanceCatalog()
_pricing_cache: TTLCache | None = None


def _get_pricing_cache() -> TTLCache:
    """Get or create the pricing cache singleton."""
    global _pricing_cache
    if _pricing_cache is None:
        settings = Settings()
        _pricing_cache = TTLCache(ttl_seconds=settings.cache.ttl_seconds)
    return _pricing_cache


def fetch_azure_pricing(
    location: str = "East US",
    timeout: float = 5.0,
) -> dict[str, dict]:
    """
    Fetch VM pricing from Azure Retail Prices API.

    Source: https://prices.azure.com/api/retail/prices

    Args:
        location: Azure region (default: East US)
        timeout: Request timeout in seconds

    Returns:
        Dict mapping instance_type -> {vcpus, memory_gb, vm_cost_per_hour, ...}
    """
    params = {
        "$filter": f"serviceName eq 'Virtual Machines' and location eq '{location}'",
    }
    response = requests.get(AZURE_PRICING_API, params=params, timeout=timeout)
    response.raise_for_status()

    items = response.json().get("Items", [])
    pricing = {}

    for item in items:
        meter_name = item.get("meterName", "")
        if "vCPU" not in meter_name or "Linux" not in meter_name:
            continue

        instance_type = item.get("skuName", "").replace(" ", "_")
        if not instance_type:
            continue

        pricing[instance_type] = {
            "instance_type": instance_type,
            "vcpus": int(item.get("vCPUs", 0)),
            "memory_gb": float(item.get("vCPUs", 0) * 4),
            "vm_cost_per_hour": float(item.get("unitPrice", 0)),
            "location": location,
            "currency": item.get("currencyCode", "USD"),
        }

    return pricing


def get_fresh_pricing(
    force_refresh: bool = False,
) -> dict[str, dict]:
    """
    Get latest Azure VM pricing with caching.

    Priority:
    1. Return cached if valid (unless force_refresh)
    2. Try API fetch if pricing_source != "embedded"
    3. Fall back to embedded with warning log if API fails

    Args:
        force_refresh: Skip cache and force API fetch

    Returns:
        Dict mapping instance_type -> pricing info dict
    """
    settings = Settings()
    cache = _get_pricing_cache()

    if not force_refresh:
        cached = cache.get("azure_pricing")
        if cached is not None:
            return cached

    if settings.pricing_source == "embedded":
        logger.warning(
            "Using embedded pricing (pricing_source='embedded'). "
            "Set pricing_source='api' for latest Azure pricing."
        )
        return {}

    try:
        pricing = fetch_azure_pricing()
        cache.set("azure_pricing", pricing)
        return pricing
    except Exception as e:
        logger.warning(
            f"Failed to fetch Azure pricing API: {e}. Falling back to embedded pricing."
        )
        return {}


def get_cluster_json(
    profile: WorkloadProfile,
    current_config: ClusterConfig | None = None,
    prefer_spot: bool = True,
    max_ips: int | None = None,
    spark_version: str = "15.4.x-scala2.12",
) -> dict:
    """
    Generate Databricks Jobs API-compatible cluster JSON based on workload profile.

    Args:
        profile: Workload characteristics (memory, CPU, spill, etc.)
        current_config: Current cluster configuration (used for baseline comparison)
        prefer_spot: Whether to use spot instances with on-demand fallback
        max_ips: Maximum instance pool IPs available (constrains worker count)
        spark_version: Databricks spark version string

    Returns:
        Dict with "new_cluster" key for Databricks Jobs API
    """
    instance = _CATALOG.recommend_for_workload(profile)

    num_workers = 2
    if current_config is not None:
        num_workers = current_config.num_workers
        if profile.peak_memory_pct < 30 and profile.peak_cpu_pct < 40:
            num_workers = max(2, num_workers - 1)
        elif profile.peak_memory_pct > 70 or profile.peak_cpu_pct > 70:
            num_workers = min(8, num_workers + 2)

    if max_ips is not None and max_ips > 0:
        available_ips = max_ips - 1
        if num_workers > available_ips:
            larger = _CATALOG.find_larger(instance.instance_type)
            if larger:
                instance = larger
            num_workers = min(num_workers, available_ips)

    spot_policy = "SPOT_WITH_ON_DEMAND_FALLBACK" if prefer_spot else "ON_DEMAND"

    return {
        "new_cluster": {
            "spark_version": spark_version,
            "node_type_id": instance.instance_type,
            "num_workers": num_workers,
            "spark_conf": {},
            "azure_attributes": {
                "availability": spot_policy,
            },
        }
    }


def get_cluster_config(
    profile: WorkloadProfile,
    current_config: ClusterConfig | None = None,
    prefer_spot: bool = True,
    max_ips: int | None = None,
) -> ClusterConfig:
    """Get ClusterConfig based on workload profile."""
    instance = _CATALOG.recommend_for_workload(profile)

    num_workers = 2
    if current_config is not None:
        num_workers = current_config.num_workers
        if profile.peak_memory_pct < 30 and profile.peak_cpu_pct < 40:
            num_workers = max(2, num_workers - 1)
        elif profile.peak_memory_pct > 70 or profile.peak_cpu_pct > 70:
            num_workers = min(8, num_workers + 2)

    if max_ips is not None and max_ips > 0:
        available_ips = max_ips - 1
        if num_workers > available_ips:
            larger = _CATALOG.find_larger(instance.instance_type)
            if larger:
                instance = larger
            num_workers = min(num_workers, available_ips)

    return ClusterConfig(
        instance_type=instance.instance_type,
        num_workers=num_workers,
        dbu_per_hour=instance.dbu_rate,
    )
