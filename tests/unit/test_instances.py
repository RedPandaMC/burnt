"""Unit tests for Azure instance catalog."""

import pytest

from burnt.core.instances import (
    AZURE_INSTANCE_CATALOG,
    AzureInstanceCatalog,
    InstanceSpec,
    WorkloadProfile,
    get_cluster_config,
    get_cluster_json,
)
from burnt.core.models import ClusterConfig


class TestInstanceSpec:
    def test_instance_spec_creation(self):
        spec = InstanceSpec(
            instance_type="Standard_DS3_v2",
            vcpus=4,
            memory_gb=14.0,
            local_storage_gb=28.0,
            dbu_rate=0.75,
            vm_cost_per_hour=0.293,
            category="general",
        )
        assert spec.instance_type == "Standard_DS3_v2"
        assert spec.vcpus == 4
        assert spec.memory_gb == 14.0
        assert spec.dbu_rate == 0.75

    def test_instance_spec_photon_default(self):
        spec = InstanceSpec(
            instance_type="Standard_DS3_v2",
            vcpus=4,
            memory_gb=14.0,
            local_storage_gb=28.0,
            dbu_rate=0.75,
            vm_cost_per_hour=0.293,
            category="general",
        )
        assert spec.photon_dbu_rate == 0.75 * 2.5

    def test_instance_spec_photon_explicit(self):
        spec = InstanceSpec(
            instance_type="Standard_DS3_v2",
            vcpus=4,
            memory_gb=14.0,
            local_storage_gb=28.0,
            dbu_rate=0.75,
            photon_dbu_rate=2.0,
            vm_cost_per_hour=0.293,
            category="general",
        )
        assert spec.photon_dbu_rate == 2.0


class TestWorkloadProfile:
    def test_workload_profile_defaults(self):
        profile = WorkloadProfile()
        assert profile.peak_memory_pct == 0.0
        assert profile.peak_cpu_pct == 0.0
        assert profile.spill_to_disk_bytes == 0
        assert profile.compute_intensity == 0.5
        assert profile.memory_intensity == 0.5

    def test_workload_profile_creation(self):
        profile = WorkloadProfile(
            peak_memory_pct=50.0,
            peak_cpu_pct=80.0,
            spill_to_disk_bytes=1024,
            data_gb=10.0,
            compute_intensity=0.8,
        )
        assert profile.peak_memory_pct == 50.0
        assert profile.peak_cpu_pct == 80.0
        assert profile.spill_to_disk_bytes == 1024


class TestAzureInstanceCatalog:
    def test_catalog_has_22_plus_instances(self):
        assert len(AZURE_INSTANCE_CATALOG) >= 22

    def test_catalog_has_all_categories(self):
        categories = {inst.category for inst in AZURE_INSTANCE_CATALOG.values()}
        assert categories == {"general", "memory", "compute", "storage"}

    def test_filter_by_category(self):
        catalog = AzureInstanceCatalog()
        general = catalog.filter_by(category="general")
        assert all(i.category == "general" for i in general)
        assert len(general) > 0

    def test_filter_by_min_memory(self):
        catalog = AzureInstanceCatalog()
        large = catalog.filter_by(min_memory_gb=64.0)
        assert all(i.memory_gb >= 64.0 for i in large)

    def test_filter_by_min_vcpus(self):
        catalog = AzureInstanceCatalog()
        many_core = catalog.filter_by(min_vcpus=16)
        assert all(i.vcpus >= 16 for i in many_core)

    def test_filter_by_combined(self):
        catalog = AzureInstanceCatalog()
        results = catalog.filter_by(category="memory", min_memory_gb=128.0)
        assert all(i.category == "memory" and i.memory_gb >= 128.0 for i in results)

    def test_get_existing(self):
        catalog = AzureInstanceCatalog()
        spec = catalog.get("Standard_DS3_v2")
        assert spec.instance_type == "Standard_DS3_v2"
        assert spec.vcpus == 4

    def test_get_unknown_raises(self):
        catalog = AzureInstanceCatalog()
        with pytest.raises(KeyError):
            catalog.get("Standard_X99_yz")

    def test_find_smaller(self):
        catalog = AzureInstanceCatalog()
        smaller = catalog.find_smaller("Standard_DS4_v2")
        assert smaller is not None
        assert smaller.instance_type == "Standard_DS3_v2"

    def test_find_smaller_smallest_returns_none(self):
        catalog = AzureInstanceCatalog()
        smaller = catalog.find_smaller("Standard_D2ds_v4")
        assert smaller is None

    def test_find_larger(self):
        catalog = AzureInstanceCatalog()
        larger = catalog.find_larger("Standard_DS3_v2")
        assert larger is not None
        assert larger.instance_type == "Standard_DS4_v2"

    def test_find_larger_largest_returns_none(self):
        catalog = AzureInstanceCatalog()
        larger = catalog.find_larger("Standard_L64s_v3")
        assert larger is None

    def test_recommend_for_workload_memory_intensive(self):
        catalog = AzureInstanceCatalog()
        profile = WorkloadProfile(spill_to_disk_bytes=1000)
        spec = catalog.recommend_for_workload(profile)
        assert spec.category == "memory"

    def test_recommend_for_workload_compute_intensive(self):
        catalog = AzureInstanceCatalog()
        profile = WorkloadProfile(compute_intensity=0.9)
        spec = catalog.recommend_for_workload(profile)
        assert spec.category == "compute"

    def test_recommend_for_workload_balanced(self):
        catalog = AzureInstanceCatalog()
        profile = WorkloadProfile(compute_intensity=0.5, memory_intensity=0.5)
        spec = catalog.recommend_for_workload(profile)
        assert spec.category == "general"


class TestGetClusterJson:
    def test_get_cluster_json_basic(self):
        profile = WorkloadProfile(peak_memory_pct=50.0, peak_cpu_pct=50.0)
        result = get_cluster_json(profile)
        assert "new_cluster" in result
        assert "spark_version" in result["new_cluster"]
        assert "node_type_id" in result["new_cluster"]
        assert "num_workers" in result["new_cluster"]

    def test_get_cluster_json_has_azure_attributes(self):
        profile = WorkloadProfile()
        result = get_cluster_json(profile, prefer_spot=True)
        assert "azure_attributes" in result["new_cluster"]
        assert result["new_cluster"]["azure_attributes"]["availability"] == (
            "SPOT_WITH_ON_DEMAND_FALLBACK"
        )

    def test_get_cluster_json_on_demand(self):
        profile = WorkloadProfile()
        result = get_cluster_json(profile, prefer_spot=False)
        assert result["new_cluster"]["azure_attributes"]["availability"] == (
            "ON_DEMAND"
        )

    def test_get_cluster_json_custom_spark_version(self):
        profile = WorkloadProfile()
        result = get_cluster_json(profile, spark_version="14.3.x-scala2.12")
        assert result["new_cluster"]["spark_version"] == "14.3.x-scala2.12"

    def test_get_cluster_json_downsizes_low_utilization(self):
        profile = WorkloadProfile(peak_memory_pct=20.0, peak_cpu_pct=30.0)
        current = ClusterConfig(num_workers=4)
        result = get_cluster_json(profile, current_config=current)
        assert result["new_cluster"]["num_workers"] == 3

    def test_get_cluster_json_upsizes_high_utilization(self):
        profile = WorkloadProfile(peak_memory_pct=80.0, peak_cpu_pct=80.0)
        current = ClusterConfig(num_workers=2)
        result = get_cluster_json(profile, current_config=current)
        assert result["new_cluster"]["num_workers"] == 4

    def test_get_cluster_json_max_ips(self):
        profile = WorkloadProfile()
        result = get_cluster_json(profile, max_ips=4)
        available_ips = 4 - 1
        assert result["new_cluster"]["num_workers"] <= available_ips


class TestGetClusterConfig:
    def test_get_cluster_config_returns_cluster_config(self):
        profile = WorkloadProfile()
        config = get_cluster_config(profile)
        assert isinstance(config, ClusterConfig)
        assert config.instance_type == "Standard_DS3_v2"

    def test_get_cluster_config_with_current(self):
        profile = WorkloadProfile(peak_memory_pct=80.0, peak_cpu_pct=80.0)
        current = ClusterConfig(num_workers=4, instance_type="Standard_DS4_v2")
        config = get_cluster_config(profile, current_config=current)
        assert config.num_workers == 6


class TestTTLCache:
    def test_cache_set_and_get(self):
        from burnt.core.cache import TTLCache

        cache = TTLCache(ttl_seconds=60.0)
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

    def test_cache_miss_returns_none(self):
        from burnt.core.cache import TTLCache

        cache = TTLCache(ttl_seconds=60.0)
        assert cache.get("nonexistent") is None

    def test_cache_clear(self):
        from burnt.core.cache import TTLCache

        cache = TTLCache(ttl_seconds=60.0)
        cache.set("key1", "value1")
        cache.clear()
        assert cache.get("key1") is None


class TestGetFreshPricing:
    def test_get_fresh_pricing_embedded_returns_empty(self, monkeypatch):
        import burnt.core.instances as instances

        monkeypatch.setenv("BURNT_PRICING_SOURCE", "embedded")
        instances._pricing_cache = None
        pricing = instances.get_fresh_pricing(force_refresh=True)
        assert pricing == {}

    def test_get_fresh_pricing_api_fallback_logs_warning(self, monkeypatch, caplog):
        import requests

        import burnt.core.instances as instances

        monkeypatch.setenv("BURNT_PRICING_SOURCE", "api")
        instances._pricing_cache = None

        class MockResponse:
            def raise_for_status(self):
                raise requests.RequestException("Network error")

        monkeypatch.setattr(requests, "get", lambda *args, **kwargs: MockResponse())

        with caplog.at_level("WARNING"):
            pricing = instances.get_fresh_pricing(force_refresh=True)

        assert pricing == {}
        assert "Falling back to embedded pricing" in caplog.text
