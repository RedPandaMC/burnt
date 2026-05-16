"""Verify the provider packages are importable and ProviderBackend is accessible."""

import burnt.providers
import burnt.providers.aws_databricks
import burnt.providers.azure_databricks
import burnt.providers.gcp_databricks
import burnt.providers.onprem_spark  # noqa: F401


class TestProviderBackendImportable:
    def test_provider_backend_importable(self):
        from burnt.providers.base import ProviderBackend

        assert ProviderBackend is not None

    def test_exchange_rate_provider_importable(self):
        from burnt.providers.exchange import FrankfurterProvider

        assert FrankfurterProvider is not None


class TestProviderRegistry:
    def test_azure_backend_registered(self):
        from burnt.providers import get_backend

        p = get_backend("azure-databricks")
        assert p is not None
        assert p.name == "azure-databricks"

    def test_aws_backend_registered(self):
        from burnt.providers import get_backend

        p = get_backend("aws-databricks")
        assert p is not None
        assert p.name == "aws-databricks"

    def test_gcp_backend_registered(self):
        from burnt.providers import get_backend

        p = get_backend("gcp-databricks")
        assert p is not None
        assert p.name == "gcp-databricks"

    def test_onprem_backend_registered(self):
        from burnt.providers import get_backend

        p = get_backend("onprem-spark")
        assert p is not None
        assert p.name == "onprem-spark"

    def test_list_backends(self):
        from burnt.providers import list_backends

        backends = list_backends()
        assert "azure-databricks" in backends
        assert "aws-databricks" in backends
        assert "gcp-databricks" in backends
        assert "onprem-spark" in backends


class TestInstanceSpec:
    def test_instance_spec_total_vcpus(self):
        from burnt.providers.base import InstanceSpec

        spec = InstanceSpec(instance_type="test", vcpus=4, memory_gb=16.0)
        assert spec.total_vcpus(2) == 8
        assert spec.total_memory_gb(2) == 32.0
