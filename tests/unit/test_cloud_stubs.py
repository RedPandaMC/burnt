"""Verify the cloud stub packages are importable and the PricingBackend is accessible."""


class TestCloudStubImports:
    def test_import_cloud_package(self):
        import burnt.cloud  # noqa: F401

    def test_import_azure_databricks(self):
        import burnt.cloud.azure_databricks  # noqa: F401

    def test_import_aws_databricks(self):
        import burnt.cloud.aws_databricks  # noqa: F401

    def test_import_gcp_databricks(self):
        import burnt.cloud.gcp_databricks  # noqa: F401

    def test_import_onprem_spark(self):
        import burnt.cloud.onprem_spark  # noqa: F401


class TestPricingBackendImportable:
    def test_pricing_backend_importable(self):
        from burnt.core.pricing import PricingBackend

        assert PricingBackend is not None

    def test_exchange_rate_provider_importable(self):
        from burnt.core.exchange import ExchangeRateProvider

        assert ExchangeRateProvider is not None

    def test_cloud_region_currencies_importable(self):
        from burnt.core.pricing import CLOUD_REGION_CURRENCIES

        assert isinstance(CLOUD_REGION_CURRENCIES, dict)
        assert len(CLOUD_REGION_CURRENCIES) > 0

    def test_supported_currencies_importable(self):
        from burnt.core.pricing import SUPPORTED_CURRENCIES

        assert isinstance(SUPPORTED_CURRENCIES, frozenset)
        assert len(SUPPORTED_CURRENCIES) > 0
