import tempfile
from pathlib import Path

import pytest

from burnt.core.config import (
    AzureSettings,
    CacheSettings,
    DatabricksSettings,
    LintSettings,
    OnPremSparkSettings,
    PricingSettings,
    Settings,
    SystemTablesSettings,
)


class TestSettings:
    def test_settings_default_values(self):
        settings = Settings()
        assert settings.workspace_url is None
        assert settings.token is None
        assert settings.target_currency == "USD"
        assert settings.pricing_source == "api"
        assert settings.cache.ttl_seconds == 3600.0
        assert settings.lint.fail_on == "error"
        assert settings.lint.select == ["ALL"]

    def test_settings_from_env_vars(self, monkeypatch):
        monkeypatch.setenv(
            "BURNT_WORKSPACE_URL", "https://example.cloud.databricks.com"
        )
        monkeypatch.setenv("BURNT_TOKEN", "test_token")
        monkeypatch.setenv("BURNT_TARGET_CURRENCY", "EUR")

        settings = Settings()
        assert settings.workspace_url == "https://example.cloud.databricks.com"
        assert settings.token == "test_token"
        assert settings.target_currency == "EUR"

    def test_settings_from_toml_burnt_toml(self, monkeypatch):
        monkeypatch.delenv("BURNT_WORKSPACE_URL", raising=False)
        monkeypatch.delenv("BURNT_TOKEN", raising=False)
        monkeypatch.delenv("BURNT_TARGET_CURRENCY", raising=False)

        # .burnt.toml style — top-level keys, no [burnt] prefix
        toml_content = """
workspace_url = "https://test.cloud.databricks.com"
token = "toml_token"
target_currency = "GBP"
pricing_source = "live"

[lint]
fail-on = "warning"
ignore = ["cross_join"]

[cache]
ttl-seconds = 600
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".toml", delete=False
        ) as f:
            f.write(toml_content)
            f.flush()
            path = Path(f.name)

        try:
            settings = Settings.from_toml(path)
            assert settings.workspace_url == "https://test.cloud.databricks.com"
            assert settings.token == "toml_token"
            assert settings.target_currency == "GBP"
            assert settings.pricing_source == "live"
            assert settings.lint.fail_on == "warning"
            assert settings.lint.ignore == ["cross_join"]
            assert settings.cache.ttl_seconds == 600
        finally:
            path.unlink()

    def test_settings_from_pyproject_toml(self, monkeypatch):
        monkeypatch.delenv("BURNT_WORKSPACE_URL", raising=False)
        monkeypatch.delenv("BURNT_TOKEN", raising=False)

        toml_content = """
[tool.burnt]
workspace_url = "https://pyproject.cloud.databricks.com"
token = "pyproject_token"

[tool.burnt.lint]
select = ["cross_join", "select_star"]
fail-on = "error"

[tool.burnt.cache]
enabled = false
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".toml", delete=False
        ) as f:
            f.write(toml_content)
            f.flush()
            # Rename to pyproject.toml so our code recognises it
            path = Path(f.name)
            pyproject_path = path.parent / "pyproject.toml"
            path.rename(pyproject_path)

        try:
            settings = Settings.from_toml(pyproject_path)
            assert settings.workspace_url == "https://pyproject.cloud.databricks.com"
            assert settings.token == "pyproject_token"
            assert settings.lint.select == ["cross_join", "select_star"]
            assert settings.lint.fail_on == "error"
            assert settings.cache.enabled is False
        finally:
            pyproject_path.unlink(missing_ok=True)


class TestLintSettings:
    def test_defaults(self):
        lint = LintSettings()
        assert lint.select == ["ALL"]
        assert lint.ignore == []
        assert lint.fail_on == "error"
        assert lint.exclude == []
        assert lint.per_file_ignores == {}


class TestCacheSettings:
    def test_defaults(self):
        cache = CacheSettings()
        assert cache.enabled is True
        assert cache.ttl_seconds == 3600.0


class TestPricingSettings:
    def test_defaults(self):
        p = PricingSettings()
        assert p.backend is None

    def test_valid_backends(self):
        for backend in (
            "azure-databricks",
            "aws-databricks",
            "gcp-databricks",
            "onprem-spark",
        ):
            p = PricingSettings(backend=backend)
            assert p.backend == backend

    def test_invalid_backend_raises(self):
        from burnt.core.exceptions import ConfigError

        with pytest.raises((ConfigError, ValueError)):
            PricingSettings(backend="invalid-backend")

    def test_none_backend_allowed(self):
        p = PricingSettings(backend=None)
        assert p.backend is None


class TestSystemTablesSettings:
    def test_defaults(self):
        st = SystemTablesSettings()
        assert st.enabled is True
        assert st.query_history == "system.query.history"
        assert st.billing_usage == "system.billing.usage"
        assert st.list_prices == "system.billing.list_prices"
        assert st.compute_clusters == "system.compute.clusters"
        assert st.node_timeline == "system.compute.node_timeline"

    def test_custom_list_prices(self):
        st = SystemTablesSettings(list_prices="custom.billing.prices")
        assert st.list_prices == "custom.billing.prices"


class TestDatabricksSettings:
    def test_defaults(self):
        d = DatabricksSettings()
        assert isinstance(d.system_tables, SystemTablesSettings)
        assert d.system_tables.enabled is True


class TestOnPremSparkSettings:
    def test_defaults(self):
        s = OnPremSparkSettings()
        assert s.cost_per_vcpu_hour is None
        assert s.cost_per_gb_hour is None
        assert s.cost_per_gb_shuffle is None

    def test_with_rates(self):
        s = OnPremSparkSettings(
            cost_per_vcpu_hour=0.048,
            cost_per_gb_hour=0.006,
            cost_per_gb_shuffle=0.001,
        )
        assert s.cost_per_vcpu_hour == 0.048
        assert s.cost_per_gb_hour == 0.006
        assert s.cost_per_gb_shuffle == 0.001


class TestAzureSettings:
    def test_defaults(self):
        a = AzureSettings()
        assert a.region == "eastus"
        assert a.subscription_id is None

    def test_custom_region(self):
        a = AzureSettings(region="uksouth", subscription_id="sub-123")
        assert a.region == "uksouth"
        assert a.subscription_id == "sub-123"


class TestSettingsNewSections:
    def test_settings_has_pricing(self):
        s = Settings()
        assert isinstance(s.pricing, PricingSettings)

    def test_settings_has_databricks(self):
        s = Settings()
        assert isinstance(s.databricks, DatabricksSettings)

    def test_settings_has_onprem_spark(self):
        s = Settings()
        assert isinstance(s.onprem_spark, OnPremSparkSettings)

    def test_settings_has_azure_databricks(self):
        s = Settings()
        assert isinstance(s.azure_databricks, AzureSettings)

    def test_pricing_backend_env_var_syncs(self, monkeypatch):
        monkeypatch.setenv("BURNT_PRICING_BACKEND", "azure-databricks")
        s = Settings()
        assert s.pricing.backend == "azure-databricks"

    def test_pricing_backend_nested_env_var(self, monkeypatch):
        monkeypatch.setenv("BURNT_PRICING__BACKEND", "onprem-spark")
        s = Settings()
        assert s.pricing.backend == "onprem-spark"

    def test_from_toml_pricing_section(self, monkeypatch):
        monkeypatch.delenv("BURNT_PRICING_BACKEND", raising=False)
        monkeypatch.delenv("BURNT_PRICING__BACKEND", raising=False)

        toml_content = """
[pricing]
backend = "onprem-spark"

[onprem_spark]
cost_per_vcpu_hour = 0.048
cost_per_gb_hour = 0.006

[azure_databricks]
region = "uksouth"
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".toml", delete=False
        ) as f:
            f.write(toml_content)
            f.flush()
            path = Path(f.name)

        try:
            settings = Settings.from_toml(path)
            assert settings.pricing.backend == "onprem-spark"
            assert settings.onprem_spark.cost_per_vcpu_hour == 0.048
            assert settings.onprem_spark.cost_per_gb_hour == 0.006
            assert settings.azure_databricks.region == "uksouth"
        finally:
            path.unlink()

    def test_from_toml_databricks_system_tables(self):
        toml_content = """
[databricks.system_tables]
enabled = false
list_prices = "custom.billing.list_prices"
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".toml", delete=False
        ) as f:
            f.write(toml_content)
            f.flush()
            path = Path(f.name)

        try:
            settings = Settings.from_toml(path)
            assert settings.databricks.system_tables.enabled is False
            assert (
                settings.databricks.system_tables.list_prices
                == "custom.billing.list_prices"
            )
        finally:
            path.unlink()


class TestSettingsMerge:
    def test_merge_first_wins(self):
        s1 = Settings(workspace_url="https://first.example.com")
        s2 = Settings(workspace_url="https://second.example.com")
        merged = Settings.merge(s1, s2)
        assert merged.workspace_url == "https://first.example.com"

    def test_merge_second_fills_missing(self):
        s1 = Settings()
        s2 = Settings(token="fallback_token")
        merged = Settings.merge(s1, s2)
        assert merged.token == "fallback_token"

    def test_merge_lint_first_wins(self):
        s1 = Settings(lint=LintSettings(fail_on="warning"))
        s2 = Settings(lint=LintSettings(fail_on="error"))
        merged = Settings.merge(s1, s2)
        assert merged.lint.fail_on == "warning"

    def test_merge_pricing_first_wins(self):
        s1 = Settings(pricing=PricingSettings(backend="azure-databricks"))
        s2 = Settings(pricing=PricingSettings(backend="onprem-spark"))
        merged = Settings.merge(s1, s2)
        assert merged.pricing.backend == "azure-databricks"

    def test_merge_onprem_spark_first_wins(self):
        s1 = Settings(onprem_spark=OnPremSparkSettings(cost_per_vcpu_hour=0.05))
        s2 = Settings(onprem_spark=OnPremSparkSettings(cost_per_vcpu_hour=0.10))
        merged = Settings.merge(s1, s2)
        assert merged.onprem_spark.cost_per_vcpu_hour == 0.05
