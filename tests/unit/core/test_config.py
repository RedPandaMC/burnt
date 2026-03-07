import tempfile
from pathlib import Path

import pytest

from dburnrate.core.config import Config, Settings


class TestSettings:
    def test_settings_default_values(self):
        settings = Settings()
        assert settings.workspace_url is None
        assert settings.token is None
        assert settings.target_currency == "USD"
        assert settings.pricing_source == "embedded"

    def test_settings_from_env_vars(self, monkeypatch):
        monkeypatch.setenv(
            "DBURNRATE_WORKSPACE_URL", "https://example.cloud.databricks.com"
        )
        monkeypatch.setenv("DBURNRATE_TOKEN", "test_token")
        monkeypatch.setenv("DBURNRATE_TARGET_CURRENCY", "EUR")

        settings = Settings()
        assert settings.workspace_url == "https://example.cloud.databricks.com"
        assert settings.token == "test_token"
        assert settings.target_currency == "EUR"

    def test_settings_from_toml(self, monkeypatch):
        monkeypatch.delenv("DBURNRATE_WORKSPACE_URL", raising=False)
        monkeypatch.delenv("DBURNRATE_TOKEN", raising=False)
        monkeypatch.delenv("DBURNRATE_TARGET_CURRENCY", raising=False)

        toml_content = """
[dburnrate]
workspace_url = "https://test.cloud.databricks.com"
token = "toml_token"
target_currency = "GBP"
pricing_source = "live"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(toml_content)
            f.flush()
            path = Path(f.name)

        try:
            settings = Settings.from_toml(path)
            assert settings.workspace_url == "https://test.cloud.databricks.com"
            assert settings.token == "toml_token"
            assert settings.target_currency == "GBP"
            assert settings.pricing_source == "live"
        finally:
            path.unlink()


class TestConfig:
    def test_config_creation(self):
        config = Config(
            workspace_url="https://example.cloud.databricks.com",
            token="test_token",
            target_currency="EUR",
            pricing_source="live",
        )
        assert config.workspace_url == "https://example.cloud.databricks.com"
        assert config.token == "test_token"
        assert config.target_currency == "EUR"
        assert config.pricing_source == "live"

    def test_config_default_values(self):
        config = Config()
        assert config.workspace_url is None
        assert config.token is None
        assert config.target_currency == "USD"
        assert config.pricing_source == "embedded"

    def test_config_is_frozen(self):
        from dataclasses import FrozenInstanceError

        config = Config(token="test")
        with pytest.raises(FrozenInstanceError):
            config.token = "new_token"

    def test_config_to_settings(self):
        config = Config(
            workspace_url="https://example.cloud.databricks.com",
            token="test_token",
            target_currency="EUR",
        )
        settings = config.to_settings()
        assert settings.workspace_url == "https://example.cloud.databricks.com"
        assert settings.token == "test_token"
        assert settings.target_currency == "EUR"
