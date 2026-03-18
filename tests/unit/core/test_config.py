import tempfile
from pathlib import Path

from burnt.core.config import CacheSettings, LintSettings, Settings


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
