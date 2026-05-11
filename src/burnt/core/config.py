"""Configuration management for burnt."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .enums import Severity


class LintSettings(BaseModel):
    """Settings for the lint / check subsystem."""

    select: list[str] = ["ALL"]
    extend_select: list[str] = []
    ignore: list[str] = []
    extend_ignore: list[str] = []
    fail_on: Severity = Severity.ERROR
    exclude: list[str] = []
    per_file_ignores: dict[str, list[str]] = {}


class CacheSettings(BaseModel):
    """Settings for the cache subsystem."""

    enabled: bool = True
    ttl_seconds: float = 3600.0


class WatchSettings(BaseModel):
    """Settings for the watch / monitoring subsystem."""

    tag_key: str | None = None
    drift_threshold: float = 0.25
    idle_threshold: float = 0.10
    budget: float | None = None
    days: int = 30
    warehouse_id: str | None = None


class AlertSettings(BaseModel):
    """Settings for alert dispatch."""

    slack: str | None = None
    teams: str | None = None
    webhook: str | None = None
    delta_table: str | None = None


class PricingSettings(BaseModel):
    """Settings for the pricing backend selection."""

    backend: str | None = None

    @field_validator("backend", mode="before")
    @classmethod
    def _validate_backend(cls, v: str | None) -> str | None:
        valid = {
            "azure-databricks",
            "aws-databricks",
            "gcp-databricks",
            "onprem-spark",
        }
        if v is not None and v not in valid:
            from burnt.core.exceptions import ConfigError

            raise ConfigError(
                f"Unknown pricing backend {v!r}. Valid: {', '.join(sorted(valid))}"
            )
        return v


class SystemTablesSettings(BaseModel):
    """System table paths for Databricks."""

    enabled: bool = True
    query_history: str = "system.query.history"
    billing_usage: str = "system.billing.usage"
    list_prices: str = "system.billing.list_prices"
    information_schema_tables: str = "system.information_schema.tables"
    compute_clusters: str = "system.compute.clusters"
    node_timeline: str = "system.compute.node_timeline"


class DatabricksSettings(BaseModel):
    """Settings for Databricks-specific configuration."""

    system_tables: SystemTablesSettings = SystemTablesSettings()


class OnPremSparkSettings(BaseModel):
    """Settings for the onprem-spark pricing backend."""

    cost_per_vcpu_hour: float | None = None
    cost_per_gb_hour: float | None = None
    cost_per_gb_shuffle: float | None = None


class AzureSettings(BaseModel):
    """Settings for Azure Databricks backend."""

    region: str = "eastus"
    subscription_id: str | None = None


class Settings(BaseSettings):
    """Application settings — loaded from env vars, then TOML config files."""

    model_config = SettingsConfigDict(
        env_prefix="BURNT_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    workspace_url: str | None = None
    token: str | None = None
    warehouse_id: str | None = None
    target_currency: str = "USD"
    pricing_source: str = "api"

    # Single-underscore env alias for pricing.backend (BURNT_PRICING_BACKEND)
    pricing_backend: str | None = None

    lint: LintSettings = LintSettings()
    cache: CacheSettings = CacheSettings()
    watch: WatchSettings = WatchSettings()
    alert: AlertSettings = AlertSettings()
    pricing: PricingSettings = PricingSettings()
    databricks: DatabricksSettings = DatabricksSettings()
    onprem_spark: OnPremSparkSettings = OnPremSparkSettings()
    azure_databricks: AzureSettings = AzureSettings()

    @model_validator(mode="after")
    def _sync_pricing_backend(self) -> Settings:
        """Propagate BURNT_PRICING_BACKEND (single _) into pricing.backend."""
        if self.pricing_backend is not None and self.pricing.backend is None:
            self.pricing = self.pricing.model_copy(
                update={"backend": self.pricing_backend}
            )
        return self

    @classmethod
    def from_toml(cls, path: Path) -> Settings:
        """Load settings from a TOML file.

        Supports both `.burnt.toml` (no prefix) and `pyproject.toml` (`[tool.burnt]` section).
        """
        import tomllib

        with path.open("rb") as f:
            data = tomllib.load(f)

        # pyproject.toml: look under [tool.burnt]
        if path.name == "pyproject.toml":
            section = data.get("tool", {}).get("burnt", {})
        else:
            # .burnt.toml: top-level keys
            section = data

        top_level = {
            k: v
            for k, v in section.items()
            if k
            not in (
                "lint",
                "cache",
                "watch",
                "alert",
                "pricing",
                "databricks",
                "onprem_spark",
                "azure_databricks",
            )
        }
        lint_data = section.get("lint", {})
        cache_data = section.get("cache", {})
        watch_data = section.get("watch", {})
        alert_data = section.get("alert", {})
        pricing_data = section.get("pricing", {})
        onprem_spark_data = section.get("onprem_spark", {})
        databricks_data = section.get("databricks", {})
        system_tables_data = databricks_data.get("system_tables", {})
        azure_databricks_data = section.get("azure_databricks", {})

        # TOML uses kebab-case; map to snake_case for pydantic
        lint_data = {k.replace("-", "_"): v for k, v in lint_data.items()}
        cache_data = {k.replace("-", "_"): v for k, v in cache_data.items()}
        watch_data = {k.replace("-", "_"): v for k, v in watch_data.items()}
        alert_data = {k.replace("-", "_"): v for k, v in alert_data.items()}
        pricing_data = {k.replace("-", "_"): v for k, v in pricing_data.items()}
        onprem_spark_data = {
            k.replace("-", "_"): v for k, v in onprem_spark_data.items()
        }
        system_tables_data = {
            k.replace("-", "_"): v for k, v in system_tables_data.items()
        }
        azure_databricks_data = {
            k.replace("-", "_"): v for k, v in azure_databricks_data.items()
        }
        top_level = {k.replace("-", "_"): v for k, v in top_level.items()}

        lint = LintSettings(**lint_data) if lint_data else LintSettings()
        cache = CacheSettings(**cache_data) if cache_data else CacheSettings()
        watch = WatchSettings(**watch_data) if watch_data else WatchSettings()
        alert = AlertSettings(**alert_data) if alert_data else AlertSettings()
        pricing = PricingSettings(**pricing_data) if pricing_data else PricingSettings()
        system_tables = (
            SystemTablesSettings(**system_tables_data)
            if system_tables_data
            else SystemTablesSettings()
        )
        databricks = DatabricksSettings(system_tables=system_tables)
        onprem_spark = (
            OnPremSparkSettings(**onprem_spark_data)
            if onprem_spark_data
            else OnPremSparkSettings()
        )
        azure_databricks = (
            AzureSettings(**azure_databricks_data)
            if azure_databricks_data
            else AzureSettings()
        )

        return cls(
            lint=lint,
            cache=cache,
            watch=watch,
            alert=alert,
            pricing=pricing,
            databricks=databricks,
            onprem_spark=onprem_spark,
            azure_databricks=azure_databricks,
            **top_level,
        )

    @classmethod
    def discover(
        cls, cwd: Path | None = None
    ) -> tuple[Path | None, Settings]:
        """Walk upward from cwd looking for a config file.

        Stops at git root or HOME. Returns (config_path, settings).
        Returns (None, Settings()) if no config found.

        Discovery order per directory:
          1. .burnt.toml  → use it, stop
          2. pyproject.toml with [tool.burnt]  → use it, stop
        """
        start = Path(cwd or Path.cwd()).resolve()
        home = Path.home().resolve()

        current = start
        while True:
            # Check .burnt.toml
            burnt_toml = current / ".burnt.toml"
            if burnt_toml.exists():
                return burnt_toml, cls.from_toml(burnt_toml)

            # Check pyproject.toml with [tool.burnt]
            pyproject = current / "pyproject.toml"
            if pyproject.exists() and cls._has_tool_burnt(pyproject):
                return pyproject, cls.from_toml(pyproject)

            # Stop at git root
            if (current / ".git").exists():
                break

            # Stop at HOME
            if current == home:
                break

            # Stop at filesystem root
            parent = current.parent
            if parent == current:
                break

            current = parent

        return None, cls()

    @classmethod
    def _has_tool_burnt(cls, path: Path) -> bool:
        """Return True if pyproject.toml contains a [tool.burnt] section."""
        try:
            import tomllib

            with path.open("rb") as f:
                data = tomllib.load(f)
            return bool(data.get("tool", {}).get("burnt"))
        except Exception:
            return False

    @classmethod
    def merge(cls, *settings: Settings) -> Settings:
        """Merge multiple Settings instances; first arg wins per field.

        For nested models (lint, cache), the first non-default value wins
        per sub-field.
        """
        if not settings:
            return cls()
        if len(settings) == 1:
            return settings[0]

        defaults = cls()

        merged: dict = {}
        for field_name in cls.model_fields:
            for s in settings:
                val = getattr(s, field_name)
                default_val = getattr(defaults, field_name)
                if val != default_val:
                    merged[field_name] = val
                    break
            else:
                merged[field_name] = default_val

        # For nested models, do field-level merge
        lint_merged = _merge_model(LintSettings, [s.lint for s in settings])
        cache_merged = _merge_model(CacheSettings, [s.cache for s in settings])
        watch_merged = _merge_model(WatchSettings, [s.watch for s in settings])
        alert_merged = _merge_model(AlertSettings, [s.alert for s in settings])
        pricing_merged = _merge_model(PricingSettings, [s.pricing for s in settings])
        databricks_merged = _merge_model(
            DatabricksSettings, [s.databricks for s in settings]
        )
        onprem_spark_merged = _merge_model(
            OnPremSparkSettings, [s.onprem_spark for s in settings]
        )
        azure_databricks_merged = _merge_model(
            AzureSettings, [s.azure_databricks for s in settings]
        )
        merged["lint"] = lint_merged
        merged["cache"] = cache_merged
        merged["watch"] = watch_merged
        merged["alert"] = alert_merged
        merged["pricing"] = pricing_merged
        merged["databricks"] = databricks_merged
        merged["onprem_spark"] = onprem_spark_merged
        merged["azure_databricks"] = azure_databricks_merged

        return cls(**merged)


def _merge_model(model_cls: type[BaseModel], instances: list[BaseModel]) -> BaseModel:
    """Merge pydantic model instances; first non-default value per field wins."""
    defaults = model_cls()
    merged: dict = {}
    for field_name in model_cls.model_fields:
        default_val = getattr(defaults, field_name)
        for inst in instances:
            val = getattr(inst, field_name)
            if val != default_val:
                merged[field_name] = val
                break
        else:
            merged[field_name] = default_val
    return model_cls(**merged)
