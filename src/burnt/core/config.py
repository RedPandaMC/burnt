"""Configuration management for burnt."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class LintSettings(BaseModel):
    """Settings for the lint / check subsystem."""

    select: list[str] = ["ALL"]
    ignore: list[str] = []
    fail_on: str = "error"  # info | warning | error
    exclude: list[str] = []
    per_file_ignores: dict[str, list[str]] = {}


class CacheSettings(BaseModel):
    """Settings for the cache subsystem."""

    enabled: bool = True
    ttl_seconds: float = 3600.0


class Settings(BaseSettings):
    """Application settings — loaded from env vars, then TOML config files."""

    model_config = SettingsConfigDict(
        env_prefix="BURNT_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    workspace_url: str | None = None
    token: str | None = None
    target_currency: str = "USD"
    pricing_source: str = "api"
    lint: LintSettings = LintSettings()
    cache: CacheSettings = CacheSettings()

    @classmethod
    def from_toml(cls, path: Path) -> Settings:
        """Load settings from a TOML file.

        Supports both `.burnt.toml` (no prefix) and `pyproject.toml` (`[tool.burnt]` section).
        """
        import tomllib

        with open(path, "rb") as f:
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
            if k not in ("lint", "cache")
        }
        lint_data = section.get("lint", {})
        cache_data = section.get("cache", {})

        # TOML uses kebab-case; map to snake_case for pydantic
        lint_data = {k.replace("-", "_"): v for k, v in lint_data.items()}
        cache_data = {k.replace("-", "_"): v for k, v in cache_data.items()}
        top_level = {k.replace("-", "_"): v for k, v in top_level.items()}

        lint = LintSettings(**lint_data) if lint_data else LintSettings()
        cache = CacheSettings(**cache_data) if cache_data else CacheSettings()

        return cls(lint=lint, cache=cache, **top_level)

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

            with open(path, "rb") as f:
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
        lint_merged = _merge_model(
            LintSettings,
            [s.lint for s in settings],
        )
        cache_merged = _merge_model(
            CacheSettings,
            [s.cache for s in settings],
        )
        merged["lint"] = lint_merged
        merged["cache"] = cache_merged

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
