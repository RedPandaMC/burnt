"""Configuration management for burnt."""

from dataclasses import dataclass
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings from environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="BURNT_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    workspace_url: str | None = None
    token: str | None = None
    target_currency: str = "USD"
    pricing_source: str = "embedded"

    @classmethod
    def from_toml(cls, path: Path) -> "Settings":
        """Load settings from a TOML file."""
        try:
            import tomli
        except ImportError:
            import tomllib

            with open(path, "rb") as f:
                data = tomllib.load(f)
            return cls(**data.get("burnt", {}))
        else:
            with open(path, "rb") as f:
                data = tomli.load(f)
            return cls(**data.get("burnt", {}))


@dataclass(frozen=True)
class Config:
    """Programmatic configuration for burnt."""

    workspace_url: str | None = None
    token: str | None = None
    target_currency: str = "USD"
    pricing_source: str = "embedded"

    def to_settings(self) -> Settings:
        """Convert Config to Settings."""
        return Settings(
            workspace_url=self.workspace_url,
            token=self.token,
            target_currency=self.target_currency,
            pricing_source=self.pricing_source,
        )
