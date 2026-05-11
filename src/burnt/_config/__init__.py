"""burnt.config() - Programmatic configuration."""

from __future__ import annotations

from typing import Any

_settings: dict[str, Any] = {}


def configure(
    warehouse_id: str | None = None,
    billing_table: str | None = None,
    skip: list[str] | None = None,
    max_cost: float | None = None,
    severity: str | None = None,
) -> None:
    """Set configuration programmatically."""
    global _settings

    if warehouse_id is not None:
        _settings["warehouse_id"] = warehouse_id
    if billing_table is not None:
        _settings["billing_table"] = billing_table
    if skip is not None:
        _settings["skip"] = skip
    if max_cost is not None:
        _settings["max_cost"] = max_cost
    if severity is not None:
        _settings["severity"] = severity


def get(key: str, default: Any = None) -> Any:
    """Get a configuration value."""
    return _settings.get(key, default)


def clear() -> None:
    """Clear all programmatic settings."""
    global _settings
    _settings = {}


__all__ = ["clear", "configure", "get"]
