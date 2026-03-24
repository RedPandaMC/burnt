"""burnt.config() - Programmatic configuration."""

from __future__ import annotations

from typing import Any

_settings: dict[str, Any] = {}


def set(
    warehouse_id: str | None = None,
    billing_table: str | None = None,
    skip: list[str] | None = None,
    max_cost: float | None = None,
    severity: str | None = None,
    tag_key: str | None = None,
    drift_threshold: float | None = None,
    idle_threshold: float | None = None,
    budget: float | None = None,
    alert_slack: str | None = None,
    alert_teams: str | None = None,
    alert_webhook: str | None = None,
    calibration_store: str | None = None,
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
    if tag_key is not None:
        _settings["tag_key"] = tag_key
    if drift_threshold is not None:
        _settings["drift_threshold"] = drift_threshold
    if idle_threshold is not None:
        _settings["idle_threshold"] = idle_threshold
    if budget is not None:
        _settings["budget"] = budget
    if alert_slack is not None:
        _settings["alert_slack"] = alert_slack
    if alert_teams is not None:
        _settings["alert_teams"] = alert_teams
    if alert_webhook is not None:
        _settings["alert_webhook"] = alert_webhook
    if calibration_store is not None:
        _settings["calibration_store"] = calibration_store


def get(key: str, default: Any = None) -> Any:
    """Get a configuration value."""
    return _settings.get(key, default)


def clear() -> None:
    """Clear all programmatic settings."""
    global _settings
    _settings = {}


__all__ = ["clear", "get", "set"]
