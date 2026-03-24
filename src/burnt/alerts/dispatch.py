"""Alert dispatch to various channels."""

from __future__ import annotations

from pydantic import BaseModel


class AlertResult(BaseModel):
    """Result of sending alerts."""

    slack_sent: bool = False
    teams_sent: bool = False
    webhook_sent: bool = False
    delta_written: bool = False
    errors: list[str] = []


def dispatch(
    message: str,
    *,
    severity: str = "warning",
    slack: str | None = None,
    teams: str | None = None,
    webhook: str | None = None,
    delta_table: str | None = None,
) -> AlertResult:
    """Dispatch alerts to configured channels.

    Args:
        message: Alert message.
        severity: Alert severity (info, warning, error).
        slack: Slack webhook URL.
        teams: Microsoft Teams webhook URL.
        webhook: Generic webhook URL.
        delta_table: Delta table path for logging (e.g., "catalog.schema.alerts").

    Returns:
        Result of sending alerts.
    """
    raise NotImplementedError(
        "Alerts require additional dependencies. "
        "Install with: pip install burnt[alerts]"
    )
