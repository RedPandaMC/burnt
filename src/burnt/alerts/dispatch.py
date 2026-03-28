"""Alert dispatch to various channels."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import requests
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

    If all destination arguments are None, reads from [alert] in burnt.toml.

    Args:
        message: Alert message.
        severity: Alert severity (info, warning, error).
        slack: Slack incoming webhook URL.
        teams: Microsoft Teams webhook URL.
        webhook: Generic webhook URL receiving a JSON payload.
        delta_table: Delta table path for logging (e.g. "catalog.schema.alerts").

    Returns:
        AlertResult with per-channel status and any errors.
    """
    if not any([slack, teams, webhook, delta_table]):
        from ..core.config import Settings

        _, settings = Settings.discover()
        slack = settings.alert.slack
        teams = settings.alert.teams
        webhook = settings.alert.webhook
        delta_table = settings.alert.delta_table

    result = AlertResult()

    if slack:
        try:
            _post_slack(slack, message, severity)
            result.slack_sent = True
        except Exception as exc:
            result.errors.append(f"Slack: {exc}")

    if teams:
        try:
            _post_teams(teams, message, severity)
            result.teams_sent = True
        except Exception as exc:
            result.errors.append(f"Teams: {exc}")

    if webhook:
        try:
            _post_webhook(webhook, message, severity)
            result.webhook_sent = True
        except Exception as exc:
            result.errors.append(f"Webhook: {exc}")

    if delta_table:
        try:
            _write_delta(delta_table, message, severity)
            result.delta_written = True
        except Exception as exc:
            result.errors.append(f"Delta: {exc}")

    return result


def _post_slack(url: str, message: str, severity: str) -> None:
    """POST a Block Kit message to a Slack incoming webhook."""
    _ICONS = {"error": ":rotating_light:", "warning": ":warning:", "info": ":information_source:"}
    icon = _ICONS.get(severity, ":bell:")
    payload = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{icon} *burnt alert* ({severity})\n{message}",
                },
            }
        ]
    }
    resp = requests.post(url, json=payload, timeout=10)
    resp.raise_for_status()


def _post_teams(url: str, message: str, severity: str) -> None:
    """POST a MessageCard to a Microsoft Teams incoming webhook."""
    _COLORS = {"error": "FF0000", "warning": "FFA500", "info": "0078D4"}
    payload: dict[str, Any] = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": _COLORS.get(severity, "0078D4"),
        "summary": f"burnt alert ({severity})",
        "sections": [{"text": message.replace("\n", "<br>")}],
    }
    resp = requests.post(url, json=payload, timeout=10)
    resp.raise_for_status()


def _post_webhook(url: str, message: str, severity: str) -> None:
    """POST a JSON payload to a generic webhook."""
    payload: dict[str, Any] = {
        "source": "burnt",
        "severity": severity,
        "message": message,
        "timestamp": datetime.now(UTC).isoformat(),
    }
    resp = requests.post(url, json=payload, timeout=10)
    resp.raise_for_status()


def _write_delta(table: str, message: str, severity: str) -> None:
    """Append an alert row to a Delta table using the active SparkSession."""
    try:
        from pyspark.sql import SparkSession  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError(
            "Delta alerts require pyspark. Install it or use a different alert destination."
        ) from exc

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession; cannot write Delta alert.")

    ts = datetime.now(UTC).isoformat()
    df = spark.createDataFrame(
        [{"source": "burnt", "severity": severity, "message": message, "timestamp": ts}]
    )
    df.write.format("delta").mode("append").saveAsTable(table)
