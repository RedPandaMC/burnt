"""
burnt - Pre-Orchestration FinOps & Cost Estimation for Databricks.

CLI = static analysis. Zero credentials. Works offline. CI-friendly.
Python API = runtime cost intelligence. Requires Databricks credentials for live features.
"""

from __future__ import annotations

from typing import Any, Literal

from .core.exceptions import (
    BurntError,
    ConfigError,
    CostBudgetExceeded,
    DatabricksConnectionError,
    EstimationError,
    ParseError,
    PricingError,
)
from .core.models import CostEstimate

__version__ = "0.2.0"

__all__ = [
    "BurntError",
    "ConfigError",
    "CostBudgetExceeded",
    "CostEstimate",
    "DatabricksConnectionError",
    "EstimationError",
    "ParseError",
    "PricingError",
    "check",
    "config",
    "version",
    "watch",
]


def check(
    path: str | None = None,
    *,
    max_cost: float | None = None,
    severity: Literal["error", "warning", "info"] = "warning",
    skip: list[str] | None = None,
    only: list[str] | None = None,
    cluster: str | None = None,
    json: bool = False,
    markdown: bool = False,
) -> Any:
    """Analyze a notebook, Python file, or SQL file for cost and best practices.

    Args:
        path: Path to a .py, .sql, or .ipynb file. Defaults to current directory.
        max_cost: Exit with error if estimated cost exceeds this amount (USD).
        severity: Minimum severity to report (error, warning, info).
        skip: List of rule IDs to skip (e.g., ["BP001", "BNT-A01"]).
        only: List of rule IDs to run (exclusive with skip).
        cluster: Cluster config for cost estimation (DABs path or inline JSON).
        json: Output results as JSON.
        markdown: Output results as Markdown.

    Returns:
        CheckResult with findings, cost estimate, and graph.
    """
    from . import _check

    return _check.run(
        path=path,
        max_cost=max_cost,
        severity=severity,
        skip=skip,
        only=only,
        cluster=cluster,
        json=json,
        markdown=markdown,
    )


def watch(
    tag_key: str | None = None,
    *,
    drift_threshold: float = 0.25,
    idle_threshold: float = 0.10,
    budget: float | None = None,
    days: int = 30,
    job_id: int | None = None,
    pipeline_id: str | None = None,
) -> Any:
    """Monitor Databricks costs via system tables.

    Requires Databricks connectivity. Use in notebooks or scheduled jobs.

    Args:
        tag_key: Databricks tag to group costs by (e.g., "team", "project").
        drift_threshold: Alert if cost changes by more than this percentage.
        idle_threshold: Alert if cluster idle time exceeds this percentage.
        budget: Monthly budget for alerts.
        days: Number of days to look back.
        job_id: Filter to specific job.
        pipeline_id: Filter to specific DLT pipeline.

    Returns:
        WatchResult with cost metrics, idle detection, and drift analysis.
    """
    from . import _watch

    return _watch.run(
        tag_key=tag_key,
        drift_threshold=drift_threshold,
        idle_threshold=idle_threshold,
        budget=budget,
        days=days,
        job_id=job_id,
        pipeline_id=pipeline_id,
    )


def config(
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
    """Configure burnt programmatically.

    These settings override config files but are overridden by CLI flags.

    Args:
        warehouse_id: SQL warehouse for queries.
        billing_table: Override for system.billing.usage table.
        skip: Rules to skip.
        max_cost: Default max cost threshold.
        severity: Default severity level.
        tag_key: Default tag key for watch().
        drift_threshold: Default drift threshold.
        idle_threshold: Default idle threshold.
        budget: Default budget.
        alert_slack: Default Slack webhook URL.
        alert_teams: Default Teams webhook URL.
        alert_webhook: Generic webhook URL.
        calibration_store: Where to store calibration data ("local" or "delta:...").
    """
    from . import _config

    _config.set(
        warehouse_id=warehouse_id,
        billing_table=billing_table,
        skip=skip,
        max_cost=max_cost,
        severity=severity,
        tag_key=tag_key,
        drift_threshold=drift_threshold,
        idle_threshold=idle_threshold,
        budget=budget,
        alert_slack=alert_slack,
        alert_teams=alert_teams,
        alert_webhook=alert_webhook,
        calibration_store=calibration_store,
    )


def version() -> str:
    """Return the current version of burnt."""
    return __version__
