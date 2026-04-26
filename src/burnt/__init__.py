"""
burnt - Performance coach for Spark data engineers.

Watches your practice runs, learns from Spark metrics,
and tells you how to ship cheaper code.
"""

from __future__ import annotations

from typing import Any, Literal

from .core.exceptions import (
    BurntError,
    ConfigError,
    CostBudgetExceeded,
    EstimationError,
    NotAvailableError,
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
    "EstimationError",
    "NotAvailableError",
    "ParseError",
    "PricingError",
    "check",
    "config",
    "start_session",
    "version",
    "watch",
]


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

_SESSION: Any = None


def start_session(
    *,
    capture_sql: bool = True,
    capture_stages: bool = True,
    capture_cells: bool = True,
) -> None:
    """Start listening to the active Spark session.

    Registers a SparkListener to capture stage metrics, SQL execution,
    and cell timings during your notebook session. Call this once at
    the top of your notebook, then run your code normally.

    Args:
        capture_sql: Capture SQL query text and duration.
        capture_stages: Capture stage-level metrics (shuffle, spill, etc.).
        capture_cells: Capture cell execution times.
    """
    global _SESSION
    from . import _session

    _SESSION = _session.start(
        capture_sql=capture_sql,
        capture_stages=capture_stages,
        capture_cells=capture_cells,
    )


def _get_session() -> Any:
    """Return the active session state, or None."""
    return _SESSION


# ---------------------------------------------------------------------------
# Check
# ---------------------------------------------------------------------------


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
    """Analyze code for cost anti-patterns and runtime performance.

    Combines static analysis (Rust engine) with runtime metrics (if
    start_session() was called) to produce actionable recommendations.

    Args:
        path: Path to a .py, .sql, or .ipynb file. Defaults to current directory.
        max_cost: Exit with error if estimated cost exceeds this amount.
        severity: Minimum severity to report (error, warning, info).
        skip: List of rule IDs to skip.
        only: List of rule IDs to run (exclusive with skip).
        cluster: Cluster config for cost estimation.
        json: Output results as JSON.
        markdown: Output results as Markdown.

    Returns:
        CheckResult with findings, graph, and optional runtime metrics.
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
        session=_SESSION,
    )


# ---------------------------------------------------------------------------
# Watch
# ---------------------------------------------------------------------------


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
    """Monitor Databricks workspace costs.

    Requires ``pip install burnt[databricks]``.
    """
    try:
        from burnt.databricks.watch.core import watch as _watch_impl
    except ImportError:
        raise NotAvailableError(
            "Workspace monitoring requires burnt[databricks]. "
            "Install with: pip install burnt[databricks]"
        ) from None

    return _watch_impl(
        tag_key=tag_key,
        drift_threshold=drift_threshold,
        idle_threshold=idle_threshold,
        budget=budget,
        days=days,
        job_id=job_id,
        pipeline_id=pipeline_id,
    )


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------


def version() -> str:
    """Return the current version of burnt."""
    return __version__
