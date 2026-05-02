"""
burnt — static cost analyzer for Spark.

Parses Python, SQL, and notebook source with a Rust engine (tree-sitter + CostGraph),
applies 43 lint rules, and produces actionable findings ranked by cost impact.

Three modes (auto-detected):
- Static lint: zero credentials, zero Spark connection required.
- In-notebook: attach to a live Spark session via the REST monitoring API for
  per-stage metric enrichment.
- CI gate: emit SARIF / JSON and block on --max-cost or --fail-on.
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
]


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

_SESSION: Any = None


def start_session() -> None:
    """Attach to the active Spark session for runtime metric enrichment.

    Resolves the Spark monitoring REST endpoint (driver-proxy-api on Databricks,
    or uiWebUrl on generic Spark) and records it for use at check() time.
    If no Spark session is active, returns silently — subsequent check() calls
    run in static-only mode.
    """
    global _SESSION
    from ._session import start

    _SESSION = start()


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
    start_session() was called) to produce findings ranked by cost impact.

    Args:
        path: Path to a .py, .sql, .ipynb, or .dbc file. Defaults to current directory.
        max_cost: Raise CostBudgetExceeded if estimated cost exceeds this amount.
        severity: Minimum severity to report (error, warning, info).
        skip: Rule IDs or prefixes to skip (e.g. ["BP008", "BNT*"]).
        only: Run only these rule IDs (exclusive with skip).
        cluster: Cluster config identifier for cost estimation.
        json: Output results as JSON.
        markdown: Output results as Markdown.

    Returns:
        CheckResult with findings, graph, and optional runtime metrics.
    """
    from . import _check
    from ._session import collect

    if _SESSION is not None and _SESSION.active:
        collect(_SESSION)

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
# Config
# ---------------------------------------------------------------------------


def config(
    warehouse_id: str | None = None,
    billing_table: str | None = None,
    skip: list[str] | None = None,
    max_cost: float | None = None,
    severity: str | None = None,
) -> None:
    """Configure burnt programmatically.

    These settings override config files but are overridden by CLI flags.
    For full configuration options use burnt.toml (see docs/configuration.md).

    Args:
        warehouse_id: Databricks SQL warehouse ID for system-table queries.
        billing_table: Override path for system.billing.usage.
        skip: Rule IDs or prefixes to suppress globally.
        max_cost: Default cost gate (same as --max-cost).
        severity: Minimum severity to report.
    """
    from . import _config

    _config.set(
        warehouse_id=warehouse_id,
        billing_table=billing_table,
        skip=skip,
        max_cost=max_cost,
        severity=severity,
    )


# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------


def version() -> str:
    """Return the current version of burnt."""
    return __version__
