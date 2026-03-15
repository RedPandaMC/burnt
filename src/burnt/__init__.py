"""
burnt - Pre-Orchestration FinOps & Cost Estimation for Databricks.

CLI = static analysis. Zero credentials. Works offline. CI-friendly.
Python API = runtime cost intelligence. Requires Databricks credentials for live features.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from .core.exceptions import CostBudgetExceeded
from .core.models import (
    ClusterConfig,
    ClusterRecommendation,
    CostEstimate,
    MultiSimulationResult,
    SimulationModification,
    SimulationResult,
)
from .estimators.simulation import Simulation
from .parsers.antipatterns import AntiPattern

__version__ = "0.1.0"


def estimate(
    query: str | Path,
    *,
    cluster: ClusterConfig | None = None,
    sku: str = "ALL_PURPOSE",
    currency: Literal["USD", "EUR"] = "USD",
    language: Literal["sql", "python", "auto"] | None = None,
    registry: Any | None = None,
) -> CostEstimate:
    """
    Estimate the DBU cost of a SQL query, Python file, or notebook.

    Args:
        query: SQL string, Python source string, or path to a .sql/.py/.ipynb/.dbc file.
               Pass a Path object or a string ending in a known extension to load from disk.
        cluster: Optional target ClusterConfig. Defaults to a standard DS3_v2 cluster.
        sku: Databricks SKU (ALL_PURPOSE, JOBS_COMPUTE, etc.).
        currency: Output currency. USD or EUR.
        language: Force language detection. None = auto.
        registry: Optional TableRegistry for enterprise governance views.

    Returns:
        A CostEstimate object containing predicted DBUs, dollar cost, and confidence level.
    """
    from .estimators.pipeline import EstimationPipeline

    # Resolve path vs inline source
    _FILE_EXTENSIONS = {".sql", ".py", ".ipynb", ".dbc"}
    source: str

    if isinstance(query, Path):
        source = _load_file(query)
    elif (
        isinstance(query, str)
        and Path(query).suffix in _FILE_EXTENSIONS
        and Path(query).exists()
    ):
        source = _load_file(Path(query))
    else:
        source = str(query)

    if cluster is None:
        cluster = ClusterConfig(
            instance_type="Standard_DS3_v2", num_workers=2, dbu_per_hour=0.75, sku=sku
        )

    pipeline = EstimationPipeline()
    result = pipeline.estimate(source, cluster)

    if currency != "USD" and result.estimated_cost_usd:
        from datetime import date
        from decimal import Decimal

        from .core.exchange import FrankfurterProvider

        exchange = FrankfurterProvider()
        converted = round(
            float(
                exchange.get_rate_for_amount(
                    Decimal(str(result.estimated_cost_usd)),
                    date.today(),
                    "USD",
                    currency,
                )
            ),
            4,
        )
        result = CostEstimate(
            estimated_dbu=result.estimated_dbu,
            estimated_cost_usd=result.estimated_cost_usd,
            estimated_cost_eur=converted if currency == "EUR" else None,
            confidence=result.confidence,
            breakdown=result.breakdown,
            warnings=result.warnings,
        )

    return result


def _load_file(path: Path) -> str:
    """Load source text from a file, handling notebooks."""
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    if path.suffix == ".sql" or path.suffix == ".py":
        return path.read_text(encoding="utf-8")
    elif path.suffix == ".ipynb":
        from .parsers.notebooks import parse_notebook

        cells = parse_notebook(path)
        return "\n\n".join(c.source for c in cells)
    elif path.suffix == ".dbc":
        from .parsers.notebooks import parse_dbc

        cells = parse_dbc(path)
        return "\n\n".join(c.source for c in cells)
    else:
        return path.read_text(encoding="utf-8")


def advise(
    run_id: str | None = None,
    statement_id: str | None = None,
    job_id: str | None = None,
    job_name: str | None = None,
) -> Any:
    """
    Analyze a historical run and recommend optimized cluster configuration.

    With no arguments, analyzes the current SparkSession (requires Databricks runtime).

    Args:
        run_id: Databricks Job Run ID to analyze.
        statement_id: SQL statement ID from query history.
        job_id: Job ID for analyzing multiple runs.
        job_name: Job name to analyze (looks up job ID first).

    Returns:
        AdvisoryReport with cost comparisons and cluster recommendation.
    """
    from .advisor.session import advise as _advise

    return _advise(
        run_id=run_id, statement_id=statement_id, job_id=job_id, job_name=job_name
    )


def right_size(profile: Any) -> Any:
    """
    Right-size cluster configuration based on workload profile.

    Args:
        profile: WorkloadProfile with memory, CPU, and data characteristics.

    Returns:
        ClusterConfig recommendation.
    """
    from .core.instances import get_cluster_config

    return get_cluster_config(profile)


__all__ = [
    "AntiPattern",
    "ClusterConfig",
    "ClusterRecommendation",
    "CostBudgetExceeded",
    "CostEstimate",
    "MultiSimulationResult",
    "Simulation",
    "SimulationModification",
    "SimulationResult",
    "WorkloadProfile",
    "advise",
    "estimate",
    "get_default_currency",
    "right_size",
    "set_default_currency",
]

_default_currency: str = "USD"

# WorkloadProfile imported lazily to avoid heavy dependency at module level
try:
    from .core.instances import WorkloadProfile
except ImportError:
    WorkloadProfile = None  # type: ignore[assignment, misc]


def set_default_currency(currency: str) -> None:
    """Set the default currency for cost budget checks.

    Args:
        currency: Currency code (USD, EUR, etc.)
    """
    global _default_currency
    _default_currency = currency.upper()


def get_default_currency() -> str:
    """Get the current default currency for cost budget checks.

    Returns:
        Currency code (defaults to USD)
    """
    return _default_currency
