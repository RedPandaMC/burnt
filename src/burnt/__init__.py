"""
burnt - Pre-Orchestration FinOps & Cost Estimation for Databricks.

The Data Engineer's best friend for cost-aware linting, interactive cluster advising,
and programmatic pipeline cost estimation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .core.models import ClusterConfig, CostEstimate
from .estimators.pipeline import EstimationPipeline
from .parsers.antipatterns import AntiPattern, detect_antipatterns

__version__ = "0.1.0"


def lint(source: str, language: str = "sql") -> list[AntiPattern]:
    """
    Detect expensive anti-patterns (CROSS JOIN, un-limited collects) in code.

    Args:
        source: The SQL or PySpark code to analyze.
        language: "sql" or "pyspark". Defaults to "sql".

    Returns:
        A list of AntiPattern objects detailing the issue and severity.
    """
    return detect_antipatterns(source, language)


def lint_file(file_path: str | Path) -> list[AntiPattern]:
    """
    Read a file and detect expensive anti-patterns.

    Args:
        file_path: Path to a .sql or .py file.

    Returns:
        A list of AntiPattern objects.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    source = path.read_text(encoding="utf-8")
    language = "pyspark" if path.suffix == ".py" else "sql"

    return lint(source, language)


def estimate(
    query: str, cluster: ClusterConfig | None = None, registry: Any | None = None
) -> CostEstimate:
    """
    Estimate the DBU cost of a SQL query without executing it.

    Args:
        query: The SQL query to estimate.
        cluster: Optional target ClusterConfig. Defaults to a standard DS3_v2 cluster.
        registry: Optional TableRegistry for enterprise governance views.

    Returns:
        A CostEstimate object containing predicted DBUs, dollar cost, and confidence level.
    """
    if cluster is None:
        cluster = ClusterConfig(
            instance_type="Standard_DS3_v2", num_workers=2, dbu_per_hour=1.5
        )

    pipeline = EstimationPipeline()
    return pipeline.estimate(query, cluster)


def estimate_file(
    file_path: str | Path,
    cluster: ClusterConfig | None = None,
    registry: Any | None = None,
) -> CostEstimate:
    """
    Estimate the DBU cost of a .sql file.

    Args:
        file_path: Path to the .sql file.
        cluster: Optional target ClusterConfig.
        registry: Optional TableRegistry.

    Returns:
        A CostEstimate object.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    source = path.read_text(encoding="utf-8")
    return estimate(source, cluster, registry)


def advise_current_session() -> Any:
    """
    Analyzes the queries recently executed in the active Databricks SparkSession
    and recommends an optimized production Jobs Cluster configuration.

    (Context-Aware "End of Notebook" Advisor)
    """
    raise NotImplementedError(
        "advise_current_session() is under active development. "
        "It will require the runtime backend to be fully integrated."
    )


__all__ = [
    "AntiPattern",
    "ClusterConfig",
    "CostEstimate",
    "advise_current_session",
    "estimate",
    "estimate_file",
    "lint",
    "lint_file",
]
