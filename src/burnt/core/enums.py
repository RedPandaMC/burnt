"""Domain enumerations for burnt.

All enums inherit from ``str`` so they serialise as plain strings in Pydantic
models, JSON output, and CLI comparisons — no migration needed for callers
that currently compare against raw string literals.
"""

from __future__ import annotations

import enum


class Severity(str, enum.Enum):
    """Finding severity level."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class Sku(str, enum.Enum):
    """Databricks compute SKU."""

    ALL_PURPOSE = "ALL_PURPOSE"
    JOBS_COMPUTE = "JOBS_COMPUTE"
    SERVERLESS_JOBS = "SERVERLESS_JOBS"
    SERVERLESS_NOTEBOOKS = "SERVERLESS_NOTEBOOKS"
    SQL_CLASSIC = "SQL_CLASSIC"
    SQL_PRO = "SQL_PRO"
    SQL_SERVERLESS = "SQL_SERVERLESS"
    DLT_CORE = "DLT_CORE"
    DLT_PRO = "DLT_PRO"
    DLT_ADVANCED = "DLT_ADVANCED"


class SpotPolicy(str, enum.Enum):
    """Cluster spot-instance policy."""

    ON_DEMAND = "ON_DEMAND"
    SPOT_WITH_FALLBACK = "SPOT_WITH_ON_DEMAND_FALLBACK"
    SPOT = "SPOT"


class NodeKind(str, enum.Enum):
    """Kind of operation in a cost graph node."""

    READ = "read"
    TRANSFORM = "transform"
    SHUFFLE = "shuffle"
    ACTION = "action"
    WRITE = "write"
    UDF_CALL = "udf_call"
    MAINTENANCE = "maintenance"


class ScalingType(str, enum.Enum):
    """Cost-scaling behaviour of a graph node."""

    LINEAR = "linear"
    LINEAR_WITH_CLIFF = "linear_with_cliff"
    QUADRATIC = "quadratic"
    STEP_FAILURE = "step_failure"
    MAINTENANCE = "maintenance"


class EdgeType(str, enum.Enum):
    """Relationship type between two cost graph nodes."""

    DATAFLOW = "dataflow"
    CONTROL = "control"
    DEPENDENCY = "dependency"


class GraphMode(str, enum.Enum):
    """Source language / execution mode of a cost graph."""

    PYTHON = "python"
    SQL = "sql"
    DLT = "dlt"


class Confidence(str, enum.Enum):
    """Confidence level of a cost estimate or graph."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    NONE = "none"


class SqlDialect(str, enum.Enum):
    """SQL dialect used when parsing a query."""

    DATABRICKS = "databricks"
    SPARK = "spark"


class CloudProvider(str, enum.Enum):
    """Cloud provider hosting the Databricks workspace."""

    AZURE = "AZURE"
    AWS = "AWS"
    GCP = "GCP"
