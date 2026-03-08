"""Pydantic models for dburnrate."""

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator


class OperationInfo(BaseModel):
    """Information about a database operation."""

    name: str
    kind: str
    weight: float


class QueryProfile(BaseModel):
    """Profile of a SQL query with complexity analysis."""

    sql: str
    dialect: str = "databricks"
    operations: list[OperationInfo] = []
    tables: list[str] = []
    complexity_score: float = 0.0


VALID_SKUS = {"ALL_PURPOSE", "JOBS_COMPUTE", "SQL_ENDPOINT", "DLT", "SERVERLESS"}


class ClusterConfig(BaseModel):
    """Databricks cluster configuration."""

    model_config = ConfigDict(frozen=True)
    instance_type: str = "Standard_DS3_v2"
    num_workers: int = 2
    dbu_per_hour: float = 0.75
    photon_enabled: bool = False
    sku: str = "ALL_PURPOSE"

    @field_validator("sku")
    @classmethod
    def validate_sku(cls, v: str) -> str:
        if v not in VALID_SKUS:
            raise ValueError(f"Invalid SKU: {v}. Must be one of: {VALID_SKUS}")
        return v


class PricingInfo(BaseModel):
    """Pricing information for a SKU."""

    sku_name: str
    dbu_rate: float
    cloud: str = "AZURE"
    region: str = "EAST_US"


class CostEstimate(BaseModel):
    """Cost estimate for a query or workload."""

    estimated_dbu: float
    estimated_cost_usd: float | None = None
    estimated_cost_eur: float | None = None
    confidence: Literal["low", "medium", "high"] = "low"
    breakdown: dict[str, float] = {}
    warnings: list[str] = []


class ClusterRecommendation(BaseModel):
    """Recommendation for cluster optimization."""

    current_config: ClusterConfig
    recommended_config: ClusterConfig
    bottleneck: list[str] = []
    estimated_savings_pct: float
    confidence: Literal["low", "medium", "high"]
    reason: str


class UsageRecord(BaseModel):
    """A single DBU usage record from system.billing.usage."""

    account_id: str
    workspace_id: str
    sku_name: str
    cloud: str
    usage_start_time: str
    usage_end_time: str
    usage_quantity: Decimal
    usage_unit: str
    cluster_id: str | None = None
    warehouse_id: str | None = None


class QueryRecord(BaseModel):
    """A query execution record from system.query.history."""

    statement_id: str
    statement_text: str
    statement_type: str | None = None
    start_time: str
    end_time: str | None = None
    execution_duration_ms: int | None = None
    compilation_duration_ms: int | None = None
    read_bytes: int | None = None
    read_rows: int | None = None
    produced_rows: int | None = None
    written_bytes: int | None = None
    total_task_duration_ms: int | None = None
    warehouse_id: str | None = None
    cluster_id: str | None = None
    status: str = ""
    error_message: str | None = None


class DeltaTableInfo(BaseModel):
    """Metadata extracted from a Delta Lake table."""

    location: str
    total_size_bytes: int
    num_files: int
    num_records: int | None = None
    partition_columns: list[str] = []


class ExplainPlan(BaseModel):
    """Parsed representation of a Databricks EXPLAIN COST output."""

    total_size_bytes: int
    estimated_rows: int | None = None
    join_types: list[str] = []
    shuffle_count: int = 0
    plan_depth: int = 0
    stats_complete: bool = False
    raw_plan: str = ""
    operations: list[OperationInfo] = []
