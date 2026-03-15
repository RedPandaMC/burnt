"""Pydantic models for burnt."""

from __future__ import annotations

from decimal import Decimal  # noqa: TC003 — used in pydantic field type
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, PrivateAttr, field_validator

if TYPE_CHECKING:
    from .estimators.simulation import Simulation


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


VALID_SKUS = {
    "ALL_PURPOSE",
    "JOBS_COMPUTE",
    "SERVERLESS_JOBS",
    "SERVERLESS_NOTEBOOKS",
    "SQL_CLASSIC",
    "SQL_PRO",
    "SQL_SERVERLESS",
    "DLT_CORE",
    "DLT_PRO",
    "DLT_ADVANCED",
}


class ClusterConfig(BaseModel):
    """Databricks cluster configuration."""

    model_config = ConfigDict(frozen=True)
    instance_type: str = "Standard_DS3_v2"
    num_workers: int = 2
    dbu_per_hour: float = 0.75
    photon_enabled: bool = False
    sku: str = "ALL_PURPOSE"
    spot_policy: Literal["ON_DEMAND", "SPOT_WITH_ON_DEMAND_FALLBACK", "SPOT"] = (
        "ON_DEMAND"
    )
    autoscale_min_workers: int | None = None
    autoscale_max_workers: int | None = None

    @field_validator("sku")
    @classmethod
    def validate_sku(cls, v: str) -> str:
        if v not in VALID_SKUS:
            raise ValueError(f"Invalid SKU: {v}. Must be one of: {VALID_SKUS}")
        return v

    def to_api_json(self, spark_version: str = "15.4.x-scala2.12") -> dict:
        """Return Databricks Jobs API-compatible cluster definition."""
        cluster = {
            "spark_version": spark_version,
            "node_type_id": self.instance_type,
            "num_workers": self.num_workers,
            "spark_conf": {},
            "azure_attributes": {
                "availability": self.spot_policy,
            },
        }
        if (
            self.autoscale_min_workers is not None
            and self.autoscale_max_workers is not None
        ):
            cluster["autoscale"] = {
                "min_workers": self.autoscale_min_workers,
                "max_workers": self.autoscale_max_workers,
            }
        return {"new_cluster": cluster}


class PricingInfo(BaseModel):
    """Pricing information for a SKU."""

    sku_name: str
    dbu_rate: float
    cloud: str = "AZURE"
    region: str = "EAST_US"


class CostEstimate(BaseModel):
    """Cost estimate for a query or workload."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    estimated_dbu: float
    estimated_cost_usd: float | None = None
    estimated_cost_eur: float | None = None
    confidence: Literal["low", "medium", "high"] = "low"
    breakdown: dict[str, float] = {}
    warnings: list[str] = []
    _cluster: ClusterConfig | None = PrivateAttr(default=None)

    def simulate(self) -> Simulation:
        """Start a simulation scenario builder from this estimate."""
        from ..estimators.simulation import Simulation

        return Simulation(self, self._cluster)

    def display(self) -> None:
        """Render a rich display of this estimate. Implemented in s2-06."""
        raise NotImplementedError("display() will be implemented in s2-06")

    def raise_if_exceeds(self, budget_usd: float, label: str = "") -> None:
        """Raise CostBudgetExceeded if cost exceeds budget. Implemented in s2-07."""
        raise NotImplementedError("raise_if_exceeds() will be implemented in s2-07")


class ClusterRecommendation(BaseModel):
    """Three-tier cluster recommendation for optimization."""

    economy: ClusterConfig
    balanced: ClusterConfig
    performance: ClusterConfig
    current_cost_usd: float
    rationale: str

    def comparison_table(self) -> str:
        """Generate ASCII comparison table."""
        lines = [
            "Cluster Recommendation Comparison",
            f"{'Tier':<12} {'Instance':<20} {'Workers':<8} {'DBU/hr':<10} {'Est. Cost':<12}",
            "-" * 62,
            f"{'Economy':<12} {self.economy.instance_type:<20} {self.economy.num_workers:<8} {self.economy.dbu_per_hour:<10.2f} {self.economy.dbu_per_hour * 1.0:<12.2f}",
            f"{'Balanced':<12} {self.balanced.instance_type:<20} {self.balanced.num_workers:<8} {self.balanced.dbu_per_hour:<10.2f} {self.balanced.dbu_per_hour * 1.5:<12.2f}",
            f"{'Performance':<12} {self.performance.instance_type:<20} {self.performance.num_workers:<8} {self.performance.dbu_per_hour:<10.2f} {self.performance.dbu_per_hour * 2.0:<12.2f}",
            "",
            f"Rationale: {self.rationale}",
        ]
        return "\n".join(lines)

    def to_api_json(self) -> dict:
        """Return the balanced cluster as Databricks Jobs API-compatible JSON."""
        return self.balanced.to_api_json()


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


class AggregatedMetrics(BaseModel):
    """Aggregated metrics from multiple job runs."""

    job_id: str
    num_runs: int
    avg_duration_ms: float
    avg_peak_memory_pct: float
    avg_peak_cpu_pct: float
    max_spill_bytes: int
    duration_variability_pct: float
    memory_variability_pct: float
    last_run_metrics: dict[str, Any] = {}


class SimulationModification(BaseModel):
    """A single modification applied in a simulation scenario."""

    name: str
    cost_multiplier: float
    is_verified: bool = False
    rationale: str
    trade_offs: list[str] = []


class SimulationResult(BaseModel):
    """Result of comparing original vs projected cost after modifications."""

    original: CostEstimate
    projected: CostEstimate
    modifications: list[SimulationModification]
    total_savings_pct: float
    recommended_cluster: ClusterConfig | None = None

    def summary(self) -> str:
        """One-line summary description."""
        original_cost = self.original.estimated_cost_usd or 0
        projected_cost = self.projected.estimated_cost_usd or 0
        return (
            f"{', '.join(m.name for m in self.modifications)}: "
            f"${original_cost:.2f} → ${projected_cost:.2f} ({self.total_savings_pct:+.1f}%)"
        )

    def comparison_table(self) -> str:
        """Generate ASCII comparison table."""
        original_cost = self.original.estimated_cost_usd or 0
        projected_cost = self.projected.estimated_cost_usd or 0

        lines = [
            "Simulation Comparison",
            f"{'Metric':<20} {'Original':<15} {'Projected':<15}",
            "-" * 50,
            f"{'DBU':<20} {self.original.estimated_dbu:<15.2f} {self.projected.estimated_dbu:<15.2f}",
            f"{'Cost (USD)':<20} {original_cost:<15.2f} {projected_cost:<15.2f}",
            f"{'Savings %':<20} {'—':<15} {self.total_savings_pct:<15.1f}",
            "",
            "Modifications:",
        ]
        for mod in self.modifications:
            verified = "✓" if mod.is_verified else "≈"
            lines.append(
                f"  {verified} {mod.name}: {mod.cost_multiplier:.2f}x - {mod.rationale}"
            )

        return "\n".join(lines)

    def get_verified_multipliers(self) -> list[str]:
        """Get list of verified modification names."""
        return [m.name for m in self.modifications if m.is_verified]

    def get_estimated_multipliers(self) -> list[str]:
        """Get list of estimated modification names."""
        return [m.name for m in self.modifications if not m.is_verified]


class MultiSimulationResult(BaseModel):
    """Result of comparing multiple simulation scenarios."""

    scenarios: list[tuple[str, SimulationResult]]

    def get_results(self) -> list[SimulationResult]:
        """Get list of SimulationResult objects."""
        return [r for _, r in self.scenarios]

    def comparison_table(self) -> str:
        """Generate ASCII comparison table for all scenarios."""
        if not self.scenarios:
            return "No scenarios to compare."

        lines = [
            "Scenario Comparison",
            f"{'Scenario':<20} {'Cost (USD)':<15} {'Savings %':<15}",
            "-" * 50,
        ]

        for name, result in self.scenarios:
            cost = result.projected.estimated_cost_usd or 0
            lines.append(f"{name:<20} {cost:<15.2f} {result.total_savings_pct:<15.1f}")

        return "\n".join(lines)
