"""Pydantic models for burnt."""

from __future__ import annotations

import logging
from decimal import Decimal  # noqa: TC003 — used in pydantic field type
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from burnt.core.exchange import ExchangeRateProvider

from pydantic import BaseModel, ConfigDict, PrivateAttr
from tabulate import tabulate

from ._display import _DisplayMixin
from .enums import CloudProvider, Confidence, Sku, SpotPolicy, SqlDialect

logger = logging.getLogger(__name__)


class OperationInfo(BaseModel):
    """Information about a database operation."""

    name: str
    kind: str
    weight: float


class QueryProfile(BaseModel):
    """Profile of a SQL query with complexity analysis."""

    sql: str
    dialect: SqlDialect = SqlDialect.DATABRICKS
    operations: list[OperationInfo] = []
    tables: list[str] = []
    complexity_score: float = 0.0


class ClusterConfig(BaseModel):
    """Databricks cluster configuration."""

    model_config = ConfigDict(frozen=True)
    instance_type: str = "Standard_DS3_v2"
    num_workers: int = 2
    dbu_per_hour: float = 0.75
    photon_enabled: bool = False
    sku: Sku = Sku.ALL_PURPOSE
    spot_policy: SpotPolicy = SpotPolicy.ON_DEMAND
    autoscale_min_workers: int | None = None
    autoscale_max_workers: int | None = None

    @classmethod
    def _lookup_dbu_rate(cls, node_type: str) -> float:
        from burnt.core.instances import (
            AZURE_INSTANCE_CATALOG,  # lazy — avoids circular import
        )

        if node_type in AZURE_INSTANCE_CATALOG:
            return AZURE_INSTANCE_CATALOG[node_type].dbu_rate
        logger.warning(
            "Unknown instance type %r; falling back to default DBU rate 0.75", node_type
        )
        return 0.75

    @classmethod
    def from_databricks_json(cls, payload: dict) -> ClusterConfig:
        """Parse a Databricks Jobs API new_cluster payload (or bare cluster dict) into a ClusterConfig."""
        cluster = payload.get("new_cluster", payload)
        node_type = cluster.get("node_type_id", "Standard_DS3_v2")
        dbu = cls._lookup_dbu_rate(node_type)
        spot_raw = cluster.get("azure_attributes", {}).get("availability", "ON_DEMAND")
        autoscale = cluster.get("autoscale", {})
        try:
            spot_policy_value = SpotPolicy(spot_raw)
        except ValueError:
            spot_policy_value = SpotPolicy.ON_DEMAND
        return cls(
            instance_type=node_type,
            num_workers=cluster.get("num_workers", autoscale.get("max_workers", 2)),
            dbu_per_hour=dbu,
            photon_enabled=(
                "photon" in cluster.get("spark_version", "").lower()
                or cluster.get("runtime_engine", "").upper() == "PHOTON"
            ),
            spot_policy=spot_policy_value,
            autoscale_min_workers=autoscale.get("min_workers"),
            autoscale_max_workers=autoscale.get("max_workers"),
        )

    def to_json(self, spark_version: str = "15.4.x-scala2.12") -> dict:
        """Return Databricks Jobs API-compatible cluster definition as dict."""
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

    def to_dab(self, name: str, spark_version: str = "15.4.x-scala2.12") -> str:
        """Return Databricks Asset Bundle YAML cluster definition.

        Args:
            name: The name for this cluster resource
            spark_version: The Spark runtime version to use

        Returns:
            YAML string with nested resources.clusters structure
        """
        import yaml

        cluster = {
            "node_type_id": self.instance_type,
            "num_workers": self.num_workers,
            "spark_version": spark_version,
            "runtime_engine": "PHOTON" if self.photon_enabled else "STANDARD",
        }
        if (
            self.autoscale_min_workers is not None
            and self.autoscale_max_workers is not None
        ):
            cluster["autoscale"] = {
                "min_workers": self.autoscale_min_workers,
                "max_workers": self.autoscale_max_workers,
            }
        dab_dict = {"resources": {"clusters": {name: cluster}}}
        return yaml.dump(dab_dict, default_flow_style=False, sort_keys=False)


class ClusterProfile(BaseModel):
    """Full cluster configuration including runtime context used by the estimation pipeline."""

    config: ClusterConfig
    driver_node_type: str | None = None
    spark_version: str | None = None
    custom_spark_conf: dict[str, str] = {}
    cluster_tags: dict[str, str] = {}
    instance_pool_id: str | None = None
    instance_pool_max_capacity: int | None = None
    cloud_provider: CloudProvider = CloudProvider.AZURE

    @classmethod
    def from_databricks_json(cls, payload: dict) -> ClusterProfile:
        """Populate both config (via ClusterConfig.from_databricks_json) and extended fields."""
        cluster = payload.get("new_cluster", payload)
        config = ClusterConfig.from_databricks_json(payload)
        return cls(
            config=config,
            driver_node_type=cluster.get("driver_node_type_id"),
            spark_version=cluster.get("spark_version"),
            custom_spark_conf=cluster.get("spark_conf", {}),
            cluster_tags=cluster.get("custom_tags", {}),
            instance_pool_id=cluster.get("instance_pool_id"),
            instance_pool_max_capacity=cluster.get("instance_pool_max_capacity"),
            cloud_provider="AZURE",
        )


class PricingInfo(BaseModel):
    """Pricing information for a SKU."""

    sku_name: str
    dbu_rate: float
    cloud: str = "AZURE"
    region: str = "EAST_US"


class CostEstimate(BaseModel, _DisplayMixin):
    """Cost estimate for a query or workload.

    All currency amounts live in ``costs`` — a plain dict keyed by ISO 4217 code.
    Use ``cost_in`` for a direct lookup (no I/O) and ``convert_to`` for live
    conversion via an injectable ``ExchangeRateProvider``.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    estimated_dbu: float | None = None
    costs: dict[str, float] = {}
    confidence: Confidence = Confidence.LOW
    breakdown: dict[str, float] = {}
    warnings: list[str] = []
    _cluster: ClusterConfig | None = PrivateAttr(default=None)

    # ------------------------------------------------------------------
    # Currency access
    # ------------------------------------------------------------------

    def cost_in(self, currency: str) -> float | None:
        """Return the pre-computed cost in *currency*, or None if unavailable.

        Pure dict lookup — no network calls, no conversion.
        """
        return self.costs.get(currency.upper())

    def convert_to(
        self,
        currency: str,
        exchange: ExchangeRateProvider | None = None,
    ) -> float:
        """Return cost in *currency*, converting via *exchange* if necessary.

        Falls back to ``FrankfurterProvider`` (live Frankfurter API) when no
        provider is supplied. Prefers USD as the conversion base; falls back
        to the first available currency in ``costs``.

        Raises:
            ValueError: If ``costs`` is empty.
        """
        from datetime import date
        from decimal import Decimal

        from burnt.core.exchange import FrankfurterProvider

        code = currency.upper()
        direct = self.cost_in(code)
        if direct is not None:
            return direct

        if not self.costs:
            raise ValueError("No cost data available for conversion")

        provider = exchange if exchange is not None else FrankfurterProvider()
        base = "USD" if "USD" in self.costs else next(iter(self.costs))
        converted = provider.get_rate_for_amount(
            Decimal(str(self.costs[base])),
            date.today(),
            from_curr=base,
            to_curr=code,
        )
        return float(converted)

    @property
    def primary_cost(self) -> float | None:
        """Best available cost amount, preferring major currencies in priority order."""
        for code in ("USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF"):
            if code in self.costs:
                return self.costs[code]
        return next(iter(self.costs.values()), None)

    @property
    def primary_currency(self) -> str | None:
        """ISO 4217 code matching ``primary_cost``, or None if ``costs`` is empty."""
        for code in ("USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF"):
            if code in self.costs:
                return code
        return next(iter(self.costs), None)

    # ------------------------------------------------------------------
    # Budget guard
    # ------------------------------------------------------------------

    def raise_if_exceeds(
        self, budget: float, label: str = "", currency: str | None = None
    ) -> CostEstimate:
        """Raise ``CostBudgetExceeded`` if cost exceeds *budget*; return self otherwise.

        Args:
            budget: Budget amount in *currency*.
            label: Optional identifier surfaced in the exception message.
            currency: ISO 4217 code (defaults to ``"USD"``).

        Returns:
            self — chainable.

        Raises:
            CostBudgetExceeded: When the estimated cost exceeds *budget*.
        """
        import warnings as _warnings

        from burnt.core.exceptions import CostBudgetExceeded

        if currency is None:
            currency = "USD"

        if not self.costs:
            _warnings.warn(
                "Cannot check budget: no cost data available"
                + (f" for {label!r}" if label else ""),
                stacklevel=2,
            )
            return self

        try:
            estimate_cost = self.convert_to(currency)
        except Exception:
            _warnings.warn(
                "Cannot check budget: currency conversion failed"
                + (f" for {label!r}" if label else ""),
                stacklevel=2,
            )
            return self

        if estimate_cost > budget:
            raise CostBudgetExceeded(self, budget, label, currency=currency)

        return self

    # ------------------------------------------------------------------
    # Display
    # ------------------------------------------------------------------

    def comparison_table(self) -> str:
        """Generate ASCII comparison table."""
        lines = [
            "Cost Estimate",
            f"{'Field':<20} {'Value':<30}",
            "-" * 50,
        ]
        if self.estimated_dbu is not None:
            lines.append(f"{'Estimated DBU':<20} {self.estimated_dbu:<30.2f}")
        for code, amount in self.costs.items():
            lines.append(f"{'Cost (' + code + ')':<20} {amount:<30.2f}")
        lines.append(f"{'Confidence':<20} {self.confidence:<30}")

        if self.breakdown:
            lines.extend(["", "Breakdown:"])
            lines.extend(f"  {key}: {value:.2f}" for key, value in self.breakdown.items())

        if self.warnings:
            lines.extend(["", "Warnings:"])
            lines.extend(f"  ⚠ {warning}" for warning in self.warnings)

        if self.confidence == "none":
            lines.append("\nConnect to a workspace for cost estimates: burnt doctor")

        return "\n".join(lines)

    def to_markdown(self) -> str:
        """Return a GFM markdown table using tabulate."""
        rows = []
        if self.estimated_dbu is not None:
            rows.append(["Estimated DBU", f"{self.estimated_dbu:.2f}"])
        for code, amount in self.costs.items():
            rows.append([f"Cost ({code})", f"{amount:.2f}"])
        rows.append(["Confidence", self.confidence])

        md = tabulate(rows, headers=["Field", "Value"], tablefmt="github")

        if self.breakdown:
            md += "\n\n**Breakdown:**\n"
            for key, value in self.breakdown.items():
                md += f"- {key}: {value:.2f}\n"

        if self.warnings:
            md += "\n**Warnings:**\n"
            for warning in self.warnings:
                md += f"- ⚠ {warning}\n"

        return md

    def __str__(self) -> str:
        """Return string representation (comparison table)."""
        return self.comparison_table()

    def __repr__(self) -> str:
        """Return developer representation."""
        cost = self.primary_cost or 0
        currency = self.primary_currency or "USD"
        dbu = f"{self.estimated_dbu:.2f}" if self.estimated_dbu is not None else "N/A"
        return (
            f"CostEstimate(dbu={dbu}, cost={currency} {cost:.2f}, confidence={self.confidence})"
        )


class ClusterRecommendation(BaseModel, _DisplayMixin):
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

    def to_markdown(self) -> str:
        """Return a GFM markdown table using tabulate."""
        rows = [
            [
                "Economy",
                self.economy.instance_type,
                self.economy.num_workers,
                f"{self.economy.dbu_per_hour:.2f}",
                f"${self.economy.dbu_per_hour * 1.0:.2f}",
            ],
            [
                "Balanced",
                self.balanced.instance_type,
                self.balanced.num_workers,
                f"{self.balanced.dbu_per_hour:.2f}",
                f"${self.balanced.dbu_per_hour * 1.5:.2f}",
            ],
            [
                "Performance",
                self.performance.instance_type,
                self.performance.num_workers,
                f"{self.performance.dbu_per_hour:.2f}",
                f"${self.performance.dbu_per_hour * 2.0:.2f}",
            ],
        ]
        md = tabulate(
            rows,
            headers=["Tier", "Instance", "Workers", "DBU/hr", "Est. Cost"],
            tablefmt="github",
        )
        md += f"\n\n**Rationale:** {self.rationale}"
        return md

    def to_json(self) -> dict:
        """Return the balanced cluster as Databricks Jobs API-compatible dict."""
        return self.balanced.to_json()

    def to_dab(self, base_name: str = "recommended") -> str:
        """Return all three tiers as Databricks Asset Bundle YAML.

        Args:
            base_name: Base name for auto-generated cluster resource names

        Returns:
            YAML string with all three cluster definitions
        """
        import yaml

        spark_version = "15.4.x-scala2.12"
        clusters = {
            f"{base_name}_economy": self._cluster_to_dab_dict(
                self.economy, spark_version
            ),
            f"{base_name}_balanced": self._cluster_to_dab_dict(
                self.balanced, spark_version
            ),
            f"{base_name}_performance": self._cluster_to_dab_dict(
                self.performance, spark_version
            ),
        }
        dab_dict = {"resources": {"clusters": clusters}}
        return yaml.dump(dab_dict, default_flow_style=False, sort_keys=False)

    def _cluster_to_dab_dict(self, config: ClusterConfig, spark_version: str) -> dict:
        """Helper to convert ClusterConfig to DAB dict."""
        cluster = {
            "node_type_id": config.instance_type,
            "num_workers": config.num_workers,
            "spark_version": spark_version,
            "runtime_engine": "PHOTON" if config.photon_enabled else "STANDARD",
        }
        if (
            config.autoscale_min_workers is not None
            and config.autoscale_max_workers is not None
        ):
            cluster["autoscale"] = {
                "min_workers": config.autoscale_min_workers,
                "max_workers": config.autoscale_max_workers,
            }
        return cluster

    def __str__(self) -> str:
        """Return string representation (comparison table)."""
        return self.comparison_table()

    def __repr__(self) -> str:
        """Return developer representation."""
        return f"ClusterRecommendation(economy={self.economy.instance_type}, balanced={self.balanced.instance_type}, performance={self.performance.instance_type})"


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


class SimulationResult(BaseModel, _DisplayMixin):
    """Result of comparing original vs projected cost after modifications."""

    original: CostEstimate
    projected: CostEstimate
    modifications: list[SimulationModification]
    total_savings_pct: float
    recommended_cluster: ClusterConfig | None = None

    def summary(self) -> str:
        """One-line summary description."""
        original_cost = self.original.primary_cost or 0
        projected_cost = self.projected.primary_cost or 0
        currency = self.original.primary_currency or "USD"
        return (
            f"{', '.join(m.name for m in self.modifications)}: "
            f"{currency} {original_cost:.2f} → {currency} {projected_cost:.2f} ({self.total_savings_pct:+.1f}%)"
        )

    def comparison_table(self) -> str:
        """Generate ASCII comparison table."""
        original_cost = self.original.primary_cost or 0
        projected_cost = self.projected.primary_cost or 0
        currency = self.original.primary_currency or "USD"

        lines = [
            "Simulation Comparison",
            f"{'Metric':<20} {'Original':<15} {'Projected':<15} {'Δ':<10}",
            "-" * 60,
            f"{'DBU':<20} {self.original.estimated_dbu:<15.2f} {self.projected.estimated_dbu:<15.2f} {self.total_savings_pct:<10.1f}%",
            f"{'Cost (' + currency + ')':<20} {original_cost:<15.2f} {projected_cost:<15.2f} {self.total_savings_pct:<10.1f}%",
            "",
            "Modifications:",
        ]
        for mod in self.modifications:
            verified = "✓" if mod.is_verified else "≈"
            lines.append(
                f"  {verified} {mod.name}: {mod.cost_multiplier:.2f}x - {mod.rationale}"
            )

        return "\n".join(lines)

    def to_markdown(self) -> str:
        """Return a GFM markdown table using tabulate."""
        original_cost = self.original.primary_cost or 0
        projected_cost = self.projected.primary_cost or 0
        currency = self.original.primary_currency or "USD"

        rows = [
            [
                f"Cost ({currency})",
                f"{original_cost:.2f}",
                f"{projected_cost:.2f}",
                f"{self.total_savings_pct:.1f}%",
            ],
            [
                "DBU",
                f"{self.original.estimated_dbu:.1f}",
                f"{self.projected.estimated_dbu:.1f}",
                f"{self.total_savings_pct:.1f}%",
            ],
        ]
        md = tabulate(
            rows, headers=["", "Original", "Projected", "Δ"], tablefmt="github"
        )

        if self.modifications:
            md += "\n\n**Modifications:**\n"
            for mod in self.modifications:
                verified = "✓" if mod.is_verified else "≈"
                md += f"- {verified} **{mod.name}**: {mod.cost_multiplier:.2f}x - {mod.rationale}\n"

        return md

    def __str__(self) -> str:
        """Return string representation (comparison table)."""
        return self.comparison_table()

    def __repr__(self) -> str:
        """Return developer representation."""
        return f"SimulationResult(savings={self.total_savings_pct:.1f}%, mods={len(self.modifications)})"

    def get_verified_multipliers(self) -> list[str]:
        """Get list of verified modification names."""
        return [m.name for m in self.modifications if m.is_verified]

    def get_estimated_multipliers(self) -> list[str]:
        """Get list of estimated modification names."""
        return [m.name for m in self.modifications if not m.is_verified]


class MultiSimulationResult(BaseModel, _DisplayMixin):
    """Result of comparing multiple simulation scenarios."""

    scenarios: list[tuple[str, SimulationResult]]

    def get_results(self) -> list[SimulationResult]:
        """Get list of SimulationResult objects."""
        return [r for _, r in self.scenarios]

    def best(self) -> tuple[str, SimulationResult]:
        """Return the scenario with the lowest projected cost.

        In case of ties, prefers higher confidence levels (high > medium > low).

        Returns:
            Tuple of (scenario_name, SimulationResult)
        """
        if not self.scenarios:
            raise ValueError("No scenarios to compare")

        confidence_order = {"high": 3, "medium": 2, "low": 1}

        def sort_key(item: tuple[str, SimulationResult]) -> tuple[float, int]:
            _name, result = item
            cost = result.projected.primary_cost or float("inf")
            confidence_score = confidence_order.get(result.projected.confidence, 0)
            return (cost, -confidence_score)  # negative so higher confidence sorts first

        return min(self.scenarios, key=sort_key)

    def comparison_table(self) -> str:
        """Generate ASCII comparison table for all scenarios."""
        if not self.scenarios:
            return "No scenarios to compare."

        first_currency = self.scenarios[0][1].projected.primary_currency or "USD"
        lines = [
            "Scenario Comparison",
            f"{'Scenario':<20} {'Cost (' + first_currency + ')':<15} {'vs Baseline':<15} {'Modifications':<30}",
            "-" * 80,
        ]

        for name, result in self.scenarios:
            cost = result.projected.primary_cost or 0
            vs_baseline = (
                "—"
                if name == self.scenarios[0][0]
                else f"{result.total_savings_pct:+.1f}%"
            )
            mods = (
                ", ".join(m.name for m in result.modifications)
                if result.modifications
                else "—"
            )
            lines.append(f"{name:<20} {cost:<15.2f} {vs_baseline:<15} {mods:<30}")

        return "\n".join(lines)

    def to_markdown(self) -> str:
        """Return a GFM markdown table using tabulate."""
        if not self.scenarios:
            return "No scenarios to compare."

        first_currency = self.scenarios[0][1].projected.primary_currency or "USD"
        rows = []
        for name, result in self.scenarios:
            cost = result.projected.primary_cost or 0
            vs_baseline = (
                "—"
                if name == self.scenarios[0][0]
                else f"{result.total_savings_pct:+.1f}%"
            )
            mods = (
                ", ".join(m.name for m in result.modifications)
                if result.modifications
                else "—"
            )
            rows.append([name, f"{cost:.2f}", vs_baseline, mods])

        return tabulate(
            rows,
            headers=["Scenario", f"Cost ({first_currency})", "vs Baseline", "Modifications"],
            tablefmt="github",
        )

    def __str__(self) -> str:
        """Return string representation (comparison table)."""
        return self.comparison_table()

    def __repr__(self) -> str:
        """Return developer representation."""
        return f"MultiSimulationResult(scenarios={len(self.scenarios)})"
