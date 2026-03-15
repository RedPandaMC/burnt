"""Pydantic models for burnt."""

from __future__ import annotations

from decimal import Decimal  # noqa: TC003 — used in pydantic field type
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, PrivateAttr, field_validator
from tabulate import tabulate

from ._display import _DisplayMixin

if TYPE_CHECKING:
    from burnt.estimators.simulation import Simulation


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


class PricingInfo(BaseModel):
    """Pricing information for a SKU."""

    sku_name: str
    dbu_rate: float
    cloud: str = "AZURE"
    region: str = "EAST_US"


class CostEstimate(BaseModel, _DisplayMixin):
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
        from burnt.estimators.simulation import Simulation

        return Simulation(self, self._cluster)

    def raise_if_exceeds(self, budget_usd: float, label: str = "") -> None:
        """Raise CostBudgetExceeded if cost exceeds budget. Implemented in s2-07."""
        raise NotImplementedError("raise_if_exceeds() will be implemented in s2-07")

    def comparison_table(self) -> str:
        """Generate ASCII comparison table."""
        lines = [
            "Cost Estimate",
            f"{'Field':<20} {'Value':<30}",
            "-" * 50,
            f"{'Estimated DBU':<20} {self.estimated_dbu:<30.2f}",
        ]
        if self.estimated_cost_usd is not None:
            lines.append(f"{'Estimated Cost':<20} ${self.estimated_cost_usd:<29.2f}")
        lines.append(f"{'Confidence':<20} {self.confidence:<30}")

        if self.breakdown:
            lines.extend(["", "Breakdown:"])
            for key, value in self.breakdown.items():
                lines.append(f"  {key}: {value:.2f}")

        if self.warnings:
            lines.extend(["", "Warnings:"])
            for warning in self.warnings:
                lines.append(f"  ⚠ {warning}")

        return "\n".join(lines)

    def _to_html_table(self) -> str:
        """Generate HTML table for notebooks."""
        rows = [
            f"<tr><td>Estimated DBU</td><td>{self.estimated_dbu:.2f}</td></tr>",
        ]
        if self.estimated_cost_usd is not None:
            rows.append(
                f"<tr><td>Estimated Cost</td><td>${self.estimated_cost_usd:.2f}</td></tr>"
            )
        rows.append(f"<tr><td>Confidence</td><td>{self.confidence}</td></tr>")

        html = f"""
        <div style="font-family: monospace; margin: 20px 0;">
            <h3>Cost Estimate</h3>
            <table style="border-collapse: collapse; width: 100%;">
                <thead>
                    <tr style="background-color: #f0f0f0;">
                        <th style="padding: 8px; border: 1px solid #ccc;">Field</th>
                        <th style="padding: 8px; border: 1px solid #ccc;">Value</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join(rows)}
                </tbody>
            </table>
        """

        if self.breakdown:
            html += "<h4>Breakdown:</h4><ul>"
            for key, value in self.breakdown.items():
                html += f"<li>{key}: {value:.2f}</li>"
            html += "</ul>"

        if self.warnings:
            html += '<div style="margin: 10px 0; padding: 10px; background-color: #fff3cd; border-left: 4px solid #ffc107;">'
            html += "<strong>Warnings:</strong><ul>"
            for warning in self.warnings:
                html += f"<li>{warning}</li>"
            html += "</ul></div>"

        html += "</div>"
        return html

    def to_markdown(self) -> str:
        """Return a GFM markdown table using tabulate."""
        rows = [
            ["Estimated DBU", f"{self.estimated_dbu:.2f}"],
        ]
        if self.estimated_cost_usd is not None:
            rows.append(["Estimated Cost", f"${self.estimated_cost_usd:.2f}"])
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
        cost = self.estimated_cost_usd or 0
        return f"CostEstimate(dbu={self.estimated_dbu:.2f}, cost=${cost:.2f}, confidence={self.confidence})"


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

    def _to_html_table(self) -> str:
        """Generate HTML table for notebooks."""
        rows = [
            f"<tr><td>Economy</td><td>{self.economy.instance_type}</td><td>{self.economy.num_workers}</td><td>{self.economy.dbu_per_hour:.2f}</td><td>${self.economy.dbu_per_hour * 1.0:.2f}</td></tr>",
            f"<tr><td>Balanced</td><td>{self.balanced.instance_type}</td><td>{self.balanced.num_workers}</td><td>{self.balanced.dbu_per_hour:.2f}</td><td>${self.balanced.dbu_per_hour * 1.5:.2f}</td></tr>",
            f"<tr><td>Performance</td><td>{self.performance.instance_type}</td><td>{self.performance.num_workers}</td><td>{self.performance.dbu_per_hour:.2f}</td><td>${self.performance.dbu_per_hour * 2.0:.2f}</td></tr>",
        ]
        return f"""
        <div style="font-family: monospace; margin: 20px 0;">
            <h3>Cluster Recommendation</h3>
            <table style="border-collapse: collapse; width: 100%;">
                <thead>
                    <tr style="background-color: #f0f0f0;">
                        <th style="padding: 8px; border: 1px solid #ccc;">Tier</th>
                        <th style="padding: 8px; border: 1px solid #ccc;">Instance</th>
                        <th style="padding: 8px; border: 1px solid #ccc;">Workers</th>
                        <th style="padding: 8px; border: 1px solid #ccc;">DBU/hr</th>
                        <th style="padding: 8px; border: 1px solid #ccc;">Est. Cost</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join(rows)}
                </tbody>
            </table>
            <p><strong>Rationale:</strong> {self.rationale}</p>
        </div>
        """

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
            f"{'Metric':<20} {'Original':<15} {'Projected':<15} {'Δ':<10}",
            "-" * 60,
            f"{'DBU':<20} {self.original.estimated_dbu:<15.2f} {self.projected.estimated_dbu:<15.2f} {self.total_savings_pct:<10.1f}%",
            f"{'Cost (USD)':<20} ${original_cost:<14.2f} ${projected_cost:<14.2f} {self.total_savings_pct:<10.1f}%",
            "",
            "Modifications:",
        ]
        for mod in self.modifications:
            verified = "✓" if mod.is_verified else "≈"
            lines.append(
                f"  {verified} {mod.name}: {mod.cost_multiplier:.2f}x - {mod.rationale}"
            )

        return "\n".join(lines)

    def _to_html_table(self) -> str:
        """Generate HTML table for notebooks."""
        original_cost = self.original.estimated_cost_usd or 0
        projected_cost = self.projected.estimated_cost_usd or 0

        mod_rows = []
        for mod in self.modifications:
            verified = "✓" if mod.is_verified else "≈"
            mod_rows.append(
                f"<li>{verified} <strong>{mod.name}</strong>: {mod.cost_multiplier:.2f}x - {mod.rationale}</li>"
            )

        return f"""
        <div style="font-family: monospace; margin: 20px 0;">
            <h3>Simulation Comparison</h3>
            <table style="border-collapse: collapse; width: 100%;">
                <thead>
                    <tr style="background-color: #f0f0f0;">
                        <th style="padding: 8px; border: 1px solid #ccc;">Metric</th>
                        <th style="padding: 8px; border: 1px solid #ccc;">Original</th>
                        <th style="padding: 8px; border: 1px solid #ccc;">Projected</th>
                        <th style="padding: 8px; border: 1px solid #ccc;">Δ</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td style="padding: 8px; border: 1px solid #ccc;">DBU</td>
                        <td style="padding: 8px; border: 1px solid #ccc;">{self.original.estimated_dbu:.2f}</td>
                        <td style="padding: 8px; border: 1px solid #ccc;">{self.projected.estimated_dbu:.2f}</td>
                        <td style="padding: 8px; border: 1px solid #ccc;">{self.total_savings_pct:.1f}%</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; border: 1px solid #ccc;">Cost (USD)</td>
                        <td style="padding: 8px; border: 1px solid #ccc;">${original_cost:.2f}</td>
                        <td style="padding: 8px; border: 1px solid #ccc;">${projected_cost:.2f}</td>
                        <td style="padding: 8px; border: 1px solid #ccc;">{self.total_savings_pct:.1f}%</td>
                    </tr>
                </tbody>
            </table>
            <h4>Modifications:</h4>
            <ul>{"".join(mod_rows)}</ul>
        </div>
        """

    def to_markdown(self) -> str:
        """Return a GFM markdown table using tabulate."""
        original_cost = self.original.estimated_cost_usd or 0
        projected_cost = self.projected.estimated_cost_usd or 0

        rows = [
            [
                "Cost (USD)",
                f"${original_cost:.2f}",
                f"${projected_cost:.2f}",
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
            cost = result.projected.estimated_cost_usd or float("inf")
            confidence_score = confidence_order.get(result.projected.confidence, 0)
            return (
                cost,
                -confidence_score,
            )  # Negative so higher confidence comes first

        return min(self.scenarios, key=sort_key)

    def comparison_table(self) -> str:
        """Generate ASCII comparison table for all scenarios."""
        if not self.scenarios:
            return "No scenarios to compare."

        lines = [
            "Scenario Comparison",
            f"{'Scenario':<20} {'Cost (USD)':<15} {'vs Baseline':<15} {'Modifications':<30}",
            "-" * 80,
        ]

        for name, result in self.scenarios:
            cost = result.projected.estimated_cost_usd or 0
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
            lines.append(f"{name:<20} ${cost:<14.2f} {vs_baseline:<15} {mods:<30}")

        return "\n".join(lines)

    def _to_html_table(self) -> str:
        """Generate HTML table for notebooks."""
        if not self.scenarios:
            return "<p>No scenarios to compare.</p>"

        rows = []
        for name, result in self.scenarios:
            cost = result.projected.estimated_cost_usd or 0
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
            rows.append(
                f"<tr><td>{name}</td><td>${cost:.2f}</td><td>{vs_baseline}</td><td>{mods}</td></tr>"
            )

        return f"""
        <div style="font-family: monospace; margin: 20px 0;">
            <h3>Scenario Comparison</h3>
            <table style="border-collapse: collapse; width: 100%;">
                <thead>
                    <tr style="background-color: #f0f0f0;">
                        <th style="padding: 8px; border: 1px solid #ccc;">Scenario</th>
                        <th style="padding: 8px; border: 1px solid #ccc;">Cost (USD)</th>
                        <th style="padding: 8px; border: 1px solid #ccc;">vs Baseline</th>
                        <th style="padding: 8px; border: 1px solid #ccc;">Modifications</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join(rows)}
                </tbody>
            </table>
        </div>
        """

    def to_markdown(self) -> str:
        """Return a GFM markdown table using tabulate."""
        if not self.scenarios:
            return "No scenarios to compare."

        rows = []
        for name, result in self.scenarios:
            cost = result.projected.estimated_cost_usd or 0
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
            rows.append([name, f"${cost:.2f}", vs_baseline, mods])

        return tabulate(
            rows,
            headers=["Scenario", "Cost (USD)", "vs Baseline", "Modifications"],
            tablefmt="github",
        )

    def __str__(self) -> str:
        """Return string representation (comparison table)."""
        return self.comparison_table()

    def __repr__(self) -> str:
        """Return developer representation."""
        return f"MultiSimulationResult(scenarios={len(self.scenarios)})"
