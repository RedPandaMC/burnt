"""Advisory report models and display rendering."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from burnt.core._display import _DisplayMixin
from burnt.core.models import (  # noqa: TC001
    ClusterConfig,
    ClusterProfile,
    ClusterRecommendation,
)


class ComputeScenario(BaseModel):
    """Compute migration scenario with cost comparison."""

    compute_type: str  # "All-Purpose", "Jobs Compute", "Serverless"
    sku: str  # "ALL_PURPOSE", "JOBS_COMPUTE", "SERVERLESS"
    estimated_cost_usd: float
    savings_pct: float  # vs baseline (negative = cheaper)
    tradeoff: str  # e.g., "Recommended", "Fastest cold start"


class AdvisoryReport(BaseModel, _DisplayMixin):
    """Advisory report with compute migration analysis and cluster recommendation."""

    baseline: ComputeScenario  # The current run (All-Purpose)
    scenarios: list[ComputeScenario]  # Jobs Compute, Serverless, etc.
    recommended: ClusterConfig  # Best cluster config with to_json()
    recommendation: ClusterRecommendation  # economy/balanced/performance tiers
    insights: list[str]  # e.g., "Peak memory 14%, downsize DS4→DS3"
    run_metrics: dict[str, Any] = {}  # Raw metrics from the analyzed run
    num_runs_analyzed: int | None = None  # Number of runs analyzed (for job_id)
    confidence_level: str | None = None  # "high", "medium", "low"
    cluster_profile: ClusterProfile | None = None  # Full runtime context when available

    def _render_rich(self) -> str:
        """Return rich-renderable string with table, insights, and recommendation."""
        lines = [self.comparison_table()]
        lines.append("\n💡 " + "\n💡 ".join(self.insights))
        lines.append("\nRecommended Cluster (paste into Job definition):")
        lines.append(self.recommended.model_dump_json(indent=2))
        return "\n".join(lines)

    def comparison_table(self) -> str:
        """ASCII table matching docs/cli-workflows.md Compute Migration Analysis format."""
        lines = [
            "  Compute Migration Analysis",
            "┌──────────────────┬───────────┬──────────┬────────────────────┐",
            "│ Compute Type     │ Est. Cost │ Savings  │ Tradeoff           │",
            "├──────────────────┼───────────┼──────────┤────────────────────┤",
        ]

        # Baseline row
        baseline_cost = f"${self.baseline.estimated_cost_usd:.2f}"
        baseline_savings = "baseline"
        lines.append(
            f"│ {self.baseline.compute_type:<16} │ {baseline_cost:<9} │ {baseline_savings:<8} │ {self.baseline.tradeoff:<18} │"
        )

        # Scenario rows
        for scenario in self.scenarios:
            cost = f"${scenario.estimated_cost_usd:.2f}"
            savings = (
                f"{scenario.savings_pct:+.1f}%" if scenario.savings_pct != 0 else "—"
            )
            lines.append(
                f"│ {scenario.compute_type:<16} │ {cost:<9} │ {savings:<8} │ {scenario.tradeoff:<18} │"
            )

        lines.append("└──────────────────┴───────────┴──────────┴────────────────────┘")
        return "\n".join(lines)

    def simulate(self) -> Any:
        """Chain into simulation scenarios from this advisory report."""
        from burnt.core.models import CostEstimate
        from burnt.estimators.simulation import Simulation

        estimate = CostEstimate(
            estimated_dbu=self.baseline.estimated_cost_usd / 0.55,
            estimated_cost_usd=self.baseline.estimated_cost_usd,
            confidence="low",
        )
        return Simulation(estimate)

    def _to_html_table(self) -> str:
        """Generate HTML table for Databricks notebook display."""
        html = """
        <div style="font-family: monospace; margin: 20px 0;">
            <h3 style="margin-bottom: 10px;">Compute Migration Analysis</h3>
            <table style="border-collapse: collapse; width: 100%;">
                <thead>
                    <tr style="background-color: #f0f0f0; border-bottom: 2px solid #ccc;">
                        <th style="padding: 8px; text-align: left; border: 1px solid #ccc;">Compute Type</th>
                        <th style="padding: 8px; text-align: left; border: 1px solid #ccc;">Est. Cost</th>
                        <th style="padding: 8px; text-align: left; border: 1px solid #ccc;">Savings</th>
                        <th style="padding: 8px; text-align: left; border: 1px solid #ccc;">Tradeoff</th>
                    </tr>
                </thead>
                <tbody>
        """

        # Baseline row
        html += f"""
                    <tr style="border-bottom: 1px solid #eee;">
                        <td style="padding: 8px; border: 1px solid #ccc;">{self.baseline.compute_type}</td>
                        <td style="padding: 8px; border: 1px solid #ccc;">${self.baseline.estimated_cost_usd:.2f}</td>
                        <td style="padding: 8px; border: 1px solid #ccc; font-style: italic;">baseline</td>
                        <td style="padding: 8px; border: 1px solid #ccc;">{self.baseline.tradeoff}</td>
                    </tr>
        """

        # Scenario rows
        for scenario in self.scenarios:
            savings_color = (
                "green"
                if scenario.savings_pct > 0
                else "red"
                if scenario.savings_pct < 0
                else "black"
            )
            savings_text = (
                f"{scenario.savings_pct:+.1f}%" if scenario.savings_pct != 0 else "—"
            )

            html += f"""
                    <tr style="border-bottom: 1px solid #eee;">
                        <td style="padding: 8px; border: 1px solid #ccc;">{scenario.compute_type}</td>
                        <td style="padding: 8px; border: 1px solid #ccc;">${scenario.estimated_cost_usd:.2f}</td>
                        <td style="padding: 8px; border: 1px solid #ccc; color: {savings_color};">{savings_text}</td>
                        <td style="padding: 8px; border: 1px solid #ccc;">{scenario.tradeoff}</td>
                    </tr>
            """

        html += """
                </tbody>
            </table>
        </div>
        """

        # Add insights
        if self.insights:
            html += """
            <div style="margin: 20px 0; padding: 10px; background-color: #fffde7; border-left: 4px solid #ffd600;">
                <strong>💡 Insights:</strong><br>
            """
            for insight in self.insights:
                html += f"• {insight}<br>"
            html += """
            </div>
            """

        # Add recommendation
        html += f"""
        <div style="margin: 20px 0;">
            <h4 style="margin-bottom: 10px;">Recommended Cluster (paste into Job definition):</h4>
            <pre style="background-color: #f5f5f5; padding: 10px; border: 1px solid #ddd; border-radius: 4px; overflow: auto;">
{self.recommended.model_dump_json(indent=2)}
            </pre>
        </div>
        """

        return html

    def to_markdown(self) -> str:
        """Return a GFM markdown table."""
        lines = [
            "| Compute Type | Est. Cost | Savings | Tradeoff |",
            "|--------------|-----------|---------|----------|",
            f"| {self.baseline.compute_type} | ${self.baseline.estimated_cost_usd:.2f} | baseline | {self.baseline.tradeoff} |",
        ]
        for scenario in self.scenarios:
            savings = (
                f"{scenario.savings_pct:+.1f}%" if scenario.savings_pct != 0 else "—"
            )
            lines.append(
                f"| {scenario.compute_type} | ${scenario.estimated_cost_usd:.2f} | {savings} | {scenario.tradeoff} |"
            )

        lines.extend(["", "**Insights:**"])
        for insight in self.insights:
            lines.append(f"- 💡 {insight}")

        return "\n".join(lines)
