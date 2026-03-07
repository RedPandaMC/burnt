"""Hybrid cost estimator combining static analysis, EXPLAIN COST, and historical data."""

from __future__ import annotations

from statistics import median

from ..core.models import ClusterConfig, CostEstimate, ExplainPlan, QueryRecord
from .static import CostEstimator

_JOIN_DBU_WEIGHTS: dict[str, float] = {
    "BroadcastHashJoin": 0.1,
    "SortMergeJoin": 0.5,
    "ShuffledHashJoin": 0.3,
    "CartesianProduct": 2.0,
}
_SCAN_DBU_PER_GB: float = 0.5
_SHUFFLE_DBU_EACH: float = 0.2
_NOMINAL_USD_PER_DBU: float = 0.20


class HybridEstimator:
    """Combines static analysis, EXPLAIN COST plan data, and historical query records."""

    def __init__(self) -> None:
        """Initialise with a static estimator as fallback."""
        self._static = CostEstimator()

    def estimate(
        self,
        query: str,
        cluster: ClusterConfig,
        explain_plan: ExplainPlan | None = None,
        historical: list[QueryRecord] | None = None,
    ) -> CostEstimate:
        """Estimate query cost by combining available signals.

        Priority: historical exact match > EXPLAIN COST > static analysis.
        Returns a CostEstimate with confidence reflecting signal quality.
        """
        # 1. Try historical match first
        if historical:
            hist_estimate = self._from_historical(historical, cluster)
            if hist_estimate is not None:
                return hist_estimate

        # 2. Get static baseline
        static_estimate = self._static.estimate(query, cluster=cluster)

        # 3. Blend with EXPLAIN if available
        if explain_plan is None:
            return static_estimate

        return self._blend(static_estimate, explain_plan, cluster)

    def _from_historical(
        self, records: list[QueryRecord], cluster: ClusterConfig
    ) -> CostEstimate | None:
        """Build a CostEstimate from historical execution records, or None if no valid durations.

        Uses the median (p50) execution duration across all records with non-None
        execution_duration_ms.
        """
        durations = [
            r.execution_duration_ms
            for r in records
            if r.execution_duration_ms is not None
        ]
        if not durations:
            return None

        p50_ms = median(durations)
        dbu = (p50_ms / 3_600_000) * cluster.dbu_per_hour
        cost_usd = round(dbu * _NOMINAL_USD_PER_DBU, 6)

        return CostEstimate(
            estimated_dbu=round(dbu, 4),
            estimated_cost_usd=cost_usd,
            confidence="high",
            breakdown={"p50_duration_ms": p50_ms, "record_count": len(records)},
            warnings=[
                f"Historical p50 from {len(records)} matched executions ({p50_ms:.0f}ms)"
            ],
        )

    def _blend(
        self, static: CostEstimate, plan: ExplainPlan, cluster: ClusterConfig
    ) -> CostEstimate:
        """Blend static estimate with EXPLAIN COST signal.

        Weights depend on whether EXPLAIN statistics are complete:
        - stats_complete=True:  70% EXPLAIN + 30% static → confidence=high
        - stats_complete=False: 40% EXPLAIN + 60% static → confidence=medium
        """
        explain_dbu = self._explain_dbu(plan, cluster)
        static_dbu = static.estimated_dbu

        if plan.stats_complete:
            weight_explain, weight_static = 0.70, 0.30
            confidence = "high"
        else:
            weight_explain, weight_static = 0.40, 0.60
            confidence = "medium"

        blended_dbu = explain_dbu * weight_explain + static_dbu * weight_static
        cost_usd = round(blended_dbu * _NOMINAL_USD_PER_DBU, 6)

        return CostEstimate(
            estimated_dbu=round(blended_dbu, 4),
            estimated_cost_usd=cost_usd,
            confidence=confidence,
            breakdown={
                "explain_dbu": round(explain_dbu, 4),
                "static_dbu": round(static_dbu, 4),
                "weight_explain": weight_explain,
                "weight_static": weight_static,
            },
            warnings=[
                f"Hybrid: EXPLAIN({weight_explain:.0%}) + static({weight_static:.0%}); "
                f"stats_complete={plan.stats_complete}"
            ],
        )

    def _explain_dbu(self, plan: ExplainPlan, cluster: ClusterConfig) -> float:
        """Compute estimated DBU from EXPLAIN COST plan data.

        Combines per-GB scan cost, per-shuffle cost, and per-join-type penalty,
        then scales by the cluster's DBU rate.
        """
        scan_dbu = (plan.total_size_bytes / 1e9) * _SCAN_DBU_PER_GB
        shuffle_dbu = plan.shuffle_count * _SHUFFLE_DBU_EACH
        join_dbu = sum(_JOIN_DBU_WEIGHTS.get(jt, 0.1) for jt in plan.join_types)
        return (scan_dbu + shuffle_dbu + join_dbu) * cluster.dbu_per_hour
