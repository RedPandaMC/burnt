"""Hybrid cost estimator combining static analysis, EXPLAIN COST, and historical data."""

from __future__ import annotations

from statistics import median

from ..core.models import (
    ClusterConfig,
    CostEstimate,
    DeltaTableInfo,
    ExplainPlan,
    QueryRecord,
)
from ..parsers.sql import extract_tables
from .static import CostEstimator

_JOIN_DBU_WEIGHTS: dict[str, float] = {
    "BroadcastHashJoin": 0.1,
    "SortMergeJoin": 0.5,
    "ShuffledHashJoin": 0.3,
    "CartesianProduct": 2.0,
}
_SCAN_DBU_PER_GB: float = 0.00013
_SHUFFLE_DBU_EACH: float = 0.2


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
        delta_tables: dict[str, DeltaTableInfo] | None = None,
    ) -> CostEstimate:
        """Estimate query cost by combining available signals.

        Priority: historical exact match > EXPLAIN COST + Delta metadata > static analysis.
        Returns a CostEstimate with confidence reflecting signal quality.

        Args:
            query: SQL query to estimate cost for
            cluster: Cluster configuration
            explain_plan: Parsed EXPLAIN COST output, if available
            historical: Historical query execution records, if available
            delta_tables: Dictionary of table_name -> DeltaTableInfo for Delta tables.
                          When provided, uses Delta table sizes instead of EXPLAIN estimates.
        """
        # 1. Try historical match first
        if historical:
            current_size_bytes = None
            if delta_tables:
                tables = extract_tables(query)
                current_size_bytes = sum(
                    delta_tables.get(
                        t, DeltaTableInfo(location="", total_size_bytes=0, num_files=0)
                    ).total_size_bytes
                    for t in tables
                )
            hist_estimate = self._from_historical(
                historical, cluster, current_size_bytes
            )
            if hist_estimate is not None:
                return hist_estimate

        # 2. Get static baseline
        static_estimate = self._static.estimate(query, cluster=cluster)

        # 3. Blend with EXPLAIN if available
        if explain_plan is None and delta_tables is None:
            return static_estimate

        # 4. Use Delta sizes or EXPLAIN for scan size
        override_size_bytes = None
        if delta_tables:
            tables = extract_tables(query)
            total_delta_size = sum(
                delta_tables.get(
                    t, DeltaTableInfo(location="", total_size_bytes=0, num_files=0)
                ).total_size_bytes
                for t in tables
            )
            if total_delta_size > 0:
                override_size_bytes = total_delta_size

        if explain_plan is None:
            # Delta-only mode: use static + delta override
            explain_plan = ExplainPlan(
                total_size_bytes=override_size_bytes or 0,
                stats_complete=override_size_bytes is not None,
                join_types=[],
                shuffle_count=0,
            )
            return self._blend(
                static_estimate, explain_plan, cluster, override_size_bytes
            )

        return self._blend(static_estimate, explain_plan, cluster, override_size_bytes)

    def _from_historical(
        self,
        records: list[QueryRecord],
        cluster: ClusterConfig,
        current_read_bytes: int | None = None,
    ) -> CostEstimate | None:
        """Build a CostEstimate from historical execution records, or None if no valid durations.

        Uses the median (p50) execution duration across all records with non-None
        execution_duration_ms. If current_read_bytes is provided and historical data
        includes read_bytes, scales the duration proportionally.
        """
        durations = [
            r.execution_duration_ms
            for r in records
            if r.execution_duration_ms is not None
        ]
        if not durations:
            return None

        p50_ms = median(durations)

        read_bytes_values = [r.read_bytes for r in records if r.read_bytes is not None]
        if (
            read_bytes_values
            and current_read_bytes is not None
            and current_read_bytes > 0
        ):
            historical_read_bytes = median(read_bytes_values)
            if historical_read_bytes > 0:
                scale_factor = current_read_bytes / historical_read_bytes
                p50_ms = p50_ms * scale_factor

        dbu = (p50_ms / 3_600_000) * cluster.dbu_per_hour

        return CostEstimate(
            estimated_dbu=round(dbu, 4),
            estimated_cost_usd=None,
            confidence="high",
            breakdown={"p50_duration_ms": p50_ms, "record_count": len(records)},
            warnings=[
                f"Historical p50 from {len(records)} matched executions ({p50_ms:.0f}ms)"
            ],
        )

    def _blend(
        self,
        static: CostEstimate,
        plan: ExplainPlan,
        cluster: ClusterConfig,
        override_size_bytes: int | None = None,
    ) -> CostEstimate:
        """Blend static estimate with EXPLAIN COST signal.

        Weights depend on whether EXPLAIN statistics are complete:
        - stats_complete=True:  70% EXPLAIN + 30% static → confidence=high
        - stats_complete=False: 40% EXPLAIN + 60% static → confidence=medium

        If override_size_bytes is provided, uses that for scan size instead of plan.total_size_bytes.
        """
        explain_dbu = self._explain_dbu(plan, cluster, override_size_bytes)
        static_dbu = static.estimated_dbu

        if plan.stats_complete:
            weight_explain, weight_static = 0.70, 0.30
            confidence = "high"
        else:
            weight_explain, weight_static = 0.40, 0.60
            confidence = "medium"

        blended_dbu = explain_dbu * weight_explain + static_dbu * weight_static

        source = "delta" if override_size_bytes else "explain"

        return CostEstimate(
            estimated_dbu=round(blended_dbu, 4),
            estimated_cost_usd=None,
            confidence=confidence,
            breakdown={
                "explain_dbu": round(explain_dbu, 4),
                "static_dbu": round(static_dbu, 4),
                "weight_explain": weight_explain,
                "weight_static": weight_static,
            },
            warnings=[
                f"Hybrid: {source}({weight_explain:.0%}) + static({weight_static:.0%}); "
                f"stats_complete={plan.stats_complete}"
            ],
        )

    def _explain_dbu(
        self,
        plan: ExplainPlan,
        cluster: ClusterConfig,
        override_size_bytes: int | None = None,
    ) -> float:
        """Compute estimated DBU from EXPLAIN COST plan data.

        Combines per-GB scan cost, per-shuffle cost, and per-join-type penalty,
        then scales by the cluster's DBU rate.

        If override_size_bytes is provided, uses that for scan size instead of
        plan.total_size_bytes (e.g., when Delta metadata is available).
        """
        size_bytes = (
            override_size_bytes
            if override_size_bytes is not None
            else plan.total_size_bytes
        )
        scan_dbu = (size_bytes / 1e9) * _SCAN_DBU_PER_GB
        shuffle_dbu = plan.shuffle_count * _SHUFFLE_DBU_EACH
        join_dbu = sum(_JOIN_DBU_WEIGHTS.get(jt, 0.1) for jt in plan.join_types)
        return (scan_dbu + shuffle_dbu + join_dbu) * cluster.dbu_per_hour
