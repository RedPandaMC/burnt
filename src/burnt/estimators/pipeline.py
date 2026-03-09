"""Estimation pipeline that orchestrates multiple estimation tiers."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ..core.models import (  # noqa: TC001
    ClusterConfig,
    CostEstimate,
    DeltaTableInfo,
    ExplainPlan,
    QueryRecord,
)
from ..parsers.delta import parse_describe_detail
from ..parsers.explain import parse_explain_cost
from ..parsers.sql import extract_tables
from ..tables.connection import DatabricksClient
from ..tables.queries import fingerprint_sql, get_query_history
from .hybrid import HybridEstimator
from .static import CostEstimator

if TYPE_CHECKING:
    from ..core.config import Settings

logger = logging.getLogger(__name__)


class EstimationPipeline:
    """Orchestrates multiple cost estimation tiers.

    Tries tiers in order, falling back gracefully:
    - Tier 1: Static analysis (always runs)
    - Tier 2: Delta metadata (DESCRIBE DETAIL per table)
    - Tier 3: EXPLAIN COST
    - Tier 4: Historical query fingerprints

    Args:
        backend: Optional DatabricksClient. If None, runs in offline mode (Tier 1 only).
        warehouse_id: SQL warehouse ID for EXPLAIN COST and query history.
    """

    def __init__(
        self,
        backend: DatabricksClient | None = None,
        warehouse_id: str | None = None,
    ) -> None:
        """Initialize the pipeline with optional backend."""
        self._backend = backend
        self._warehouse_id = warehouse_id
        self._hybrid = HybridEstimator()
        self._static = CostEstimator()

    def estimate(
        self,
        query: str,
        cluster: ClusterConfig,
    ) -> CostEstimate:
        """Estimate query cost using available data sources.

        Args:
            query: SQL query to estimate
            cluster: Cluster configuration

        Returns:
            CostEstimate with DBU estimate and confidence level
        """
        result = self._static.estimate(query, cluster=cluster)
        signal = "static"

        if self._backend is None:
            logger.debug("No backend configured - using static estimation only")
            return result

        if self._warehouse_id is None:
            logger.debug("No warehouse_id - using static estimation only")
            return result

        # Tier 2: Delta metadata
        delta_tables: dict[str, DeltaTableInfo] = {}
        try:
            tables = extract_tables(query)
            for table in tables:
                try:
                    detail_rows = self._backend.execute_sql(
                        f"DESCRIBE DETAIL {table}", self._warehouse_id
                    )
                    delta_tables[table] = parse_describe_detail(detail_rows)
                except Exception as e:
                    logger.warning(f"Failed to get Delta metadata for {table}: {e}")
            if delta_tables:
                logger.info(f"Retrieved Delta metadata for {len(delta_tables)} tables")
        except Exception as e:
            logger.warning(f"Tier 2 (Delta metadata) failed: {e}")

        # Tier 3: EXPLAIN COST
        explain_plan: ExplainPlan | None = None
        try:
            explain_sql = f"EXPLAIN COST {query}"
            rows = self._backend.execute_sql(explain_sql, self._warehouse_id)
            plan_text = rows[0].get("plan", "") if rows else ""
            if plan_text:
                explain_plan = parse_explain_cost(plan_text)
                logger.info("Retrieved EXPLAIN COST plan")
            else:
                logger.warning("EXPLAIN COST returned empty result")
        except Exception as e:
            logger.warning(f"Tier 3 (EXPLAIN COST) failed: {e}")

        # Tier 4: Historical fingerprints
        historical: list[QueryRecord] = []
        try:
            fp = fingerprint_sql(query)
            history = get_query_history(self._backend, self._warehouse_id, days=30)
            for record in history:
                if fingerprint_sql(record.statement_text) == fp:
                    historical.append(record)
            if historical:
                logger.info(f"Found {len(historical)} historical executions")
        except Exception as e:
            logger.warning(f"Tier 4 (historical fingerprints) failed: {e}")

        # Use hybrid estimator with all available signals
        if explain_plan or delta_tables or historical:
            result = self._hybrid.estimate(
                query,
                cluster,
                explain_plan=explain_plan,
                historical=historical if historical else None,
                delta_tables=delta_tables if delta_tables else None,
            )
            if historical:
                signal = "hybrid+history"
            elif explain_plan and delta_tables:
                signal = "hybrid+explain+delta"
            elif explain_plan:
                signal = "hybrid+explain"
            elif delta_tables:
                signal = "hybrid+delta"

        result.warnings.append(f"Signal: {signal}")
        return result


def create_pipeline(
    settings: Settings | None = None, warehouse_id: str | None = None
) -> EstimationPipeline:
    """Factory function to create an EstimationPipeline with Databricks client.

    Args:
        settings: Application settings. If None, creates offline pipeline.
        warehouse_id: SQL warehouse ID. Required for Tiers 2-4.

    Returns:
        EstimationPipeline instance
    """
    if settings and settings.workspace_url and settings.token and warehouse_id:
        from ..core.config import Settings as ConfigSettings

        settings = settings or ConfigSettings()
        backend = DatabricksClient(settings)
        return EstimationPipeline(backend=backend, warehouse_id=warehouse_id)

    return EstimationPipeline(backend=None, warehouse_id=None)
