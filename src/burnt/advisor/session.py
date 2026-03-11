"""Session advisor implementation for burnt."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from burnt.runtime import Backend

from burnt.core.instances import WorkloadProfile, get_cluster_config
from burnt.core.models import ClusterConfig, ClusterRecommendation
from burnt.core.pricing import get_dbu_rate
from burnt.estimators.whatif import apply_serverless_migration

from .report import AdvisoryReport, ComputeScenario

logger = logging.getLogger(__name__)


def advise_current_session(backend: Backend | None = None) -> AdvisoryReport:
    """
    Analyze queries executed in the current SparkSession.

    1. Detect backend via auto_backend() if not provided
    2. Get session metrics: peak memory, disk spill, total duration, read_bytes
    3. Identify the current cluster config (instance type, workers)
    4. Calculate All-Purpose baseline cost
    5. Project onto Jobs Compute, Serverless, Spot scenarios
    6. Run right_size() to recommend optimal cluster
    7. Build AdvisoryReport
    """
    # 1. Auto-detect backend if not provided
    if backend is None:
        backend = _auto_backend_or_error()

    # 2. Get session metrics
    try:
        metrics = backend.get_session_metrics()
    except Exception as e:
        raise RuntimeError(
            "Could not retrieve session metrics. "
            f"Make sure you're running in a Databricks notebook with active queries. Error: {e}"
        ) from e

    # 3. Get current cluster config
    current_cluster = _get_current_cluster_config(backend, metrics)

    # 4. Calculate baseline cost
    baseline_cost = _calculate_baseline_cost(current_cluster, metrics)

    # 5. Project scenarios
    scenarios = _project_scenarios(current_cluster, baseline_cost, metrics)

    # 6. Right-size recommendation
    workload_profile = _create_workload_profile(metrics)
    recommended_cluster = get_cluster_config(
        workload_profile, current_config=current_cluster, prefer_spot=True
    )

    # Create cluster recommendation (economy/balanced/performance)
    recommendation = _create_cluster_recommendation(
        current_cluster, recommended_cluster, workload_profile
    )

    # 7. Build insights
    insights = _generate_insights(metrics, current_cluster, recommended_cluster)

    # Build report
    baseline_scenario = ComputeScenario(
        compute_type="All-Purpose",
        sku="ALL_PURPOSE",
        estimated_cost_usd=baseline_cost,
        savings_pct=0.0,
        tradeoff="(Your test run)",
    )

    return AdvisoryReport(
        baseline=baseline_scenario,
        scenarios=scenarios,
        recommended=recommended_cluster,
        recommendation=recommendation,
        insights=insights,
        run_metrics=metrics,
    )


def advise(
    run_id: str | None = None,
    statement_id: str | None = None,
    job_id: str | None = None,
    backend: Backend | None = None,
) -> AdvisoryReport:
    """
    Advise for a specific historical run via system tables.

    1. Fetch metrics from system.query.history or system.lakeflow.job_run_timeline
    2. Extract: duration, read_bytes, spill_to_disk, peak memory
    3. Same projection logic as advise_current_session()
    """
    if job_id is not None:
        raise NotImplementedError(
            "advise(job_id=...) is not yet implemented. "
            "Use --run-id or --statement-id instead. "
            "Job ID analysis for multiple runs will be added in a future release."
        )

    if run_id is None and statement_id is None:
        raise ValueError("Either run_id or statement_id must be provided")

    if backend is None:
        backend = _auto_backend_or_error()

    # 1. Fetch metrics from system tables
    metrics = _fetch_metrics_from_history(backend, run_id, statement_id)

    # 2. Extract key metrics
    current_cluster = _infer_cluster_from_metrics(metrics)

    # 3. Calculate baseline cost
    baseline_cost = _calculate_baseline_cost(current_cluster, metrics)

    # 4. Project scenarios
    scenarios = _project_scenarios(current_cluster, baseline_cost, metrics)

    # 5. Right-size recommendation
    workload_profile = _create_workload_profile(metrics)
    recommended_cluster = get_cluster_config(
        workload_profile, current_config=current_cluster, prefer_spot=True
    )

    # Create cluster recommendation
    recommendation = _create_cluster_recommendation(
        current_cluster, recommended_cluster, workload_profile
    )

    # 6. Build insights
    insights = _generate_insights(metrics, current_cluster, recommended_cluster)

    # Build report
    baseline_scenario = ComputeScenario(
        compute_type="All-Purpose",
        sku="ALL_PURPOSE",
        estimated_cost_usd=baseline_cost,
        savings_pct=0.0,
        tradeoff="(Historical run)",
    )

    return AdvisoryReport(
        baseline=baseline_scenario,
        scenarios=scenarios,
        recommended=recommended_cluster,
        recommendation=recommendation,
        insights=insights,
        run_metrics=metrics,
    )


def _auto_backend_or_error() -> Backend:
    """Get backend via auto-detection or raise informative error."""
    from burnt.runtime import auto_backend

    backend = auto_backend()
    if backend is None:
        raise RuntimeError(
            "No Databricks execution context detected. "
            "advise_current_session() requires a Databricks runtime. "
            "Set DATABRICKS_HOST and authentication credentials, "
            "or run inside a Databricks notebook."
        )
    return backend


def _get_current_cluster_config(backend: Backend, metrics: dict) -> ClusterConfig:
    """Extract current cluster configuration from metrics."""
    # Try to get cluster ID from metrics
    cluster_id = metrics.get("cluster_id")
    if cluster_id:
        try:
            return backend.get_cluster_config(cluster_id)
        except Exception as e:
            logger.warning(f"Could not fetch cluster config for {cluster_id}: {e}")

    # Fallback to default configuration
    return ClusterConfig(
        instance_type="Standard_DS4_v2",  # Typical test cluster
        num_workers=2,
        dbu_per_hour=1.5,  # DS4_v2 DBU rate
        sku="ALL_PURPOSE",
        photon_enabled=False,
        spot_policy="ON_DEMAND",
    )


def _calculate_baseline_cost(cluster: ClusterConfig, metrics: dict) -> float:
    """Calculate baseline cost for All-Purpose compute."""
    duration_hours = metrics.get("duration_ms", 0) / 1000 / 3600  # ms → hours

    # DBU cost
    dbu_rate = get_dbu_rate("ALL_PURPOSE")
    dbu_cost = cluster.dbu_per_hour * float(dbu_rate) * duration_hours

    # VM cost (for classic compute)
    # TODO: Get actual VM rate from instance catalog
    vm_cost_per_hour = 0.585  # DS4_v2 hourly rate
    vm_cost = vm_cost_per_hour * cluster.num_workers * duration_hours

    return dbu_cost + vm_cost


def _project_scenarios(
    current_cluster: ClusterConfig, baseline_cost: float, metrics: dict
) -> list[ComputeScenario]:
    """Project costs for different compute scenarios."""
    scenarios = []

    # Jobs Compute scenario
    jobs_compute_cost = baseline_cost * (
        0.30 / 0.55
    )  # Jobs Compute rate / All-Purpose rate
    scenarios.append(
        ComputeScenario(
            compute_type="Jobs Compute",
            sku="JOBS_COMPUTE",
            estimated_cost_usd=jobs_compute_cost,
            savings_pct=((baseline_cost - jobs_compute_cost) / baseline_cost) * 100,
            tradeoff="Recommended",
        )
    )

    # Serverless scenario
    # Use the whatif.py function
    from burnt.core.models import CostEstimate

    # Create a dummy estimate for the serverless calculation
    dummy_estimate = CostEstimate(
        estimated_dbu=current_cluster.dbu_per_hour
        * (metrics.get("duration_ms", 0) / 1000 / 3600),
        estimated_cost_usd=baseline_cost,
        confidence="medium",
        breakdown={},
        warnings=[],
    )

    serverless_result = apply_serverless_migration(
        dummy_estimate,
        current_sku="ALL_PURPOSE",
        utilization_pct=metrics.get("utilization_pct", 50.0),
    )

    # Handle None case for serverless cost
    serverless_cost = (
        serverless_result.estimated_cost_usd or baseline_cost * 0.7
    )  # Fallback

    scenarios.append(
        ComputeScenario(
            compute_type="SQL Serverless",
            sku="SERVERLESS",
            estimated_cost_usd=serverless_cost,
            savings_pct=((baseline_cost - serverless_cost) / baseline_cost) * 100,
            tradeoff="Fastest cold start",
        )
    )

    # Spot scenario (with fallback)
    spot_cost = baseline_cost * 0.6  # 40% savings for spot instances
    scenarios.append(
        ComputeScenario(
            compute_type="Jobs Compute + Spot",
            sku="JOBS_COMPUTE",
            estimated_cost_usd=spot_cost,
            savings_pct=((baseline_cost - spot_cost) / baseline_cost) * 100,
            tradeoff="Max savings (interruptible)",
        )
    )

    return scenarios


def _create_workload_profile(metrics: dict) -> WorkloadProfile:
    """Create workload profile from metrics."""
    peak_memory_pct = min(metrics.get("peak_memory_pct", 30.0), 100.0)
    peak_cpu_pct = min(metrics.get("peak_cpu_pct", 40.0), 100.0)
    spill_bytes = metrics.get("spill_to_disk_bytes", 0)

    return WorkloadProfile(
        peak_memory_pct=peak_memory_pct,
        peak_cpu_pct=peak_cpu_pct,
        spill_to_disk_bytes=spill_bytes,
        data_gb=metrics.get("read_bytes", 0) / 1024**3,
        shuffle_bytes=metrics.get("shuffle_bytes", 0),
        compute_intensity=min(peak_cpu_pct / 100.0, 1.0),
        memory_intensity=min(peak_memory_pct / 100.0, 1.0),
    )


def _create_cluster_recommendation(
    current: ClusterConfig, recommended: ClusterConfig, profile: WorkloadProfile
) -> ClusterRecommendation:
    """Create three-tier cluster recommendation."""
    from burnt.core.instances import AZURE_INSTANCE_CATALOG

    # Economy: smallest viable
    economy_instance = "Standard_DS3_v2"
    if economy_instance not in AZURE_INSTANCE_CATALOG:
        economy_instance = next(iter(AZURE_INSTANCE_CATALOG.keys()))

    # Performance: larger with headroom
    performance_instance = "Standard_DS5_v2"
    if performance_instance not in AZURE_INSTANCE_CATALOG:
        # Find largest instance
        performance_instance = max(
            AZURE_INSTANCE_CATALOG.keys(), key=lambda k: AZURE_INSTANCE_CATALOG[k].vcpus
        )

    economy = ClusterConfig(
        instance_type=economy_instance,
        num_workers=max(2, recommended.num_workers - 1),
        dbu_per_hour=AZURE_INSTANCE_CATALOG[economy_instance].dbu_rate,
        sku="JOBS_COMPUTE",
        spot_policy="SPOT_WITH_ON_DEMAND_FALLBACK",
    )

    balanced = recommended

    performance = ClusterConfig(
        instance_type=performance_instance,
        num_workers=min(8, recommended.num_workers + 2),
        dbu_per_hour=AZURE_INSTANCE_CATALOG[performance_instance].dbu_rate,
        sku="JOBS_COMPUTE",
        spot_policy="ON_DEMAND",  # Performance prefers reliability
    )

    current_cost = (
        current.dbu_per_hour * float(get_dbu_rate(current.sku)) * 1.0
    )  # 1 hour

    rationale = _generate_rationale(profile, current, recommended)

    return ClusterRecommendation(
        economy=economy,
        balanced=balanced,
        performance=performance,
        current_cost_usd=float(current_cost),
        rationale=rationale,
    )


def _generate_insights(
    metrics: dict, current: ClusterConfig, recommended: ClusterConfig
) -> list[str]:
    """Generate actionable insights from metrics."""
    insights = []

    peak_memory = metrics.get("peak_memory_pct", 0.0)
    if peak_memory < 30.0:
        insights.append(
            f"Peak memory utilization was {peak_memory:.1f}%. Downsizing could save costs."
        )

    spill_bytes = metrics.get("spill_to_disk_bytes", 0)
    if spill_bytes > 0:
        insights.append(
            f"Query spilled {spill_bytes / 1024**2:.1f} MB to disk. Consider increasing memory."
        )

    if current.instance_type != recommended.instance_type:
        insights.append(
            f"Switch from {current.instance_type} to {recommended.instance_type} for better cost/performance."
        )

    if current.num_workers != recommended.num_workers:
        insights.append(
            f"Adjust worker count from {current.num_workers} to {recommended.num_workers}."
        )

    if not insights:
        insights.append(
            "Your current configuration is well-sized. Consider spot instances for additional savings."
        )

    return insights


def _fetch_metrics_from_history(
    backend: Backend, run_id: str | None, statement_id: str | None
) -> dict:
    """Fetch metrics from system.query.history."""
    if statement_id:
        # Look up specific statement
        sql = f"""
        SELECT
            statement_id,
            statement_text,
            execution_duration_ms,
            compilation_duration_ms,
            read_bytes,
            read_rows,
            produced_rows,
            written_bytes,
            total_task_duration_ms,
            compute.cluster_id AS cluster_id
        FROM system.query.history
        WHERE statement_id = '{statement_id}'
        LIMIT 1
        """
    else:
        # Look up by run ID (approximate)
        sql = f"""
        SELECT
            statement_id,
            statement_text,
            execution_duration_ms,
            compilation_duration_ms,
            read_bytes,
            read_rows,
            produced_rows,
            written_bytes,
            total_task_duration_ms,
            compute.cluster_id AS cluster_id
        FROM system.query.history
        WHERE statement_text LIKE '%{run_id}%'
        ORDER BY start_time DESC
        LIMIT 1
        """

    try:
        results = backend.execute_sql(sql)
        if not results:
            if statement_id:
                identifier = f"statement {statement_id}"
            else:
                # run_id is guaranteed to be not None here because of earlier check
                identifier = f"run {run_id}"
            raise ValueError(f"No query history found for {identifier}")

        row = results[0]
        return {
            "duration_ms": row.get("execution_duration_ms", 0),
            "read_bytes": row.get("read_bytes", 0),
            "cluster_id": row.get("cluster_id"),
            "peak_memory_pct": 30.0,  # Default assumption
            "peak_cpu_pct": 40.0,  # Default assumption
            "spill_to_disk_bytes": 0,  # Not available in query.history
            "utilization_pct": 50.0,  # Default assumption
        }
    except Exception as e:
        raise RuntimeError(
            f"Failed to fetch metrics from system.query.history: {e}. "
            "Make sure you have access to system tables."
        ) from e


def _infer_cluster_from_metrics(metrics: dict) -> ClusterConfig:
    """Infer cluster configuration from metrics."""
    # Default to typical test cluster
    return ClusterConfig(
        instance_type="Standard_DS4_v2",
        num_workers=2,
        dbu_per_hour=1.5,
        sku="ALL_PURPOSE",
        photon_enabled=False,
        spot_policy="ON_DEMAND",
    )


def _generate_rationale(
    profile: WorkloadProfile, current: ClusterConfig, recommended: ClusterConfig
) -> str:
    """Generate rationale for cluster recommendation."""
    rationales = []

    if profile.peak_memory_pct < 30 and profile.peak_cpu_pct < 40:
        rationales.append("Workload is under-utilizing resources.")
    elif profile.peak_memory_pct > 70 or profile.peak_cpu_pct > 70:
        rationales.append("Workload is nearing resource limits.")

    if profile.spill_to_disk_bytes > 0:
        rationales.append("Disk spill indicates memory pressure.")

    if current.instance_type != recommended.instance_type:
        rationales.append("Instance type change for better fit.")

    if current.num_workers != recommended.num_workers:
        rationales.append("Worker count adjustment for optimal parallelism.")

    if not rationales:
        rationales.append("Balanced configuration for typical workloads.")

    return " ".join(rationales)
