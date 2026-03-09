from decimal import Decimal

from ..core.models import ClusterConfig, CostEstimate

SPEEDUP_FACTORS = {
    "complex_join": 2.7,
    "aggregation": 4.0,
    "window": 2.5,
    "simple_insert": 1.0,
}

PHOTON_COST_MULTIPLIER = Decimal("2.5")


def apply_photon_scenario(
    estimate: CostEstimate,
    query_type: str = "complex_join",
) -> CostEstimate:
    speedup = SPEEDUP_FACTORS.get(query_type, 2.0)

    new_dbu = estimate.estimated_dbu * float(PHOTON_COST_MULTIPLIER) / speedup
    new_cost = estimate.estimated_cost_usd * float(PHOTON_COST_MULTIPLIER) / speedup

    savings_pct = (
        (estimate.estimated_cost_usd - new_cost) / estimate.estimated_cost_usd * 100
    )

    warnings = estimate.warnings.copy()
    if savings_pct < 0:
        warnings.append(
            f"Photon increases cost by {-savings_pct:.1f}% for {query_type}"
        )

    return CostEstimate(
        estimated_dbu=round(new_dbu, 2),
        estimated_cost_usd=round(new_cost, 4),
        confidence="medium",
        breakdown={**estimate.breakdown, "photon": True, "speedup": speedup},
        warnings=warnings,
    )


def apply_cluster_resize(
    estimate: CostEstimate,
    current_cluster: ClusterConfig,
    new_cluster: ClusterConfig,
) -> CostEstimate:
    current_factor = current_cluster.num_workers * current_cluster.dbu_per_hour
    new_factor = new_cluster.num_workers * new_cluster.dbu_per_hour

    ratio = new_factor / current_factor
    new_cost = estimate.estimated_cost_usd * ratio

    savings_pct = (
        (estimate.estimated_cost_usd - new_cost) / estimate.estimated_cost_usd * 100
    )

    return CostEstimate(
        estimated_dbu=estimate.estimated_dbu,
        estimated_cost_usd=round(new_cost, 4),
        confidence="medium",
        breakdown={**estimate.breakdown, "cluster_resize_ratio": ratio},
        warnings=[f"Estimated savings: {savings_pct:.1f}%"],
    )


def apply_serverless_migration(
    estimate: CostEstimate,
    current_sku: str = "ALL_PURPOSE",
    utilization_pct: float = 50.0,
) -> CostEstimate:
    serverless_rates = {
        "ALL_PURPOSE": 0.95,
        "JOBS_COMPUTE": 0.45,
        "SQL_PRO": 0.70,
    }

    classic_rates = {
        "ALL_PURPOSE": 0.55,
        "JOBS_COMPUTE": 0.30,
        "SQL_PRO": 0.55,
    }

    serverless_rate = serverless_rates.get(current_sku, 0.70)
    classic_rate = classic_rates.get(current_sku, 0.55)

    if utilization_pct < 30:
        ratio = serverless_rate / classic_rate
    else:
        effective_classic = classic_rate * (utilization_pct / 100)
        ratio = serverless_rate / effective_classic

    new_cost = estimate.estimated_cost_usd * ratio

    return CostEstimate(
        estimated_dbu=estimate.estimated_dbu,
        estimated_cost_usd=round(new_cost, 4),
        confidence="low",
        breakdown={
            **estimate.breakdown,
            "serverless": True,
            "utilization": utilization_pct,
        },
        warnings=[
            f"Serverless is {'cheaper' if ratio < 1 else 'more expensive'} at {utilization_pct}% utilization"
        ],
    )
