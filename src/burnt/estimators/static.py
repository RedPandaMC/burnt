from datetime import date
from decimal import Decimal
from typing import Literal

from ..core.exchange import FrankfurterProvider
from ..core.models import ClusterConfig, CostEstimate, QueryProfile
from ..core.pricing import get_dbu_rate
from ..parsers.pyspark import analyze_pyspark
from ..parsers.sql import analyze_query

MIN_DBU = 0.01


class CostEstimator:
    def __init__(
        self,
        cluster: ClusterConfig | None = None,
        target_currency: str = "USD",
        exchange_rate_provider: FrankfurterProvider | None = None,
    ):
        self.cluster = cluster or ClusterConfig()
        self.target_currency = target_currency
        self.exchange_rate = exchange_rate_provider or FrankfurterProvider()

    def estimate(
        self,
        query: str,
        language: str = "sql",
        cluster: ClusterConfig | None = None,
    ) -> CostEstimate:
        cluster = cluster or self.cluster
        profile: QueryProfile | None = None

        if language == "sql":
            profile = analyze_query(query)
            complexity = profile.complexity_score
        else:
            ops = analyze_pyspark(query)
            complexity = sum(op.weight for op in ops)

        cluster_factor = cluster.num_workers * cluster.dbu_per_hour

        throughput_bps = cluster.num_workers * 3.2e9
        scan_bytes = complexity * 1e9
        scan_sec = scan_bytes / throughput_bps if throughput_bps > 0 else 0
        shuffle_sec = complexity * 5.0
        estimated_sec = scan_sec + shuffle_sec
        estimated_dbu = max((estimated_sec / 3600) * cluster.dbu_per_hour, MIN_DBU)

        if cluster.photon_enabled:
            estimated_dbu = estimated_dbu * 2.5 / 2.7

        estimated_cost_usd = float(
            Decimal(str(estimated_dbu)) * get_dbu_rate(cluster.sku)
        )

        estimated_cost_eur = None
        if self.target_currency != "USD":
            estimated_cost_eur = self.exchange_rate.get_rate_for_amount(
                Decimal(str(estimated_cost_usd)),
                date.today(),
                "USD",
                self.target_currency,
            )

        return CostEstimate(
            estimated_dbu=round(estimated_dbu, 2),
            estimated_cost_usd=round(estimated_cost_usd, 4),
            estimated_cost_eur=round(float(estimated_cost_eur), 4)
            if estimated_cost_eur
            else None,
            confidence=self._compute_confidence(profile),
            breakdown={"complexity": complexity, "cluster_factor": cluster_factor},
            warnings=[],
        )

    def _compute_confidence(
        self, profile: QueryProfile | None
    ) -> Literal["low", "medium", "high"]:
        if profile is None:
            return "low"
        if not profile.tables:
            return "low"
        if profile.complexity_score > 50:
            return "low"
        if profile.complexity_score > 20:
            return "medium"
        return "high"


def estimate_cost(
    query: str,
    cluster: ClusterConfig | None = None,
    language: str = "sql",
    target_currency: str = "USD",
) -> CostEstimate:
    estimator = CostEstimator(cluster=cluster, target_currency=target_currency)
    return estimator.estimate(query, language=language)
