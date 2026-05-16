"""On-premises Spark pricing backend — pure Python, zero external deps."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from ..base import InstanceSpec, register_backend
from ..exchange import FrankfurterProvider
from .config import OnPremConfig

if TYPE_CHECKING:
    from burnt.core.models import CostEstimate


def compute_onprem_cost(
    compute_seconds: float,
    *,
    total_vcpus: int,
    total_memory_gb: float,
    shuffle_bytes: int = 0,
    config: OnPremConfig | None = None,
) -> tuple[float, dict[str, float]]:
    """Compute on-prem Spark cost from raw metrics.

    Formula:
        vcpu_cost  = vcpu_hours  * cost_per_vcpu_hour
        mem_cost   = gb_hours    * cost_per_gb_hour
        shuffle_cost = shuffle_gb * cost_per_gb_shuffle
        subtotal   = vcpu_cost + mem_cost + shuffle_cost
        total      = subtotal * (1 + overhead_pct / 100)
    """
    cfg = config or OnPremConfig()

    vcpu_hours = (compute_seconds / 3600.0) * total_vcpus
    gb_hours = (compute_seconds / 3600.0) * total_memory_gb
    shuffle_gb = shuffle_bytes / 1e9

    vcpu_cost = vcpu_hours * cfg.cost_per_vcpu_hour
    mem_cost = gb_hours * cfg.cost_per_gb_hour
    shuffle_cost = shuffle_gb * cfg.cost_per_gb_shuffle
    subtotal = vcpu_cost + mem_cost + shuffle_cost
    overhead_multiplier = 1 + cfg.datacenter_overhead_pct / 100
    total = subtotal * overhead_multiplier

    breakdown = {
        "vcpu_hours": round(vcpu_hours, 4),
        "gb_hours": round(gb_hours, 4),
        "shuffle_gb": round(shuffle_gb, 4),
        "vcpu_cost": round(vcpu_cost, 6),
        "mem_cost": round(mem_cost, 6),
        "shuffle_cost": round(shuffle_cost, 6),
        "datacenter_overhead_pct": cfg.datacenter_overhead_pct,
    }

    return round(total, 6), breakdown


class OnPremSparkBackend:
    """On-premises Spark pricing backend — pure Python, zero external deps."""

    name = "onprem-spark"

    def __init__(
        self,
        total_vcpus: int = 16,
        total_memory_gb: float = 64.0,
        config: OnPremConfig | None = None,
    ):
        self.total_vcpus = total_vcpus
        self.total_memory_gb = total_memory_gb
        self.config = config or OnPremConfig()
        self._default_spec = InstanceSpec(
            instance_type="onprem",
            vcpus=total_vcpus,
            memory_gb=total_memory_gb,
            local_storage_gb=0.0,
            vm_cost_per_hour=0.0,
            category="general",
            dbu_rate=0.0,
        )

    def estimate(
        self,
        compute_seconds: float,
        *,
        instance_type: str | None = None,
        num_workers: int = 1,
        region: str | None = None,
        sku: str | None = None,
        photon_enabled: bool = False,
        spot_policy: str = "ON_DEMAND",
        shuffle_bytes: int = 0,
        currency: str = "USD",
    ) -> CostEstimate:
        from burnt.core.models import CostEstimate as CoreCostEstimate

        total_vcpus = self.total_vcpus * num_workers
        total_memory_gb = self.total_memory_gb * num_workers

        cost_usd, breakdown = compute_onprem_cost(
            compute_seconds,
            total_vcpus=total_vcpus,
            total_memory_gb=total_memory_gb,
            shuffle_bytes=shuffle_bytes,
            config=self.config,
        )

        result = CoreCostEstimate(
            costs={"USD": cost_usd},
            confidence="medium",
            breakdown=breakdown,
        )

        if currency != "USD":
            from datetime import date as _date

            exchange = FrankfurterProvider()
            converted = exchange.get_rate_for_amount(
                Decimal(str(cost_usd)),
                _date.today(),
                from_curr="USD",
                to_curr=currency,
            )
            result.costs[currency] = round(float(converted), 6)

        return result

    def resolve_instance(
        self, instance_type: str, region: str | None = None
    ) -> InstanceSpec | None:
        return self._default_spec

    def refresh_cache(self) -> None:
        pass

    def is_available(self) -> bool:
        return True


register_backend("onprem-spark", OnPremSparkBackend)
