"""GCP Databricks pricing backend."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from ..base import InstanceSpec, register_backend
from ..compute_units import ComputeComponents
from ..exchange import FrankfurterProvider
from .catalog import _get_api_key, load_catalog
from .rates import DBU_RATES, PHOTON_MULTIPLIER

if TYPE_CHECKING:
    from burnt.core.models import CostEstimate


class GcpDatabricksBackend:
    name = "gcp-databricks"

    def __init__(self, region: str = "us-central1"):
        self.region = region

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

        spec = self.resolve_instance(instance_type or "n1-standard-4", region)
        if spec is None:
            return CoreCostEstimate(
                confidence="none",
                warnings=["Unknown instance type — cannot estimate cost"],
            )

        components = ComputeComponents.from_raw(
            compute_seconds,
            instance_spec=spec,
            num_workers=num_workers,
            shuffle_bytes=shuffle_bytes,
        )

        dbu_rate = DBU_RATES.get(sku or "ALL_PURPOSE", 0.50)
        if photon_enabled:
            dbu_rate *= PHOTON_MULTIPLIER

        executor_hours = components.executor_hours()
        total_dbu = executor_hours * dbu_rate
        cost_usd = total_dbu * spec.dbu_rate

        breakdown: dict[str, float | str] = {
            "executor_hours": round(executor_hours, 4),
            "dbu_consumed": round(total_dbu, 4),
            "dbu_rate": dbu_rate,
            "instance_type": spec.instance_type,
        }

        confidence: str = "medium" if spec.vm_cost_per_hour > 0 else "low"
        result = CoreCostEstimate(
            estimated_dbu=total_dbu,
            costs={"USD": round(cost_usd, 6)},
            confidence=confidence,
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
        catalog = load_catalog()
        return catalog.get(instance_type)

    def refresh_cache(self) -> None:
        load_catalog(force_refresh=True)

    def is_available(self) -> bool:
        return _get_api_key() is not None


register_backend("gcp-databricks", GcpDatabricksBackend)
