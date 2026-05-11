"""AWS Databricks PricingBackend — live EC2 prices + DBU rates."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from burnt.graph.model import CostGraph

_log = logging.getLogger(__name__)

# Photon multiplier for AWS: 2.5x standard DBU rate
PHOTON_MULTIPLIER_AWS = Decimal("2.5")

# Default DBU rate for ALL_PURPOSE on AWS (fallback when system tables unavailable)
_FALLBACK_DBU_RATE = Decimal("0.55")


def _fetch_live_dbu_rates(
    workspace_url: str, token: str, warehouse_id: str
) -> dict[str, Decimal]:
    """Fetch current DBU rates from system.billing.list_prices."""
    import requests

    sql = """
        SELECT sku_name, pricing.effective_list.default AS price
        FROM system.billing.list_prices
        WHERE price_end_time IS NULL
    """
    resp = requests.post(
        f"{workspace_url}/api/2.0/sql/statements",
        headers={"Authorization": f"Bearer {token}"},
        json={"warehouse_id": warehouse_id, "statement": sql, "wait_timeout": "10s"},
        timeout=15,
    )
    resp.raise_for_status()
    rows = resp.json().get("result", {}).get("data_array", [])
    return {row[0].upper(): Decimal(str(row[1])) for row in rows}


class AwsDatabricksBackend:
    """PricingBackend for Databricks on AWS.

    EC2 rates: AWS public pricing JSON API (no auth, 24 h cache).
    DBU rates: system.billing.list_prices when workspace credentials are
    provided, otherwise falls back to bundled defaults.
    """

    name = "aws-databricks"

    def __init__(
        self,
        region: str = "us-east-1",
        workspace_url: str | None = None,
        token: str | None = None,
        warehouse_id: str | None = None,
        instance_type: str = "m5.xlarge",
    ) -> None:
        self._region = region
        self._workspace_url = workspace_url
        self._token = token
        self._warehouse_id = warehouse_id
        self._instance_type = instance_type
        self._dbu_rates: dict[str, Decimal] | None = None

    def _resolve_dbu_rates(self) -> dict[str, Decimal]:
        if self._dbu_rates is not None:
            return self._dbu_rates

        if self._workspace_url and self._token and self._warehouse_id:
            try:
                self._dbu_rates = _fetch_live_dbu_rates(
                    self._workspace_url, self._token, self._warehouse_id
                )
                return self._dbu_rates
            except Exception:
                _log.info(
                    "system.billing.list_prices unavailable; using fallback DBU rates"
                )

        self._dbu_rates = {"ALL_PURPOSE": _FALLBACK_DBU_RATE}
        return self._dbu_rates

    def map(self, graph: CostGraph) -> object:
        """Map CostGraph to CostEstimate using AWS EC2 + DBU rates."""
        from burnt.core.models import CostEstimate

        if not graph.nodes:
            return CostEstimate(costs={}, confidence="low")

        dbu_rates = self._resolve_dbu_rates()

        try:
            from ._prices import get_ec2_price_usd

            ec2_price_per_hour = get_ec2_price_usd(self._instance_type, self._region)
        except Exception as exc:
            _log.warning("Could not fetch EC2 price for %s: %s", self._instance_type, exc)
            ec2_price_per_hour = Decimal("0")

        sku_rate = dbu_rates.get("ALL_PURPOSE", _FALLBACK_DBU_RATE)
        breakdown: dict[str, float] = {}
        total_usd = Decimal("0")

        for node in graph.nodes:
            if node.estimated_input_bytes is None:
                continue

            data_gb = Decimal(str(node.estimated_input_bytes)) / Decimal("1073741824")
            run_hours = data_gb / Decimal("3600")  # 1 GB/s heuristic

            dbu_per_hour = Decimal("1")
            if node.photon_eligible:
                dbu_per_hour = dbu_per_hour * PHOTON_MULTIPLIER_AWS

            node_dbu_cost = run_hours * dbu_per_hour * sku_rate
            node_ec2_cost = run_hours * ec2_price_per_hour
            node_total = node_dbu_cost + node_ec2_cost
            total_usd += node_total
            if float(node_total):
                breakdown[node.id] = float(node_total)

        costs = {"USD": float(round(total_usd, 6))} if total_usd else {}
        confidence = "medium" if costs else "low"

        return CostEstimate(
            costs=costs,
            confidence=confidence,
            breakdown=breakdown,
        )
