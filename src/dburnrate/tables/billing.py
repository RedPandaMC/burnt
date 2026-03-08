"""Billing system table queries for system.billing.usage and system.billing.list_prices."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .connection import DatabricksClient

from ..core.models import UsageRecord
from .connection import _sanitize_id

_USAGE_COLUMNS = (
    "account_id, workspace_id, sku_name, cloud, usage_start_time, usage_end_time,"
    " usage_quantity, usage_unit,"
    " usage_metadata.cluster_id AS cluster_id,"
    " usage_metadata.warehouse_id AS warehouse_id"
)
_PRICES_COLUMNS = (
    "sku_name, cloud, currency_code, pricing.default AS price_usd,"
    " price_start_time, price_end_time"
)


def get_historical_usage(
    client: DatabricksClient, warehouse_id: str, days: int = 30
) -> list[UsageRecord]:
    """Fetch historical DBU usage for the past N days from system.billing.usage."""
    sql = f"""
        SELECT {_USAGE_COLUMNS}
        FROM system.billing.usage
        WHERE usage_start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        ORDER BY usage_start_time DESC
    """
    rows = client.execute_sql(sql, warehouse_id)
    return [UsageRecord(**_coerce_usage_row(row)) for row in rows]


def get_live_prices(
    client: DatabricksClient, warehouse_id: str, sku_names: list[str]
) -> dict[str, Decimal]:
    """Fetch current USD prices per DBU for the given SKU names from system.billing.list_prices."""
    sanitized_skus = [_sanitize_id(s, "sku_name") for s in sku_names]
    placeholders = ", ".join(f"'{s}'" for s in sanitized_skus)
    sql = f"""
        SELECT {_PRICES_COLUMNS}
        FROM system.billing.list_prices
        WHERE sku_name IN ({placeholders})
          AND price_end_time IS NULL
    """
    rows = client.execute_sql(sql, warehouse_id)
    return {
        row["sku_name"]: Decimal(str(row["price_usd"]))
        for row in rows
        if row.get("price_usd") is not None
    }


def _coerce_usage_row(row: dict[str, Any]) -> dict[str, Any]:
    """Coerce raw API row dict to UsageRecord-compatible types."""
    return {
        "account_id": row.get("account_id", ""),
        "workspace_id": row.get("workspace_id", ""),
        "sku_name": row.get("sku_name", ""),
        "cloud": row.get("cloud", ""),
        "usage_start_time": row.get("usage_start_time", ""),
        "usage_end_time": row.get("usage_end_time", ""),
        "usage_quantity": Decimal(str(row.get("usage_quantity", "0"))),
        "usage_unit": row.get("usage_unit", ""),
        "cluster_id": row.get("cluster_id"),
        "warehouse_id": row.get("warehouse_id"),
    }
