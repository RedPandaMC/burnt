"""Unit tests for src/burnt/tables/billing.py."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from burnt.core.exceptions import DatabricksQueryError
from burnt.core.models import UsageRecord
from burnt.tables.billing import (
    _coerce_usage_row,
    get_historical_usage,
    get_live_prices,
)

_WAREHOUSE_ID = "test-warehouse-123"


def _make_usage_row(
    account_id: str = "acc1",
    workspace_id: str = "ws1",
    sku_name: str = "STANDARD_ALL_PURPOSE_COMPUTE",
    cloud: str = "AZURE",
    usage_start_time: str = "2026-02-05T00:00:00Z",
    usage_end_time: str = "2026-02-05T01:00:00Z",
    usage_quantity: str = "1.5",
    usage_unit: str = "DBU",
    cluster_id: str | None = "cluster-abc",
    warehouse_id: str | None = None,
) -> dict:
    """Build a raw API row dict resembling a system.billing.usage row."""
    return {
        "account_id": account_id,
        "workspace_id": workspace_id,
        "sku_name": sku_name,
        "cloud": cloud,
        "usage_start_time": usage_start_time,
        "usage_end_time": usage_end_time,
        "usage_quantity": usage_quantity,
        "usage_unit": usage_unit,
        "cluster_id": cluster_id,
        "warehouse_id": warehouse_id,
    }


# ---------------------------------------------------------------------------
# get_historical_usage
# ---------------------------------------------------------------------------


class TestGetHistoricalUsage:
    """Tests for get_historical_usage()."""

    def test_returns_usage_records_for_two_rows(self) -> None:
        """Two mocked rows should be returned as UsageRecord instances."""
        client = MagicMock()
        client.execute_sql.return_value = [
            _make_usage_row(account_id="acc1", usage_quantity="2.0"),
            _make_usage_row(account_id="acc2", usage_quantity="3.5"),
        ]

        records = get_historical_usage(client, _WAREHOUSE_ID, days=7)

        assert len(records) == 2
        assert all(isinstance(r, UsageRecord) for r in records)
        assert records[0].account_id == "acc1"
        assert records[0].usage_quantity == Decimal("2.0")
        assert records[1].account_id == "acc2"
        assert records[1].usage_quantity == Decimal("3.5")

    def test_sql_contains_days_parameter(self) -> None:
        """SQL sent to client must reference the days argument."""
        client = MagicMock()
        client.execute_sql.return_value = []

        get_historical_usage(client, _WAREHOUSE_ID, days=14)

        call_args = client.execute_sql.call_args
        sql_sent: str = call_args[0][0]
        assert "-14" in sql_sent
        assert "system.billing.usage" in sql_sent

    def test_empty_result_returns_empty_list(self) -> None:
        """Empty result from client should return empty list."""
        client = MagicMock()
        client.execute_sql.return_value = []

        records = get_historical_usage(client, _WAREHOUSE_ID)

        assert records == []

    def test_propagates_databricks_query_error(self) -> None:
        """DatabricksQueryError raised by client should propagate unchanged."""
        client = MagicMock()
        client.execute_sql.side_effect = DatabricksQueryError("statement failed")

        with pytest.raises(DatabricksQueryError, match="statement failed"):
            get_historical_usage(client, _WAREHOUSE_ID)

    def test_optional_fields_are_none(self) -> None:
        """cluster_id and warehouse_id may be None."""
        client = MagicMock()
        client.execute_sql.return_value = [
            _make_usage_row(cluster_id=None, warehouse_id=None),
        ]

        records = get_historical_usage(client, _WAREHOUSE_ID)

        assert records[0].cluster_id is None
        assert records[0].warehouse_id is None


# ---------------------------------------------------------------------------
# get_live_prices
# ---------------------------------------------------------------------------


class TestGetLivePrices:
    """Tests for get_live_prices()."""

    def test_returns_price_dict_for_two_skus(self) -> None:
        """Two mocked price rows should produce a dict keyed by sku_name."""
        client = MagicMock()
        client.execute_sql.return_value = [
            {"sku_name": "STANDARD_ALL_PURPOSE_COMPUTE", "price_usd": "0.07"},
            {"sku_name": "PREMIUM_SQL_COMPUTE", "price_usd": "0.22"},
        ]

        prices = get_live_prices(
            client,
            _WAREHOUSE_ID,
            ["STANDARD_ALL_PURPOSE_COMPUTE", "PREMIUM_SQL_COMPUTE"],
        )

        assert len(prices) == 2
        assert prices["STANDARD_ALL_PURPOSE_COMPUTE"] == Decimal("0.07")
        assert prices["PREMIUM_SQL_COMPUTE"] == Decimal("0.22")
        assert all(isinstance(v, Decimal) for v in prices.values())

    def test_empty_result_returns_empty_dict(self) -> None:
        """Empty result from client should return empty dict."""
        client = MagicMock()
        client.execute_sql.return_value = []

        prices = get_live_prices(client, _WAREHOUSE_ID, ["SOME_SKU"])

        assert prices == {}

    def test_rows_missing_price_usd_are_skipped(self) -> None:
        """Rows where price_usd is None should be excluded from the result."""
        client = MagicMock()
        client.execute_sql.return_value = [
            {"sku_name": "VALID_SKU", "price_usd": "0.10"},
            {"sku_name": "MISSING_PRICE_SKU", "price_usd": None},
            {"sku_name": "NO_KEY_SKU"},
        ]

        prices = get_live_prices(
            client, _WAREHOUSE_ID, ["VALID_SKU", "MISSING_PRICE_SKU", "NO_KEY_SKU"]
        )

        assert list(prices.keys()) == ["VALID_SKU"]
        assert prices["VALID_SKU"] == Decimal("0.10")

    def test_sql_contains_sku_names(self) -> None:
        """SQL sent to client must include the requested SKU names."""
        client = MagicMock()
        client.execute_sql.return_value = []

        get_live_prices(client, _WAREHOUSE_ID, ["SKU_A", "SKU_B"])

        call_args = client.execute_sql.call_args
        sql_sent: str = call_args[0][0]
        assert "'SKU_A'" in sql_sent
        assert "'SKU_B'" in sql_sent
        assert "system.billing.list_prices" in sql_sent
        assert "price_end_time IS NULL" in sql_sent

    def test_propagates_databricks_query_error(self) -> None:
        """DatabricksQueryError raised by client should propagate unchanged."""
        client = MagicMock()
        client.execute_sql.side_effect = DatabricksQueryError("prices query failed")

        with pytest.raises(DatabricksQueryError, match="prices query failed"):
            get_live_prices(client, _WAREHOUSE_ID, ["SKU_A"])


# ---------------------------------------------------------------------------
# _coerce_usage_row
# ---------------------------------------------------------------------------


class TestCoerceUsageRow:
    """Tests for the internal _coerce_usage_row helper."""

    def test_coerces_normal_row(self) -> None:
        """Normal row should be coerced with Decimal usage_quantity."""
        raw = _make_usage_row(usage_quantity="4.25")
        result = _coerce_usage_row(raw)

        assert result["usage_quantity"] == Decimal("4.25")
        assert isinstance(result["usage_quantity"], Decimal)
        assert result["account_id"] == "acc1"

    def test_none_optional_fields_pass_through(self) -> None:
        """None values for cluster_id and warehouse_id should remain None."""
        raw = _make_usage_row(cluster_id=None, warehouse_id=None)
        result = _coerce_usage_row(raw)

        assert result["cluster_id"] is None
        assert result["warehouse_id"] is None

    def test_missing_keys_default_to_empty_string_or_zero(self) -> None:
        """Completely missing keys should default gracefully."""
        result = _coerce_usage_row({})

        assert result["account_id"] == ""
        assert result["sku_name"] == ""
        assert result["usage_quantity"] == Decimal("0")
        assert result["cluster_id"] is None
        assert result["warehouse_id"] is None
