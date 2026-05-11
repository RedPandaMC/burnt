"""Unit tests for AzureDatabricksBackend and Azure Retail Prices client."""

import sys
from decimal import Decimal
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

from burnt.cloud.azure_databricks import AzureDatabricksBackend
from burnt.core.enums import NodeKind, ScalingType
from burnt.graph.model import CostGraph, CostNode


def _node(
    node_id: str,
    input_bytes: int | None = None,
    photon_eligible: bool = False,
) -> CostNode:
    return CostNode(
        id=node_id,
        kind=NodeKind.READ,
        scaling_type=ScalingType.LINEAR,
        estimated_input_bytes=input_bytes,
        photon_eligible=photon_eligible,
    )


def _make_requests_mock(
    get_json: dict | None = None,
    post_json: dict | None = None,
    raise_on_get: Exception | None = None,
) -> ModuleType:
    """Build a fake `requests` module usable as sys.modules['requests']."""
    mod = ModuleType("requests")

    if raise_on_get:
        mod.get = MagicMock(side_effect=raise_on_get)  # type: ignore[attr-defined]
    else:
        get_resp = MagicMock()
        get_resp.json.return_value = get_json or {}
        get_resp.raise_for_status = MagicMock()
        mod.get = MagicMock(return_value=get_resp)  # type: ignore[attr-defined]

    if post_json is not None:
        post_resp = MagicMock()
        post_resp.json.return_value = post_json
        post_resp.raise_for_status = MagicMock()
        mod.post = MagicMock(return_value=post_resp)  # type: ignore[attr-defined]
    else:
        mod.post = MagicMock(side_effect=Exception("unexpected POST"))  # type: ignore[attr-defined]

    mod.Timeout = Exception  # type: ignore[attr-defined]
    return mod


# ---------------------------------------------------------------------------
# Azure Retail Prices client
# ---------------------------------------------------------------------------


class TestAzureRetailPricesClient:
    def setup_method(self):
        from burnt.cloud.azure_databricks._prices import _vm_cache

        _vm_cache.clear()

    def test_get_vm_price_returns_decimal(self):
        from burnt.cloud.azure_databricks._prices import get_vm_price_usd

        req = _make_requests_mock(
            get_json={"Items": [{"skuName": "Standard_DS3_v2", "retailPrice": 0.251}]}
        )
        with patch.dict(sys.modules, {"requests": req}):
            price = get_vm_price_usd("Standard_DS3_v2", "eastus")

        assert isinstance(price, Decimal)
        assert price == Decimal("0.251")

    def test_get_vm_price_caches_result(self):
        from burnt.cloud.azure_databricks._prices import get_vm_price_usd

        req = _make_requests_mock(
            get_json={"Items": [{"skuName": "Standard_DS3_v2", "retailPrice": 0.251}]}
        )
        with patch.dict(sys.modules, {"requests": req}):
            get_vm_price_usd("Standard_DS3_v2", "eastus")
            get_vm_price_usd("Standard_DS3_v2", "eastus")

        # Second call should hit the cache — only one HTTP call
        assert req.get.call_count == 1  # type: ignore[attr-defined]

    def test_get_vm_price_raises_pricing_error_on_not_found(self):
        from burnt.cloud.azure_databricks._prices import get_vm_price_usd
        from burnt.core.exceptions import PricingError

        req = _make_requests_mock(get_json={"Items": []})
        with patch.dict(sys.modules, {"requests": req}), pytest.raises(
            PricingError, match="No on-demand Linux price found"
        ):
            get_vm_price_usd("NonExistent_SKU", "eastus")

    def test_get_vm_price_raises_pricing_error_on_http_error(self):
        from burnt.cloud.azure_databricks._prices import get_vm_price_usd
        from burnt.core.exceptions import PricingError

        req = _make_requests_mock(raise_on_get=Exception("connection refused"))
        with patch.dict(sys.modules, {"requests": req}), pytest.raises(
            PricingError, match="Azure pricing API error"
        ):
            get_vm_price_usd("Standard_DS3_v2", "eastus")

    def test_windows_skus_are_filtered_out(self):
        from burnt.cloud.azure_databricks._prices import get_vm_price_usd
        from burnt.core.exceptions import PricingError

        req = _make_requests_mock(
            get_json={
                "Items": [
                    {"skuName": "Standard_DS3_v2 Windows", "retailPrice": 0.50},
                    {"skuName": "Standard_DS3_v2 Spot", "retailPrice": 0.10},
                ]
            }
        )
        with patch.dict(sys.modules, {"requests": req}), pytest.raises(
            PricingError, match="No on-demand Linux price found"
        ):
            get_vm_price_usd("Standard_DS3_v2", "eastus")


# ---------------------------------------------------------------------------
# AzureDatabricksBackend
# ---------------------------------------------------------------------------


class TestAzureDatabricksBackend:
    def setup_method(self):
        from burnt.cloud.azure_databricks._prices import _vm_cache

        _vm_cache.clear()

    def test_name_is_azure_databricks(self):
        backend = AzureDatabricksBackend()
        assert backend.name == "azure-databricks"

    def test_map_empty_graph_returns_empty_costs(self):
        backend = AzureDatabricksBackend()
        graph = CostGraph()
        result = backend.map(graph)
        assert result.costs == {}
        assert result.confidence == "low"

    def test_map_uses_live_vm_price_from_api(self):
        req = _make_requests_mock(
            get_json={"Items": [{"skuName": "Standard_DS3_v2", "retailPrice": 0.251}]}
        )
        backend = AzureDatabricksBackend(region="eastus")
        graph = CostGraph()
        graph.add_node(_node("n1", input_bytes=1_073_741_824))

        with patch.dict(sys.modules, {"requests": req}):
            result = backend.map(graph)

        assert "USD" in result.costs
        assert result.costs["USD"] > 0

    def test_map_falls_back_to_bundled_dbu_rates_when_no_workspace(self):
        req = _make_requests_mock(
            get_json={"Items": [{"skuName": "Standard_DS3_v2", "retailPrice": 0.251}]}
        )
        # No workspace credentials
        backend = AzureDatabricksBackend()
        graph = CostGraph()
        graph.add_node(_node("n1", input_bytes=1_073_741_824))

        with patch.dict(sys.modules, {"requests": req}):
            result = backend.map(graph)

        assert "USD" in result.costs
        assert result.costs["USD"] > 0

    def test_map_uses_live_dbu_rates_when_workspace_available(self):
        live_dbu_json = {
            "result": {
                "data_array": [
                    ["ALL_PURPOSE", "0.45"],
                    ["JOBS_COMPUTE", "0.25"],
                ]
            }
        }
        vm_json = {"Items": [{"skuName": "Standard_DS3_v2", "retailPrice": 0.251}]}
        req = _make_requests_mock(get_json=vm_json, post_json=live_dbu_json)

        backend = AzureDatabricksBackend(
            workspace_url="https://adb.example.com",
            token="tok",
            warehouse_id="wh1",
        )
        graph = CostGraph()
        graph.add_node(_node("n1", input_bytes=1_073_741_824))

        with patch.dict(sys.modules, {"requests": req}):
            result = backend.map(graph)

        assert "USD" in result.costs
        req.post.assert_called_once()  # type: ignore[attr-defined]

    def test_map_photon_multiplier_applied_when_photon_eligible(self):
        vm_json = {"Items": [{"skuName": "Standard_DS3_v2", "retailPrice": 0.251}]}
        req = _make_requests_mock(get_json=vm_json)

        backend_std = AzureDatabricksBackend()
        backend_photon = AzureDatabricksBackend()

        graph_std = CostGraph()
        graph_std.add_node(_node("n1", input_bytes=1_073_741_824, photon_eligible=False))

        graph_photon = CostGraph()
        graph_photon.add_node(
            _node("n1", input_bytes=1_073_741_824, photon_eligible=True)
        )

        with patch.dict(sys.modules, {"requests": req}):
            std_result = backend_std.map(graph_std)
            self.setup_method()  # clear cache between calls
            photon_result = backend_photon.map(graph_photon)

        assert photon_result.costs.get("USD", 0) > std_result.costs.get("USD", 0)

    def test_map_continues_when_vm_price_unavailable(self):
        """Backend must not raise if the Azure pricing API is unreachable."""
        req = _make_requests_mock(raise_on_get=Exception("network down"))
        backend = AzureDatabricksBackend()
        graph = CostGraph()
        graph.add_node(_node("n1", input_bytes=1_073_741_824))

        with patch.dict(sys.modules, {"requests": req}):
            result = backend.map(graph)

        # DBU cost alone is non-zero even without VM contribution
        assert "USD" in result.costs
