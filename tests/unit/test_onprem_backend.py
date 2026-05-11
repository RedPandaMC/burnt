"""Unit tests for OnPremSparkBackend."""

import pytest

from burnt.cloud.onprem_spark import OnPremSparkBackend
from burnt.core.config import OnPremSparkSettings
from burnt.core.enums import NodeKind, ScalingType
from burnt.graph.model import CostGraph, CostNode


def _node(
    node_id: str,
    input_bytes: int | None = None,
    driver_bound: bool = False,
    shuffle_required: bool = False,
) -> CostNode:
    return CostNode(
        id=node_id,
        kind=NodeKind.READ,
        scaling_type=ScalingType.LINEAR,
        estimated_input_bytes=input_bytes,
        driver_bound=driver_bound,
        shuffle_required=shuffle_required,
    )


class TestOnPremSparkBackend:
    def test_name_is_onprem_spark(self):
        backend = OnPremSparkBackend(OnPremSparkSettings(cost_per_vcpu_hour=0.05))
        assert backend.name == "onprem-spark"

    def test_backend_name_is_string(self):
        backend = OnPremSparkBackend(OnPremSparkSettings(cost_per_vcpu_hour=0.05))
        assert isinstance(backend.name, str)

    def test_raises_config_error_if_vcpu_rate_missing(self):
        from burnt.core.exceptions import ConfigError

        with pytest.raises(ConfigError, match="cost_per_vcpu_hour"):
            OnPremSparkBackend(OnPremSparkSettings())

    def test_empty_graph_returns_zero_cost(self):
        backend = OnPremSparkBackend(OnPremSparkSettings(cost_per_vcpu_hour=0.05))
        graph = CostGraph()
        result = backend.map(graph)
        assert result.costs == {}

    def test_confidence_low_without_input_bytes(self):
        backend = OnPremSparkBackend(OnPremSparkSettings(cost_per_vcpu_hour=0.05))
        graph = CostGraph()
        graph.add_node(_node("n1", input_bytes=None))
        result = backend.map(graph)
        assert result.confidence == "low"

    def test_confidence_medium_with_input_bytes(self):
        backend = OnPremSparkBackend(OnPremSparkSettings(cost_per_vcpu_hour=0.05))
        graph = CostGraph()
        # 1 GiB of data
        graph.add_node(_node("n1", input_bytes=1_073_741_824))
        result = backend.map(graph)
        assert result.confidence == "medium"

    def test_input_bytes_produces_cpu_cost(self):
        backend = OnPremSparkBackend(OnPremSparkSettings(cost_per_vcpu_hour=3600.0))
        graph = CostGraph()
        # 3600 GiB → 3600s / 1 GB/s → 1 hour → 1 vCPU * $3600/h = $3600
        graph.add_node(_node("n1", input_bytes=3600 * 1_073_741_824))
        result = backend.map(graph)
        assert "USD" in result.costs
        assert result.costs["USD"] > 0
        assert "cpu" in result.breakdown

    def test_memory_rate_adds_memory_cost(self):
        backend = OnPremSparkBackend(
            OnPremSparkSettings(
                cost_per_vcpu_hour=0.048,
                cost_per_gb_hour=0.006,
            )
        )
        graph = CostGraph()
        graph.add_node(_node("n1", input_bytes=1_073_741_824))
        result = backend.map(graph)
        assert "memory" in result.breakdown
        assert result.breakdown["memory"] > 0

    def test_shuffle_node_adds_shuffle_cost(self):
        backend = OnPremSparkBackend(
            OnPremSparkSettings(
                cost_per_vcpu_hour=0.048,
                cost_per_gb_shuffle=0.001,
            )
        )
        graph = CostGraph()
        graph.add_node(
            _node("n1", input_bytes=1_073_741_824, shuffle_required=True)
        )
        result = backend.map(graph)
        assert "shuffle" in result.breakdown
        assert result.breakdown["shuffle"] > 0

    def test_driver_bound_nodes_excluded_from_data_sum(self):
        backend = OnPremSparkBackend(OnPremSparkSettings(cost_per_vcpu_hour=0.05))
        graph = CostGraph()
        # Driver node should not contribute to data scanned
        graph.add_node(_node("driver", input_bytes=99_999_999_999, driver_bound=True))
        result = backend.map(graph)
        assert result.costs == {}
        assert result.confidence == "low"

    def test_no_shuffle_cost_when_no_shuffle_nodes(self):
        backend = OnPremSparkBackend(
            OnPremSparkSettings(
                cost_per_vcpu_hour=0.048,
                cost_per_gb_shuffle=0.001,
            )
        )
        graph = CostGraph()
        graph.add_node(_node("n1", input_bytes=1_073_741_824, shuffle_required=False))
        result = backend.map(graph)
        assert "shuffle" not in result.breakdown
