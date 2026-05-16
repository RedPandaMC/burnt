"""PyO3 boundary tests for the Rust physical-plan parser."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from burnt._engine import parse_physical_plan

_FIXTURES = Path(__file__).parent.parent / "fixtures" / "plans"


def _load(name: str) -> str:
    return (_FIXTURES / name).read_text(encoding="utf-8")


class TestEmptyAndMalformed:
    def test_empty_string(self) -> None:
        assert parse_physical_plan("") == []

    def test_empty_object(self) -> None:
        assert parse_physical_plan("{}") == []

    def test_unknown_input(self) -> None:
        assert parse_physical_plan("not json") == []

    def test_wrong_type(self) -> None:
        # `nodes` is a string instead of an array — must not raise.
        assert parse_physical_plan(json.dumps({"nodes": "wrong"})) == []


class TestCanonicalShape:
    def test_sort_exchange_scan(self) -> None:
        nodes = parse_physical_plan(_load("sort_exchange.json"))
        assert len(nodes) == 3

        by_name = {n.node_name: n for n in nodes}
        assert by_name["Sort"].parent_id is None
        assert by_name["Exchange"].parent_id == 1
        assert by_name["Scan parquet"].parent_id == 2

        # Metric values are joined to the schema by accumulatorId.
        assert by_name["Exchange"].metrics["shuffle write size"] == "128.0 MiB"

    def test_reused_exchange_keeps_multi_parent(self) -> None:
        nodes = parse_physical_plan(_load("reused_exchange.json"))
        exchange = next(n for n in nodes if n.node_name == "Exchange")
        # Exchange is consumed by both Join (1) and ReusedExchange (3).
        assert sorted(exchange.parent_ids) == [1, 3]


class TestChildrenShape:
    def test_children_field_synthesises_edges(self) -> None:
        nodes = parse_physical_plan(_load("children_shape.json"))
        assert len(nodes) == 3
        # Sort is still the root; Scan still the leaf.
        by_name = {n.node_name: n for n in nodes}
        assert by_name["Sort"].parent_id is None
        assert by_name["Scan parquet"].parent_id == 2


class TestUnknownMetricValues:
    def test_missing_accumulator_id_is_silently_dropped(self) -> None:
        body = json.dumps(
            {
                "nodes": [
                    {
                        "nodeId": 1,
                        "nodeName": "Filter",
                        "metrics": [{"name": "rows", "accumulatorId": 999}],
                    }
                ],
                "metricValues": {},
            }
        )
        nodes = parse_physical_plan(body)
        assert len(nodes) == 1
        assert nodes[0].metrics == {}


@pytest.mark.parametrize("fixture", ["sort_exchange.json", "children_shape.json"])
def test_repr_includes_node_id_and_name(fixture: str) -> None:
    nodes = parse_physical_plan(_load(fixture))
    assert all("PlanNode(id=" in repr(n) for n in nodes)
