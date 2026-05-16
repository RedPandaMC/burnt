"""Unit tests for graph.enrich.enrich_graph."""

from __future__ import annotations

from burnt.core.enums import NodeKind, ScalingType
from burnt.graph.enrich import enrich_graph
from burnt.graph.model import CostGraph, CostNode


class _FakeSession:
    def __init__(self, stages: list[dict] | None = None) -> None:
        self.stages = stages or []


def _node(node_id: str, line: int) -> CostNode:
    return CostNode(
        id=node_id,
        kind=NodeKind.READ,
        scaling_type=ScalingType.LINEAR,
        line_number=line,
    )


def test_observed_input_bytes_back_filled() -> None:
    g = CostGraph()
    g.add_node(_node("n1", 42))
    g.add_node(_node("n2", 100))

    enriched = enrich_graph(
        g,
        session=_FakeSession(
            stages=[
                {
                    "stageId": 1,
                    "name": "scan at nb.py:42",
                    "inputBytes": 4_509_715_456,
                }
            ]
        ),
    )

    by_id = {n.id: n for n in enriched.nodes}
    assert by_id["n1"].estimated_input_bytes == 4_509_715_456
    # n2 had no matching stage — original (None) preserved.
    assert by_id["n2"].estimated_input_bytes is None


def test_no_session_is_pass_through() -> None:
    g = CostGraph()
    g.add_node(_node("n1", 42))
    enriched = enrich_graph(g, session=None)
    # Same nodes, same identity-equal values.
    assert enriched.nodes[0].estimated_input_bytes is None


def test_unmatched_stage_window_is_ignored() -> None:
    g = CostGraph()
    g.add_node(_node("n1", 10))
    enriched = enrich_graph(
        g,
        session=_FakeSession(
            stages=[
                {
                    "stageId": 1,
                    "name": "scan at nb.py:99",
                    "inputBytes": 1_000_000,
                }
            ]
        ),
    )
    assert enriched.nodes[0].estimated_input_bytes is None
