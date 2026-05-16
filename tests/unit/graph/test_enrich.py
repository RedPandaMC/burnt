"""Unit tests for graph.enrich.enrich_graph."""

from __future__ import annotations

from burnt.core.enums import NodeKind, ScalingType
from burnt.graph.enrich import enrich_graph
from burnt.graph.model import PyGraph, PyNode


class _FakeSession:
    def __init__(self, stages: list[dict] | None = None) -> None:
        self.stages = stages or []


def _node(node_id: str, line: int) -> PyNode:
    return PyNode(
        id=node_id,
        kind=NodeKind.READ,
        scaling_type=ScalingType.LINEAR,
        line_number=line,
    )


def test_observed_input_bytes_back_filled() -> None:
    g = PyGraph()
    g.add_node(_node("n1", 42))
    g.add_node(_node("n2", 100))

    observed = enrich_graph(
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

    assert observed == {"n1": 4_509_715_456}
    # n2 had no matching stage — absent from the map, not zero.
    assert "n2" not in observed


def test_no_session_is_empty_dict() -> None:
    g = PyGraph()
    g.add_node(_node("n1", 42))
    assert enrich_graph(g, session=None) == {}


def test_unmatched_stage_window_is_ignored() -> None:
    g = PyGraph()
    g.add_node(_node("n1", 10))
    observed = enrich_graph(
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
    assert observed == {}


def test_works_with_duck_typed_node_objects() -> None:
    """enrich_graph must not depend on dataclass replace — Rust
    PyNode instances are #[pyclass], not dataclasses."""

    class FakeNode:
        def __init__(self, node_id: str, line: int) -> None:
            self.id = node_id
            self.line_number = line

    class FakeGraph:
        def __init__(self, nodes: list[FakeNode]) -> None:
            self.nodes = nodes

    g = FakeGraph([FakeNode("rust_node_1", 7)])
    observed = enrich_graph(
        g,
        session=_FakeSession(
            stages=[
                {"stageId": 5, "name": "x at nb.py:7", "inputBytes": 999}
            ]
        ),
    )
    assert observed == {"rust_node_1": 999}
