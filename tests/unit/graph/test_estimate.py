"""Unit tests for graph.estimate.estimate (resolved-graph era).

These tests exercise the estimator against a duck-typed fake resolved
graph — the same shape `_check._merge_runtime` constructs via
`burnt._engine._resolve_graph`. Tests of the resolution layer itself
live in `tests/unit/graph/test_resolved_graph.py` and the Rust-side
integration test in `src/burnt-engine/tests/resolved_graph.rs`.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from burnt.core.enums import EdgeType, NodeKind, ScalingType
from burnt.graph.estimate import PHOTON_SPEEDUP, estimate
from burnt.graph.model import PyEdge, PyGraph, PyNode


def _node(
    node_id: str,
    *,
    line: int | None = None,
    scaling: ScalingType = ScalingType.LINEAR,
    shuffle: bool = False,
    photon: bool = False,
    input_bytes: int | None = None,
) -> PyNode:
    return PyNode(
        id=node_id,
        kind=NodeKind.READ,
        scaling_type=scaling,
        photon_eligible=photon,
        shuffle_required=shuffle,
        line_number=line,
        estimated_input_bytes=input_bytes,
    )


# ---------------------------------------------------------------------------
# Fake resolved-graph shape — mirrors PyResolvedGraph's read surface.
# ---------------------------------------------------------------------------


@dataclass
class _FakeStage:
    stage_id: int
    input_bytes: int | None = None
    shuffle_read_bytes: int | None = None
    shuffle_write_bytes: int | None = None
    duration_ms: int | None = None
    source_line: int | None = None


@dataclass
class _FakePlanNode:
    node_id: int
    node_name: str
    parent_ids: list[int] = field(default_factory=list)
    metrics: dict = field(default_factory=dict)


@dataclass
class _FakePlanSubtree:
    sql_exec_id: int
    root: int
    nodes: list[_FakePlanNode]


@dataclass
class _FakeOverlay:
    stages: list[_FakeStage] = field(default_factory=list)
    plan_subtree: _FakePlanSubtree | None = None
    provenance: int = 0b001


class _FakeResolved:
    def __init__(
        self,
        overlays: dict[str, _FakeOverlay] | None = None,
        table_specs: dict[str, object] | None = None,
    ) -> None:
        self._overlays = overlays or {}
        self._specs = table_specs or {}

    def overlay(self, node_id: str) -> _FakeOverlay | None:
        return self._overlays.get(node_id)

    def node_ids(self) -> list[str]:
        return list(self._overlays.keys())

    def table_spec(self, fqn: str):
        return self._specs.get(fqn)


class TestCorrelation:
    def test_two_stages_three_node_graph(self) -> None:
        g = PyGraph()
        g.add_node(_node("n1", line=10))
        g.add_node(_node("n2", line=42, shuffle=True))
        g.add_node(_node("n3", line=99))

        resolved = _FakeResolved(
            overlays={
                "n1": _FakeOverlay(stages=[_FakeStage(1, duration_ms=5000)]),
                "n2": _FakeOverlay(stages=[_FakeStage(2, duration_ms=84300)]),
                "n3": _FakeOverlay(),
            },
        )
        result = estimate(g, session=None, resolved=resolved)

        assert result.breakdown["n1"] == pytest.approx(5.0)
        assert result.breakdown["n2"] == pytest.approx(84.3)
        assert result.breakdown["n3"] > 0
        assert result.coverage_ratio == pytest.approx(2 / 3)
        assert result.confidence == "high"

    def test_no_overlay_falls_through_to_scaling(self) -> None:
        g = PyGraph()
        g.add_node(_node("n1", line=10, input_bytes=1_000_000_000))
        result = estimate(g, session=None, resolved=None)
        assert result.coverage_ratio == 0.0
        # Linear: 1GB → 30 s.
        assert result.breakdown["n1"] == pytest.approx(30.0, rel=0.01)

    def test_empty_overlay_keeps_node_uncovered(self) -> None:
        g = PyGraph()
        g.add_node(_node("n1", line=10, input_bytes=1_000_000_000))
        resolved = _FakeResolved(overlays={"n1": _FakeOverlay()})
        result = estimate(g, session=None, resolved=resolved)
        assert result.coverage_ratio == 0.0
        # Linear: 1GB → 30 s, no observed override.
        assert result.breakdown["n1"] == pytest.approx(30.0, rel=0.01)


class TestFallback:
    def test_empty_session_and_resolved_uses_scaling(self) -> None:
        g = PyGraph()
        g.add_node(_node("n1", scaling=ScalingType.LINEAR, input_bytes=1_000_000_000))
        g.add_node(
            _node(
                "n2",
                scaling=ScalingType.QUADRATIC,
                input_bytes=1_000_000_000,
            )
        )

        result = estimate(g, session=None)
        assert result.coverage_ratio == 0.0
        assert result.confidence == "low"
        assert result.breakdown["n1"] == pytest.approx(30.0, rel=0.01)
        assert result.breakdown["n2"] == pytest.approx(300.0, rel=0.01)

    def test_empty_graph_returns_empty(self) -> None:
        result = estimate(PyGraph(), session=None)
        assert result.breakdown == {}
        assert result.coverage_ratio == 0.0


class TestPhoton:
    def test_photon_speedup_only_when_plan_confirms(self) -> None:
        g = PyGraph()
        g.add_node(
            _node("n1", scaling=ScalingType.LINEAR, photon=True, input_bytes=1_000_000_000)
        )
        baseline = estimate(g, session=None, resolved=None)

        photon_overlay = _FakeOverlay(
            plan_subtree=_FakePlanSubtree(
                sql_exec_id=1,
                root=1,
                nodes=[_FakePlanNode(node_id=1, node_name="PhotonHashAggregate")],
            )
        )
        with_plan = estimate(
            g,
            session=None,
            resolved=_FakeResolved(overlays={"n1": photon_overlay}),
        )

        # n1 has no observed stages → scaling estimate × PHOTON_SPEEDUP.
        assert with_plan.breakdown["n1"] == pytest.approx(
            baseline.breakdown["n1"] * PHOTON_SPEEDUP, rel=0.01
        )


class TestShuffleCrossLink:
    def test_shuffle_bytes_attached_for_matched_shuffle_nodes(self) -> None:
        g = PyGraph()
        g.add_node(_node("n1", line=42, shuffle=True))
        overlay = _FakeOverlay(
            stages=[_FakeStage(1, duration_ms=1000)],
            plan_subtree=_FakePlanSubtree(
                sql_exec_id=1,
                root=1,
                nodes=[
                    _FakePlanNode(
                        node_id=1,
                        node_name="Exchange",
                        metrics={"shuffle write size": "128.0 MiB"},
                    )
                ],
            ),
        )
        result = estimate(
            g, session=None, resolved=_FakeResolved(overlays={"n1": overlay})
        )
        assert result.shuffle_bytes["n1"] == 1024 * 1024 * 128


class TestDagAware:
    def test_child_contribution_subtracted_from_parent(self) -> None:
        g = PyGraph()
        g.add_node(_node("parent", line=10))
        g.add_node(_node("child", line=12))
        g.add_edge(PyEdge(source="child", target="parent", edge_type=EdgeType.DATAFLOW))

        resolved = _FakeResolved(
            overlays={
                "parent": _FakeOverlay(stages=[_FakeStage(1, duration_ms=10_000)]),
                "child": _FakeOverlay(stages=[_FakeStage(2, duration_ms=4_000)]),
            }
        )
        result = estimate(g, session=None, resolved=resolved)
        # parent 10s, child 4s; DAG subtraction: parent = 10 - 4 = 6s.
        assert result.breakdown["parent"] == pytest.approx(6.0)
        assert result.breakdown["child"] == pytest.approx(4.0)


class TestTableSpecOverlay:
    def test_table_spec_size_used_as_scaling_input(self) -> None:
        from burnt.graph.model import PyTableRef

        # Node with no observed stages but a TableRef whose TableSpec has
        # size_bytes — estimator should use that as the scaling input.
        g = PyGraph()
        g.add_node(
            PyNode(
                id="n1",
                kind=NodeKind.READ,
                scaling_type=ScalingType.LINEAR,
                line_number=10,
                tables_referenced=[
                    PyTableRef(raw="cat.sch.t", table="t", catalog="cat", schema="sch")
                ],
            )
        )

        @dataclass
        class _Spec:
            fqn: str
            size_bytes: int | None

        resolved = _FakeResolved(
            overlays={"n1": _FakeOverlay()},
            table_specs={"cat.sch.t": _Spec(fqn="cat.sch.t", size_bytes=2_000_000_000)},
        )
        result = estimate(g, session=None, resolved=resolved)
        # 2 GB × 30 s/GB = 60 s.
        assert result.breakdown["n1"] == pytest.approx(60.0, rel=0.01)
        # Coverage widens to include table-spec-only nodes.
        assert result.coverage_ratio == 1.0
