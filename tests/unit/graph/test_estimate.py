"""Unit tests for graph.estimate.estimate_cost."""

from __future__ import annotations

import pytest

from burnt.core.enums import EdgeType, NodeKind, ScalingType
from burnt.graph.estimate import (
    PHOTON_SPEEDUP,
    estimate_cost,
)
from burnt.graph.model import CostEdge, CostGraph, CostNode


def _node(
    node_id: str,
    *,
    line: int | None = None,
    scaling: ScalingType = ScalingType.LINEAR,
    shuffle: bool = False,
    photon: bool = False,
    input_bytes: int | None = None,
) -> CostNode:
    return CostNode(
        id=node_id,
        kind=NodeKind.READ,
        scaling_type=scaling,
        photon_eligible=photon,
        shuffle_required=shuffle,
        line_number=line,
        estimated_input_bytes=input_bytes,
    )


class _FakeSession:
    def __init__(
        self,
        stages: list[dict] | None = None,
        plan_bundles: list[dict] | None = None,
    ) -> None:
        self.stages = stages or []
        self.plan_bundles = plan_bundles or []


class TestCorrelation:
    def test_two_stages_three_node_graph(self) -> None:
        g = CostGraph()
        g.add_node(_node("n1", line=10))
        g.add_node(_node("n2", line=42, shuffle=True))
        g.add_node(_node("n3", line=99))

        session = _FakeSession(
            stages=[
                {
                    "stageId": 1,
                    "name": "scan at nb.py:10",
                    "executorRunTime": 5000,
                },
                {
                    "stageId": 2,
                    "name": "crossJoin at nb.py:42",
                    "executorRunTime": 84300,
                },
            ]
        )
        result = estimate_cost(g, session)

        # n1 and n2 should pick up observed seconds.
        assert result.breakdown["n1"] == pytest.approx(5.0)
        assert result.breakdown["n2"] == pytest.approx(84.3)
        # n3 is unmatched — pure scaling fallback.
        assert result.breakdown["n3"] > 0
        # coverage = 2/3
        assert result.coverage_ratio == pytest.approx(2 / 3)
        assert result.confidence == "high"

    def test_tie_break_picks_smallest_stage_id(self) -> None:
        g = CostGraph()
        g.add_node(_node("n1", line=10))
        session = _FakeSession(
            stages=[
                {"stageId": 99, "name": "x at f.py:8", "executorRunTime": 8000},
                {"stageId": 7, "name": "y at f.py:12", "executorRunTime": 12000},
            ]
        )
        result = estimate_cost(g, session)
        # Both are 2 lines off — tie. Smaller stageId (7) wins.
        assert result.breakdown["n1"] == pytest.approx(12.0)

    def test_line_window_rejects_far_stages(self) -> None:
        g = CostGraph()
        g.add_node(_node("n1", line=10))
        session = _FakeSession(
            stages=[
                {"stageId": 1, "name": "z at f.py:99", "executorRunTime": 100000}
            ]
        )
        result = estimate_cost(g, session)
        # Stage too far away to match — node falls back to scaling.
        assert result.coverage_ratio == 0.0
        assert result.breakdown["n1"] != pytest.approx(100.0)

    def test_unanchored_line_numbers_do_not_match(self) -> None:
        g = CostGraph()
        g.add_node(_node("n1", line=42))
        # Stage description has `:42` in a port number, not a callsite.
        session = _FakeSession(
            stages=[
                {
                    "stageId": 1,
                    "name": "host:42 collect",
                    "executorRunTime": 99999,
                }
            ]
        )
        result = estimate_cost(g, session)
        assert result.coverage_ratio == 0.0


class TestFallback:
    def test_empty_session_uses_scaling(self) -> None:
        g = CostGraph()
        g.add_node(_node("n1", scaling=ScalingType.LINEAR, input_bytes=1_000_000_000))
        g.add_node(
            _node(
                "n2",
                scaling=ScalingType.QUADRATIC,
                input_bytes=1_000_000_000,
            )
        )

        result = estimate_cost(g, session=None)
        assert result.coverage_ratio == 0.0
        assert result.confidence == "low"
        # linear: 1GB * 30 = 30s
        assert result.breakdown["n1"] == pytest.approx(30.0, rel=0.01)
        # quadratic: (1e9 * 1e9) / 1e18 * 300 = 300s
        assert result.breakdown["n2"] == pytest.approx(300.0, rel=0.01)

    def test_empty_graph_returns_empty(self) -> None:
        result = estimate_cost(CostGraph(), session=None)
        assert result.breakdown == {}
        assert result.coverage_ratio == 0.0


class TestPhoton:
    def test_photon_speedup_only_when_plan_confirms(self) -> None:
        g = CostGraph()
        g.add_node(
            _node("n1", scaling=ScalingType.LINEAR, photon=True, input_bytes=1_000_000_000)
        )
        # No plan bundle — no Photon confirmation → full estimate.
        no_plan = estimate_cost(g, _FakeSession())
        # With Photon node in plan → halved.
        with_plan = estimate_cost(
            g,
            _FakeSession(
                plan_bundles=[
                    {
                        "sqlExecId": 1,
                        "planNodes": [
                            {"nodeId": 1, "nodeName": "PhotonHashAggregate"}
                        ],
                    }
                ]
            ),
        )
        assert with_plan.breakdown["n1"] == pytest.approx(
            no_plan.breakdown["n1"] * PHOTON_SPEEDUP, rel=0.01
        )


class TestShuffleCrossLink:
    def test_shuffle_bytes_attached_for_matched_shuffle_nodes(self) -> None:
        g = CostGraph()
        g.add_node(_node("n1", line=42, shuffle=True))
        session = _FakeSession(
            stages=[
                {
                    "stageId": 1,
                    "name": "shuffle at nb.py:42",
                    "executorRunTime": 1000,
                }
            ],
            plan_bundles=[
                {
                    "sqlExecId": 1,
                    "planNodes": [
                        {
                            "nodeId": 1,
                            "nodeName": "Exchange",
                            "metrics": {"shuffle write size": "128.0 MiB"},
                        }
                    ],
                }
            ],
        )
        result = estimate_cost(g, session)
        assert result.shuffle_bytes["n1"] == 1024 * 1024 * 128


class TestDagAware:
    def test_child_contribution_subtracted_from_parent(self) -> None:
        g = CostGraph()
        g.add_node(_node("parent", line=10))
        g.add_node(_node("child", line=12))
        g.add_edge(CostEdge(source="child", target="parent", edge_type=EdgeType.DATAFLOW))

        session = _FakeSession(
            stages=[
                {"stageId": 1, "name": "p at f.py:10", "executorRunTime": 10_000},
                {"stageId": 2, "name": "c at f.py:12", "executorRunTime": 4_000},
            ]
        )
        result = estimate_cost(g, session)
        # parent originally 10s, child 4s; DAG subtraction: parent = 10 - 4 = 6s.
        assert result.breakdown["parent"] == pytest.approx(6.0)
        assert result.breakdown["child"] == pytest.approx(4.0)
