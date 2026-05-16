"""End-to-end test that the REST API session enriches the Rust-built
graph and feeds estimate_cost.

The unit tests in tests/unit/graph/ exercise enrich and estimate against
the pure-Python CostGraph. This file pins the *real* boundary: the Rust
``analyze_file`` returns a ``PyGraph`` of ``PyCostNode`` pyclasses, and
the Python runtime-merge layer must handle them without dataclass
mutation.

Regression guard for the bug where enrich_graph used
``dataclasses.replace`` on a non-dataclass and crashed the moment a
real session was passed to check().
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from burnt._check import run

if TYPE_CHECKING:
    from pathlib import Path


class _FakeSession:
    """Duck-typed SessionState — only `.stages` and `.plan_bundles` are
    read by the merge path."""

    def __init__(
        self,
        stages: list[dict] | None = None,
        plan_bundles: list[dict] | None = None,
    ) -> None:
        self.stages = stages or []
        self.plan_bundles = plan_bundles or []


def _write_source(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "nb.py"
    p.write_text(body, encoding="utf-8")
    return p


def test_real_pygraph_does_not_crash_with_session(tmp_path: Path) -> None:
    src = (
        'import pyspark\n'
        'df = spark.read.table("orders")\n'
        'result = df.collect()\n'
    )
    fpath = _write_source(tmp_path, src)
    session = _FakeSession(
        stages=[
            {
                "stageId": 1,
                "name": "collect at nb.py:3",
                "executorRunTime": 5000,
                "inputBytes": 1_000_000_000,
            }
        ]
    )

    # Before the fix, dataclasses.replace on PyCostNode raised TypeError.
    result = run(path=str(fpath), session=session)

    # The Rust graph reached estimate_cost and produced a breakdown.
    assert result.graph is not None
    assert hasattr(result.graph, "nodes")
    assert hasattr(result.graph, "edges")
    assert result.cost_estimate is not None
    assert result.compute_seconds is not None
    assert result.compute_seconds >= 0


def test_session_compute_seconds_reflects_observed_stage(tmp_path: Path) -> None:
    src = (
        'df = spark.read.table("orders")\n'
        'result = df.collect()\n'
    )
    fpath = _write_source(tmp_path, src)
    session = _FakeSession(
        stages=[
            {
                "stageId": 1,
                "name": "collect at nb.py:2",
                "executorRunTime": 12_345,
                "inputBytes": 1_000_000_000,
            }
        ]
    )
    result = run(path=str(fpath), session=session)

    # At least one node must have been matched to the stage (otherwise
    # the integration is silently failing back to the scaling-only path).
    assert result.cost_estimate is not None
    assert result.cost_estimate.coverage_ratio > 0
    # The matched contribution should be reflected in the total.
    assert result.compute_seconds >= 12.0


def test_no_session_path_unchanged(tmp_path: Path) -> None:
    src = 'df = spark.read.table("orders")\n'
    fpath = _write_source(tmp_path, src)
    result = run(path=str(fpath), session=None)
    # Without a session the runtime merge is skipped; nothing crashes.
    assert result.compute_seconds is None


def test_session_without_stages_attr_is_no_op(tmp_path: Path) -> None:
    """A session-shaped object missing `.stages` makes _merge_runtime
    early-return — compute_seconds stays None and nothing raises."""
    src = 'df = spark.read.table("orders")\n'
    fpath = _write_source(tmp_path, src)

    class Bare:
        pass

    result = run(path=str(fpath), session=Bare())
    assert result.compute_seconds is None
    assert result.cost_estimate is None


def test_session_with_empty_stages_uses_scaling_fallback(tmp_path: Path) -> None:
    """Empty stages on a session with `.stages` is *not* the same as no
    session at all — the runtime merge runs, finds nothing to correlate,
    and falls through to the scaling-function fallback for every node.
    Coverage is 0% but compute_seconds is populated."""
    src = 'df = spark.read.table("orders")\n'
    fpath = _write_source(tmp_path, src)

    result = run(path=str(fpath), session=_FakeSession(stages=[]))
    assert result.cost_estimate is not None
    assert result.cost_estimate.coverage_ratio == 0.0
    assert result.compute_seconds is not None
    assert result.compute_seconds > 0.0
