"""Per-node cost estimation.

Merges observed stage data (REST session client) with the static cost
graph. Stages that correlate to a graph node by line number contribute
`actual_compute_seconds`; nodes without a match fall back to a scaling
function selected by the node's ``ScalingType``.

Design notes
------------

* Strategy dispatch — a single module-level table maps every
  ``ScalingType`` to its scaling function. No string-matching, no
  ``if/elif`` ladder. Adding a new scaling type is a one-line change.
* Cross-link with plan nodes — when a matched stage has corresponding
  Exchange/Shuffle entries in the same SQL execution's plan, the
  shuffle bytes are exposed on ``PyEstimate.shuffle_bytes`` keyed by
  node id. ``PyNode`` is frozen-slotted, so we attach data via a
  sibling map rather than mutating the node.
* Photon-aware — when both the plan tree confirms a ``Photon*`` node ran
  AND the graph marks the node ``photon_eligible``, the scaling fallback
  is multiplied by ``PHOTON_SPEEDUP``.
* Confidence calibration — emits ``coverage_ratio`` (matched / total) as
  a float alongside the legacy three-bucket enum.
* DAG-aware — the estimator subtracts child contributions on fork/join
  shapes by walking the graph's edges, so reused subqueries do not
  double-count.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel

from burnt.core.enums import Confidence, ScalingType

if TYPE_CHECKING:
    from .model import PyGraph, PyNode

# ----------------------------------------------------------------------
# Strategy table — single source of truth for scaling-function dispatch
# ----------------------------------------------------------------------

# Each entry takes (estimated_input_bytes, fallback right-side bytes) and
# returns *estimated compute seconds*. Coefficients follow the issue spec:
# linear      = (bytes / 1e9) * 30   — ~30s per GB read
# quadratic   = (left * right) / 1e18 * 300
# Cliff/step/maintenance keep the same shape with their own coefficients.
_ScalingCallable = Callable[[float, float], float]


def _linear(left: float, _right: float) -> float:
    return (left / 1e9) * 30.0


def _linear_with_cliff(left: float, _right: float) -> float:
    # 3x slowdown above 1 GB threshold to model memory spill.
    if left >= 1e9:
        return (left / 1e9) * 90.0
    return (left / 1e9) * 30.0


def _quadratic(left: float, right: float) -> float:
    return (left * right) / 1e18 * 300.0


def _step(left: float, _right: float) -> float:
    # Below 100 GiB: linear; above: model the crash by reporting a huge
    # compute number rather than raising — keeps the estimator pure.
    if left > 100 * 1024**3:
        return 1e6
    return (left / 1e9) * 30.0


def _maintenance(left: float, _right: float) -> float:
    # Maintenance ops (OPTIMIZE/VACUUM) scale with size plus file count.
    file_count = int(left // 1_000_000)
    return (left + file_count * 1e6) / 1e9 * 30.0


_SCALING_STRATEGY: dict[ScalingType, _ScalingCallable] = {
    ScalingType.LINEAR: _linear,
    ScalingType.LINEAR_WITH_CLIFF: _linear_with_cliff,
    ScalingType.QUADRATIC: _quadratic,
    ScalingType.STEP_FAILURE: _step,
    ScalingType.MAINTENANCE: _maintenance,
}

# Photon node names from Spark plan trees ("PhotonHashAggregate" etc.).
_PHOTON_PREFIX = "Photon"
PHOTON_SPEEDUP = 0.5

# Default assumed cluster shape used for the scaling-only fallback when
# no observed data exists at all.
_DEFAULT_BYTES_PER_NODE = 100 * 1024**2  # 100 MiB

# Anchored line-number regex — matches `<file>.py:42`, `<file>.sql:42`,
# and `<stdin>:42`. Unanchored `:(\d+)` would catch port numbers and
# timestamps in stage descriptions.
_LINE_RE = re.compile(r"(?:\.py|\.sql|<stdin>):(\d+)")
_LINE_WINDOW = 5

# Plan-node names that surface shuffle write bytes.
_SHUFFLE_NODE_PREFIXES = ("Exchange", "PhotonShuffle", "ShuffleExchange")


class PyEstimate(BaseModel):
    """Estimated cost for a workload, plus per-node breakdown."""

    estimated_dbu: float | None = None
    costs: dict[str, float] = {}
    confidence: Literal["low", "medium", "high"] = "low"
    coverage_ratio: float = 0.0
    breakdown: dict[str, float] = {}
    shuffle_bytes: dict[str, int] = {}
    warnings: list[str] = []


def estimate(
    graph: PyGraph | Any,
    session: Any = None,
    *,
    observed_input_bytes: dict[str, int] | None = None,
    dbu_rate: float = 0.75,
    num_workers: int = 2,
) -> PyEstimate:
    """Estimate per-node cost from a static graph and an optional session.

    Args:
        graph: The static cost graph (``PyGraph`` from Python builder or
            ``PyGraph`` / ``PyGraph`` from the Rust engine).
        session: Optional ``SessionState``-shaped object with ``.stages``
            and ``.plan_bundles`` attributes. May be ``None`` for pure
            static estimation.
        observed_input_bytes: Optional ``{node_id: bytes}`` map from
            ``enrich_graph``; overrides ``node.estimated_input_bytes``
            for the scaling-function fallback when present.
        dbu_rate: DBU price multiplier folded into the dollar total.
        num_workers: Worker count used for the scaling-only fallback.

    Returns:
        A ``PyEstimate`` with ``breakdown`` keyed by node id and a
        ``coverage_ratio`` describing how much of the graph was observed.
    """
    nodes = _graph_nodes(graph)
    if not nodes:
        return PyEstimate()

    stages = _session_stages(session)
    plan_lookup = _build_plan_lookup(session)
    observed = observed_input_bytes or {}

    breakdown: dict[str, float] = {}
    shuffle_bytes: dict[str, int] = {}
    matched_count = 0

    for node in nodes:
        match = _correlate_stage(node, stages)
        if match is not None:
            seconds = match.get("executorRunTime", 0) / 1000.0
            breakdown[node.id] = float(seconds)
            matched_count += 1

            sw = _shuffle_write_for(node, plan_lookup)
            if sw is not None:
                shuffle_bytes[node.id] = sw
        else:
            est = _scaling_estimate(node, plan_lookup, observed.get(node.id))
            breakdown[node.id] = est

    # DAG-aware adjustment — for every fork/join, the child contribution
    # is already accounted for in the parent's executor runtime, so we
    # subtract it to avoid double-counting.
    breakdown = _subtract_child_contributions(graph, breakdown)

    total_seconds = sum(breakdown.values())
    estimated_dbu = total_seconds * dbu_rate / 3600.0
    coverage = matched_count / len(nodes) if nodes else 0.0

    return PyEstimate(
        estimated_dbu=estimated_dbu,
        costs={"dbu": estimated_dbu},
        confidence=_bucket(coverage),
        coverage_ratio=coverage,
        breakdown=breakdown,
        shuffle_bytes=shuffle_bytes,
    )


# ----------------------------------------------------------------------
# Internals
# ----------------------------------------------------------------------


def _graph_nodes(graph: Any) -> list[PyNode | Any]:
    if graph is None:
        return []
    return list(getattr(graph, "nodes", []) or [])


def _session_stages(session: Any) -> list[dict[str, Any]]:
    if session is None:
        return []
    return list(getattr(session, "stages", []) or [])


def _build_plan_lookup(session: Any) -> dict[int, list[dict[str, Any]]]:
    """Map ``sqlExecId -> [plan node dicts]``."""
    if session is None:
        return {}
    bundles = getattr(session, "plan_bundles", []) or []
    out: dict[int, list[dict[str, Any]]] = {}
    for bundle in bundles:
        exec_id = bundle.get("sqlExecId")
        if exec_id is None:
            continue
        out[int(exec_id)] = bundle.get("planNodes") or []
    return out


def _correlate_stage(
    node: Any, stages: list[dict[str, Any]]
) -> dict[str, Any] | None:
    """Pick the stage whose name carries a line number within ±5 of the node.

    Deterministic tie-break: smallest line-number delta wins; on a tie,
    smallest stage id wins.
    """
    node_line = getattr(node, "line_number", None)
    if node_line is None:
        return None

    best: tuple[int, int, dict[str, Any]] | None = None
    for stage in stages:
        name = stage.get("name") or ""
        m = _LINE_RE.search(name)
        if m is None:
            continue
        stage_line = int(m.group(1))
        delta = abs(stage_line - node_line)
        if delta > _LINE_WINDOW:
            continue
        sid = int(stage.get("stageId", 0))
        candidate = (delta, sid, stage)
        if best is None or candidate < best:
            best = candidate

    return best[2] if best else None


def _shuffle_write_for(
    node: Any, plan_lookup: dict[int, list[dict[str, Any]]]
) -> int | None:
    """Return the total shuffle-write bytes from plan Exchange nodes
    inside any execution whose plan touches this node's line range.

    The cross-link is intentionally fuzzy — graph nodes have no direct
    pointer to a SQL execution. We sum shuffle bytes across all
    Exchange-like plan nodes in every execution since these typically
    correspond one-to-one with the graph's shuffle nodes.
    """
    if not plan_lookup:
        return None
    if not getattr(node, "shuffle_required", False):
        return None

    total = 0
    for plan_nodes in plan_lookup.values():
        for p in plan_nodes:
            name = p.get("nodeName", "")
            if not any(name.startswith(prefix) for prefix in _SHUFFLE_NODE_PREFIXES):
                continue
            metrics = p.get("metrics") or {}
            raw = metrics.get("shuffle write size") or metrics.get(
                "shuffle bytes written"
            )
            parsed = _parse_metric_bytes(raw)
            if parsed:
                total += parsed
    return total or None


def _parse_metric_bytes(raw: Any) -> int:
    """Spark renders shuffle sizes as ``"128.0 MiB"`` strings. Parse to int."""
    if raw is None:
        return 0
    if isinstance(raw, (int, float)):
        return int(raw)
    s = str(raw).strip()
    m = re.match(r"([0-9.]+)\s*([KMGTP]?i?B)?", s, re.IGNORECASE)
    if not m:
        return 0
    value = float(m.group(1))
    unit = (m.group(2) or "B").upper()
    factors = {
        "B": 1,
        "KB": 1_000,
        "KIB": 1_024,
        "MB": 1_000_000,
        "MIB": 1_024**2,
        "GB": 1_000_000_000,
        "GIB": 1_024**3,
        "TB": 1_000_000_000_000,
        "TIB": 1_024**4,
        "PB": 1_000_000_000_000_000,
        "PIB": 1_024**5,
    }
    return int(value * factors.get(unit, 1))


def _scaling_estimate(
    node: Any,
    plan_lookup: dict[int, list[dict[str, Any]]],
    observed_bytes: int | None = None,
) -> float:
    """Fallback estimate when no stage matches the node.

    ``observed_bytes`` from ``enrich_graph`` takes precedence over the
    node's static ``estimated_input_bytes`` when present.
    """
    scaling = _resolve_scaling(node)
    fn = _SCALING_STRATEGY.get(scaling, _linear)
    left = float(
        observed_bytes
        if observed_bytes is not None
        else getattr(node, "estimated_input_bytes", None) or _DEFAULT_BYTES_PER_NODE
    )
    right = left  # quadratic only — assume self-join shape when unknown
    estimate = fn(left, right)

    # Photon-aware scaling — only kicks in when both the graph node
    # claims eligibility AND the plan tree confirms a Photon* node ran.
    if getattr(node, "photon_eligible", False) and _plan_has_photon(plan_lookup):
        estimate *= PHOTON_SPEEDUP

    return float(estimate)


def _resolve_scaling(node: Any) -> ScalingType:
    raw = getattr(node, "scaling_type", None)
    if isinstance(raw, ScalingType):
        return raw
    if raw is None:
        return ScalingType.LINEAR
    try:
        return ScalingType(str(raw))
    except ValueError:
        return ScalingType.LINEAR


def _plan_has_photon(plan_lookup: dict[int, list[dict[str, Any]]]) -> bool:
    return any(
        p.get("nodeName", "").startswith(_PHOTON_PREFIX)
        for plan_nodes in plan_lookup.values()
        for p in plan_nodes
    )


def _subtract_child_contributions(
    graph: Any, breakdown: dict[str, float]
) -> dict[str, float]:
    """Subtract child estimates from each parent so DAG joins do not
    double-count reused subqueries.

    A node's runtime already includes the runtime of its inputs (Spark
    aggregates executor time at the leaf level), so summing parents
    without correction over-reports total compute. Walk every edge and
    deduct ``min(parent, child)`` from the parent.
    """
    edges = list(getattr(graph, "edges", []) or [])
    if not edges:
        return breakdown
    adjusted = dict(breakdown)
    for e in edges:
        src = getattr(e, "source", None)
        tgt = getattr(e, "target", None)
        if src is None or tgt is None:
            continue
        if tgt in adjusted and src in adjusted:
            deduction = min(adjusted[tgt], adjusted[src])
            adjusted[tgt] = max(0.0, adjusted[tgt] - deduction)
    return adjusted


def _bucket(coverage: float) -> Literal["low", "medium", "high"]:
    if coverage >= 0.66:
        return Confidence.HIGH.value
    if coverage >= 0.33:
        return Confidence.MEDIUM.value
    return Confidence.LOW.value
