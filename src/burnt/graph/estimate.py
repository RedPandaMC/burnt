"""Per-node cost estimation, consumed by ``burnt._check`` only.

After the resolved-graph refactor this module is `_check`-internal: it is
not exported from ``burnt.__init__`` and the only caller is
``_check._merge_runtime``. The signature accepts a ``resolved``
``PyResolvedGraph`` argument; runtime correlation has moved into the
Rust merge layer (``src/burnt-engine/src/resolved/merge.rs``) so the
line-number heuristic lives in exactly one place.

Per-node estimate priority:

1. Observed compute from any attached stage in the resolved graph
   (executor run time → seconds).
2. Sum of ``TableSpec.size_bytes`` for the node's ``tables_referenced``
   when a table-spec overlay is present.
3. Static ``estimated_input_bytes`` from the Rust side.
4. ``_DEFAULT_BYTES_PER_NODE`` fallback.

Whichever wins feeds the scaling-function dispatch, with the
plan-subtree contributing Photon-aware multipliers and the existing
DAG-aware double-count subtraction unchanged.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel

from burnt.core.enums import Confidence, ScalingType

if TYPE_CHECKING:
    from .model import PyGraph, PyNode


_ScalingCallable = Callable[[float, float], float]


def _linear(left: float, _right: float) -> float:
    return (left / 1e9) * 30.0


def _linear_with_cliff(left: float, _right: float) -> float:
    if left >= 1e9:
        return (left / 1e9) * 90.0
    return (left / 1e9) * 30.0


def _quadratic(left: float, right: float) -> float:
    return (left * right) / 1e18 * 300.0


def _step(left: float, _right: float) -> float:
    if left > 100 * 1024**3:
        return 1e6
    return (left / 1e9) * 30.0


def _maintenance(left: float, _right: float) -> float:
    file_count = int(left // 1_000_000)
    return (left + file_count * 1e6) / 1e9 * 30.0


_SCALING_STRATEGY: dict[ScalingType, _ScalingCallable] = {
    ScalingType.LINEAR: _linear,
    ScalingType.LINEAR_WITH_CLIFF: _linear_with_cliff,
    ScalingType.QUADRATIC: _quadratic,
    ScalingType.STEP_FAILURE: _step,
    ScalingType.MAINTENANCE: _maintenance,
}

_PHOTON_PREFIX = "Photon"
PHOTON_SPEEDUP = 0.5
_DEFAULT_BYTES_PER_NODE = 100 * 1024**2  # 100 MiB

# Plan-operator names that surface shuffle-write bytes via metrics.
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
    resolved: Any = None,
    dbu_rate: float = 0.75,
    num_workers: int = 2,
) -> PyEstimate:
    """Estimate per-node cost from a static graph and an optional resolved overlay.

    Args:
        graph: The static cost graph. The Rust ``PyGraph`` shape is the
            primary input; pure-Python ``PyGraph`` is supported for tests.
        session: Optional ``SessionState``-shaped object. Used only to
            populate ``compute_seconds`` on the result when no
            ``resolved`` overlay supplies stages — i.e. when called
            outside the ``_check`` orchestrator.
        resolved: Optional ``PyResolvedGraph`` produced by the
            ``_resolve_graph`` Rust entry point. When present, runtime
            correlation, plan-subtree extraction, and table-spec lookup
            all flow through it.
        dbu_rate: DBU price multiplier folded into the dollar total.
        num_workers: Worker count used for the scaling-only fallback
            (kept for API back-compat; not currently used in this
            simplified implementation).

    Returns:
        A ``PyEstimate`` with ``breakdown`` keyed by node id and a
        ``coverage_ratio`` describing how much of the graph was observed.
    """
    _ = (session, num_workers)  # silence unused-parameter checks while keeping API

    nodes = _graph_nodes(graph)
    if not nodes:
        return PyEstimate()

    breakdown: dict[str, float] = {}
    shuffle_bytes: dict[str, int] = {}
    covered = 0

    plan_has_photon = _plan_has_photon(resolved)

    for node in nodes:
        overlay = _overlay_for(resolved, node.id)
        if overlay is not None and overlay.stages:
            seconds = sum(
                (s.duration_ms or 0) / 1000.0 for s in overlay.stages
            )
            breakdown[node.id] = float(seconds)
            covered += 1
            sw = _shuffle_write_for(node, overlay)
            if sw is not None:
                shuffle_bytes[node.id] = sw
            continue

        if overlay is not None and overlay.plan_subtree is not None:
            covered += 1

        # Fallback: scaling function over the best input-bytes estimate.
        bytes_left = _resolve_input_bytes(node, resolved)
        if _node_covered_by_table_spec(node, resolved):
            covered += 1
        est = _scaling_estimate(node, bytes_left, plan_has_photon)
        breakdown[node.id] = est

    breakdown = _subtract_child_contributions(graph, breakdown)

    total_seconds = sum(breakdown.values())
    estimated_dbu = total_seconds * dbu_rate / 3600.0
    coverage = min(covered / len(nodes), 1.0) if nodes else 0.0

    return PyEstimate(
        estimated_dbu=estimated_dbu,
        costs={"dbu": estimated_dbu},
        confidence=_bucket(coverage),
        coverage_ratio=coverage,
        breakdown=breakdown,
        shuffle_bytes=shuffle_bytes,
    )


def _graph_nodes(graph: Any) -> list[PyNode | Any]:
    if graph is None:
        return []
    return list(getattr(graph, "nodes", []) or [])


def _overlay_for(resolved: Any, node_id: str) -> Any:
    if resolved is None:
        return None
    overlay_fn = getattr(resolved, "overlay", None)
    if overlay_fn is None:
        return None
    return overlay_fn(node_id)


def _resolve_input_bytes(node: Any, resolved: Any) -> float:
    """Priority chain: table-spec sum → static heuristic → default."""
    spec_total = _table_spec_total(node, resolved)
    if spec_total is not None:
        return float(spec_total)
    static = getattr(node, "estimated_input_bytes", None)
    if static is not None:
        return float(static)
    return float(_DEFAULT_BYTES_PER_NODE)


def _table_spec_total(node: Any, resolved: Any) -> int | None:
    if resolved is None:
        return None
    spec_lookup = getattr(resolved, "table_spec", None)
    if spec_lookup is None:
        return None
    total = 0
    matched_any = False
    for tref in getattr(node, "tables_referenced", []) or []:
        fqn = getattr(tref, "fqn", None)
        if fqn is None:
            continue
        spec = spec_lookup(fqn)
        if spec is None:
            continue
        if spec.size_bytes is None:
            continue
        total += int(spec.size_bytes)
        matched_any = True
    return total if matched_any else None


def _node_covered_by_table_spec(node: Any, resolved: Any) -> bool:
    if resolved is None:
        return False
    spec_lookup = getattr(resolved, "table_spec", None)
    if spec_lookup is None:
        return False
    for tref in getattr(node, "tables_referenced", []) or []:
        fqn = getattr(tref, "fqn", None)
        if fqn is None:
            continue
        if spec_lookup(fqn) is not None:
            return True
    return False


def _scaling_estimate(
    node: Any,
    bytes_left: float,
    plan_has_photon: bool,
) -> float:
    scaling = _resolve_scaling(node)
    fn = _SCALING_STRATEGY.get(scaling, _linear)
    estimate_val = fn(bytes_left, bytes_left)
    if getattr(node, "photon_eligible", False) and plan_has_photon:
        estimate_val *= PHOTON_SPEEDUP
    return float(estimate_val)


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


def _plan_has_photon(resolved: Any) -> bool:
    if resolved is None:
        return False
    for node_id in resolved.node_ids():
        overlay = resolved.overlay(node_id)
        if overlay is None or overlay.plan_subtree is None:
            continue
        for plan_node in overlay.plan_subtree.nodes:
            if plan_node.node_name.startswith(_PHOTON_PREFIX):
                return True
    return False


def _shuffle_write_for(node: Any, overlay: Any) -> int | None:
    if not getattr(node, "shuffle_required", False):
        return None
    subtree = overlay.plan_subtree
    if subtree is None:
        return None
    total = 0
    for p in subtree.nodes:
        name = p.node_name
        if not any(name.startswith(prefix) for prefix in _SHUFFLE_NODE_PREFIXES):
            continue
        metrics = p.metrics or {}
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


def _subtract_child_contributions(
    graph: Any, breakdown: dict[str, float]
) -> dict[str, float]:
    """Subtract child estimates from each parent so DAG joins do not
    double-count reused subqueries."""
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
