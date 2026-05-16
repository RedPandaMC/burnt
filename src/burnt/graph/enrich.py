"""Enrich graph nodes with observed runtime metadata.

When a session has collected stages, this layer back-fills each
``CostNode.estimated_input_bytes`` with the observed ``inputBytes`` of
the stage that correlates to that node (same line-number correlation
the estimator uses). Falls back to a no-op when no session is present.

``CostNode`` is frozen-slotted, so the in-place enrichment returns a
new ``CostGraph`` rather than mutating the input.
"""

from __future__ import annotations

import re
from dataclasses import replace
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .model import CostGraph

_LINE_RE = re.compile(r"(?:\.py|\.sql|<stdin>):(\d+)")
_LINE_WINDOW = 5


def enrich_graph(
    graph: CostGraph,
    *,
    session: Any = None,
    warehouse_id: str | None = None,
) -> CostGraph:
    """Annotate every matched node with its observed ``inputBytes``.

    Args:
        graph: Static cost graph from the Rust engine.
        session: Optional ``SessionState`` with ``.stages``.
        warehouse_id: Reserved for a future Delta/UC metadata enrichment
            path. Unused today.

    Returns:
        A new ``CostGraph`` whose nodes carry observed input bytes where
        a stage matched, untouched copies otherwise.
    """
    if session is None or not getattr(graph, "nodes", None):
        return graph

    stages = list(getattr(session, "stages", []) or [])
    if not stages:
        return graph

    new_nodes = []
    for node in graph.nodes:
        observed = _observed_input_bytes(node, stages)
        if observed is None:
            new_nodes.append(node)
        else:
            new_nodes.append(replace(node, estimated_input_bytes=observed))

    return replace(graph, nodes=new_nodes)


def enrich_dlt(
    pipeline_id: str,
    *,
    warehouse_id: str | None = None,
) -> dict[str, Any]:
    """Stub — DLT pipeline enrichment is tracked separately."""
    return {"pipeline_id": pipeline_id, "warehouse_id": warehouse_id, "tables": []}


def _observed_input_bytes(node: Any, stages: list[dict[str, Any]]) -> int | None:
    node_line = getattr(node, "line_number", None)
    if node_line is None:
        return None
    best: tuple[int, int, int] | None = None
    for stage in stages:
        name = stage.get("name") or ""
        m = _LINE_RE.search(name)
        if m is None:
            continue
        delta = abs(int(m.group(1)) - node_line)
        if delta > _LINE_WINDOW:
            continue
        sid = int(stage.get("stageId", 0))
        bytes_in = int(stage.get("inputBytes", 0))
        candidate = (delta, sid, bytes_in)
        if best is None or candidate < best:
            best = candidate
    return best[2] if best is not None else None
