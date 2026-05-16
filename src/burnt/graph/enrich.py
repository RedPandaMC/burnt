"""Enrich graph nodes with observed runtime metadata.

When a session has collected stages, this layer correlates stages to
graph nodes by line number (same ±5-line window the estimator uses)
and returns the observed ``inputBytes`` keyed by node id. The estimator
then prefers observed bytes over the static Rust-side estimate when
computing scaling-function fallbacks for unmatched nodes.

Returning a sibling map (rather than mutating nodes) avoids two issues:

* The Rust ``PyNode`` adapter is a ``#[pyclass]``, not a Python
  dataclass — ``dataclasses.replace`` would raise ``TypeError`` on it.
* The Python ``PyNode`` is frozen-slotted, so in-place mutation is
  also off the table.

The map shape mirrors ``PyEstimate.shuffle_bytes``: ``dict[node_id, int]``.
"""

from __future__ import annotations

import re
from typing import Any

_LINE_RE = re.compile(r"(?:\.py|\.sql|<stdin>):(\d+)")
_LINE_WINDOW = 5


def enrich_graph(
    graph: Any,
    *,
    session: Any = None,
    warehouse_id: str | None = None,
) -> dict[str, int]:
    """Return observed ``inputBytes`` per graph node id.

    Args:
        graph: Static cost graph (either the Rust ``PyGraph`` or the
            pure-Python ``PyGraph``). Only ``.nodes`` is accessed.
        session: Optional ``SessionState`` with ``.stages``.
        warehouse_id: Reserved for a future Delta/UC metadata path. Unused.

    Returns:
        A dict ``{node_id: observed_input_bytes}`` containing only
        nodes that had a matching stage. Empty when no session is
        present or no stage correlates.
    """
    if session is None or graph is None:
        return {}

    nodes = list(getattr(graph, "nodes", []) or [])
    if not nodes:
        return {}

    stages = list(getattr(session, "stages", []) or [])
    if not stages:
        return {}

    observed: dict[str, int] = {}
    for node in nodes:
        bytes_in = _observed_input_bytes(node, stages)
        if bytes_in is not None:
            observed[node.id] = bytes_in
    return observed


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
