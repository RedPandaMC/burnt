"""Enrich graph with Delta table metadata."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .model import CostGraph


def enrich_graph(graph: CostGraph, *, warehouse_id: str | None = None) -> CostGraph:
    """Enrich cost graph with Delta table metadata.

    Args:
        graph: The cost graph to enrich.
        warehouse_id: Optional SQL warehouse for queries.

    Returns:
        Enriched cost graph with estimated_input_bytes filled in.
    """
    raise NotImplementedError(
        "Graph enrichment requires burnt-engine. "
        "Install with: pip install burnt[engine]"
    )


def enrich_dlt(pipeline_id: str, *, warehouse_id: str | None = None) -> dict[str, Any]:
    """Enrich DLT pipeline with metadata from Pipelines API.

    Args:
        pipeline_id: The DLT pipeline ID.
        warehouse_id: Optional SQL warehouse for queries.

    Returns:
        Pipeline metadata including table sizes and event log.
    """
    raise NotImplementedError(
        "DLT enrichment requires burnt-engine. Install with: pip install burnt[engine]"
    )
