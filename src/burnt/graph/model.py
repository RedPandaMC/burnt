"""Graph data models."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class CostNode(BaseModel):
    """A single operation in a cost graph."""

    id: str
    kind: Literal[
        "read", "transform", "shuffle", "action", "write", "udf_call", "maintenance"
    ]
    scaling_type: Literal[
        "linear", "linear_with_cliff", "quadratic", "step_failure", "maintenance"
    ]
    photon_eligible: bool = False
    shuffle_required: bool = False
    driver_bound: bool = False
    tables_referenced: list[str] = Field(default_factory=list)
    estimated_input_bytes: int | None = None
    estimated_cost_usd: float | None = None
    line_number: int | None = None
    source_code: str | None = None


class CostEdge(BaseModel):
    """An edge between cost nodes."""

    source: str
    target: str
    edge_type: Literal["dataflow", "control", "dependency"] = "dataflow"


class CostGraph(BaseModel):
    """Graph of cost operations for Python/SQL workloads."""

    nodes: list[CostNode] = Field(default_factory=list)
    edges: list[CostEdge] = Field(default_factory=list)
    mode: Literal["python", "sql", "dlt"] = "python"
    confidence: Literal["low", "medium", "high"] = "low"

    def add_node(self, node: CostNode) -> None:
        """Add a node to the graph."""
        self.nodes.append(node)

    def add_edge(self, edge: CostEdge) -> None:
        """Add an edge to the graph."""
        self.edges.append(edge)

    def get_node(self, node_id: str) -> CostNode | None:
        """Get a node by ID."""
        for node in self.nodes:
            if node.id == node_id:
                return node
        return None


class PipelineTable(BaseModel):
    """A table in a DLT pipeline."""

    id: str
    name: str
    kind: Literal["streaming", "materialized_view", "temporary_view"]
    source_type: Literal["cloud_files", "kafka", "dlt_read", "live_ref"] = "cloud_files"
    inner_nodes: list[CostNode] = Field(default_factory=list)
    expectations: list[str] = Field(default_factory=list)
    is_incremental: bool = True


class PipelineGraph(BaseModel):
    """Graph of DLT pipeline tables."""

    tables: list[PipelineTable] = Field(default_factory=list)
    mode: Literal["dlt"] = "dlt"
    confidence: Literal["low", "medium", "high"] = "low"

    def add_table(self, table: PipelineTable) -> None:
        """Add a table to the pipeline."""
        self.tables.append(table)
