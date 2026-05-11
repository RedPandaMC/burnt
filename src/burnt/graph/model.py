"""Graph data models."""

from __future__ import annotations

from pydantic import ConfigDict, Field
from pydantic.dataclasses import dataclass

from burnt.core.enums import Confidence, EdgeType, GraphMode, NodeKind, ScalingType

_frozen_slots = ConfigDict(frozen=True, slots=True)
_mutable_slots = ConfigDict(slots=True)


@dataclass(config=_frozen_slots)
class CostNode:
    """A single operation in a cost graph."""

    id: str
    kind: NodeKind
    scaling_type: ScalingType
    photon_eligible: bool = False
    shuffle_required: bool = False
    driver_bound: bool = False
    tables_referenced: list[str] = Field(default_factory=list)
    estimated_input_bytes: int | None = None
    estimated_cost_usd: float | None = None
    line_number: int | None = None
    source_code: str | None = None


@dataclass(config=_frozen_slots)
class CostEdge:
    """An edge between cost nodes."""

    source: str
    target: str
    edge_type: EdgeType = EdgeType.DATAFLOW


@dataclass(config=_mutable_slots)
class CostGraph:
    """Graph of cost operations for Python/SQL workloads."""

    nodes: list[CostNode] = Field(default_factory=list)
    edges: list[CostEdge] = Field(default_factory=list)
    mode: GraphMode = GraphMode.PYTHON
    confidence: Confidence = Confidence.LOW

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


@dataclass(config=_frozen_slots)
class PipelineTable:
    """A table in a DLT pipeline."""

    id: str
    name: str
    kind: str
    source_type: str = "cloud_files"
    inner_nodes: list[CostNode] = Field(default_factory=list)
    expectations: list[str] = Field(default_factory=list)
    is_incremental: bool = True


@dataclass(config=_mutable_slots)
class PipelineGraph:
    """Graph of DLT pipeline tables."""

    tables: list[PipelineTable] = Field(default_factory=list)
    mode: GraphMode = GraphMode.DLT
    confidence: Confidence = Confidence.LOW

    def add_table(self, table: PipelineTable) -> None:
        """Add a table to the pipeline."""
        self.tables.append(table)
