"""Graph data models."""

from __future__ import annotations

from pydantic import ConfigDict, Field
from pydantic.dataclasses import dataclass

from burnt.core.enums import Confidence, EdgeType, GraphMode, NodeKind, ScalingType

_frozen_slots = ConfigDict(frozen=True, slots=True)
_mutable_slots = ConfigDict(slots=True)


@dataclass(config=_frozen_slots)
class PyTableRef:
    """Pure-Python mirror of the Rust `PyTableRef` PyO3 type.

    Used by duck-typed unit tests that construct fake graphs without going
    through the Rust builder. Field-for-field equivalent to
    `burnt._engine.PyTableRef`.
    """

    raw: str
    table: str
    catalog: str | None = None
    schema: str | None = None
    is_temp_view: bool = False
    is_path_read: bool = False
    path: str | None = None
    fqn: str = ""

    def __post_init__(self) -> None:
        # Materialise fqn from components when the caller did not supply one,
        # matching the Rust adapter's behaviour.
        if not self.fqn:
            if self.is_path_read:
                computed = f"path:{self.path or self.raw}"
            elif self.catalog and self.schema:
                computed = f"{self.catalog}.{self.schema}.{self.table}"
            elif self.schema:
                computed = f"{self.schema}.{self.table}"
            elif self.catalog:
                computed = f"{self.catalog}.{self.table}"
            else:
                computed = self.table
            object.__setattr__(self, "fqn", computed)


@dataclass(config=_frozen_slots)
class PyNode:
    """A single operation in a cost graph."""

    id: str
    kind: NodeKind
    scaling_type: ScalingType
    photon_eligible: bool = False
    shuffle_required: bool = False
    driver_bound: bool = False
    tables_referenced: list[PyTableRef] = Field(default_factory=list)
    estimated_input_bytes: int | None = None
    estimated_cost_usd: float | None = None
    line_number: int | None = None
    source_code: str | None = None


@dataclass(config=_frozen_slots)
class PyEdge:
    """An edge between cost nodes."""

    source: str
    target: str
    edge_type: EdgeType = EdgeType.DATAFLOW


@dataclass(config=_mutable_slots)
class PyGraph:
    """Graph of cost operations for Python/SQL workloads."""

    nodes: list[PyNode] = Field(default_factory=list)
    edges: list[PyEdge] = Field(default_factory=list)
    mode: GraphMode = GraphMode.PYTHON
    confidence: Confidence = Confidence.LOW

    def add_node(self, node: PyNode) -> None:
        """Add a node to the graph."""
        self.nodes.append(node)

    def add_edge(self, edge: PyEdge) -> None:
        """Add an edge to the graph."""
        self.edges.append(edge)

    def get_node(self, node_id: str) -> PyNode | None:
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
    inner_nodes: list[PyNode] = Field(default_factory=list)
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
