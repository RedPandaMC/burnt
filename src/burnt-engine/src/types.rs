use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use strum::Display;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlFragment {
    pub text: String,
    pub provenance: Provenance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provenance {
    pub source_path: Option<PathBuf>,
    pub start_line: u32,
    pub end_line: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SdpSignal {
    Import,
    Decorator(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonParseResult {
    pub sql_fragments: Vec<SqlFragment>,
    pub sdp_signals: Vec<SdpSignal>,
    pub findings: Vec<Finding>,
}

#[derive(Debug, Clone, Display, Serialize, Deserialize, PartialEq, Eq)]
#[strum(serialize_all = "snake_case")]
pub enum OperationKind {
    Read,
    Transform,
    Shuffle,
    Action,
    Write,
    UdfCall,
    Maintenance,
    Unknown,
}

#[derive(Debug, Clone, Display, Serialize, Deserialize, PartialEq, Eq)]
#[strum(serialize_all = "snake_case")]
pub enum ScalingBehavior {
    Linear,
    LinearWithCliff,
    Quadratic,
    StepFailure,
    Maintenance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostNode {
    pub id: String,
    pub kind: OperationKind,
    pub scaling_type: ScalingBehavior,
    pub photon_eligible: bool,
    pub shuffle_required: bool,
    pub driver_bound: bool,
    pub tables_referenced: Vec<String>,
    pub estimated_input_bytes: Option<u64>,
    pub estimated_cost_usd: Option<f64>,
    pub line_number: Option<u32>,
    pub source_code: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostEdge {
    pub source: String,
    pub target: String,
    pub edge_type: String,
}

#[derive(Debug, Clone, Copy, Display, Serialize, Deserialize, PartialEq, Eq)]
#[strum(serialize_all = "snake_case")]
pub enum SdpTableKind {
    StreamingTable,
    MaterializedView,
    TemporaryView,
}

#[derive(Debug, Clone, Copy, Display, Serialize, Deserialize, PartialEq, Eq)]
#[strum(serialize_all = "snake_case")]
pub enum SdpSourceType {
    CloudFiles,
    Kafka,
    SdpRead,
    DpRead,
    LiveRef,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineTable {
    pub id: String,
    pub name: String,
    pub kind: SdpTableKind,
    pub source_type: SdpSourceType,
    pub inner_nodes: Vec<CostNode>,
    pub expectations: Vec<String>,
    pub is_incremental: bool,
}

// Core types for Task 01
#[pyclass(eq, eq_int)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CellKind {
    Python,
    Sql,
    RunRef,
}

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cell {
    #[pyo3(get)]
    pub kind: CellKind,
    #[pyo3(get)]
    pub source: String,
    #[pyo3(get)]
    pub byte_offset: u32,
    #[pyo3(get)]
    pub line_offset: u32,
    #[pyo3(get)]
    pub origin_path: Option<PathBuf>,
}

#[pyclass(eq, eq_int)]
#[derive(Debug, Clone, Display, Serialize, Deserialize, PartialEq, Eq)]
#[strum(serialize_all = "snake_case")]
pub enum AnalysisMode {
    Python,
    Sql,
    Sdp,
}

#[pyclass(eq, eq_int)]
#[derive(Debug, Clone, Display, Serialize, Deserialize, PartialEq, Eq)]
#[strum(serialize_all = "snake_case")]
pub enum Severity {
    Error,
    Warning,
    Info,
}

#[pymethods]
impl Severity {
    fn __str__(&self) -> String {
        self.to_string()
    }
}

#[pyclass(eq, eq_int)]
#[derive(Debug, Clone, Display, Serialize, Deserialize, PartialEq, Eq)]
#[strum(serialize_all = "snake_case")]
pub enum Confidence {
    Low,
    Medium,
    High,
    None,
}

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Finding {
    #[pyo3(get)]
    pub rule_id: String,
    #[pyo3(get)]
    pub code: String,
    #[pyo3(get)]
    pub severity: Severity,
    #[pyo3(get)]
    pub message: String,
    #[pyo3(get)]
    pub suggestion: Option<String>,
    #[pyo3(get)]
    pub line_number: Option<u32>,
    #[pyo3(get)]
    pub column: Option<u32>,
    #[pyo3(get)]
    pub confidence: Confidence,
}

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleEntry {
    #[pyo3(get)]
    pub id: String,
    #[pyo3(get)]
    pub code: String,
    #[pyo3(get)]
    pub severity: Severity,
    #[pyo3(get)]
    pub language: String,
    #[pyo3(get)]
    pub description: String,
    #[pyo3(get)]
    pub suggestion: String,
    #[pyo3(get)]
    pub category: String,
    #[pyo3(get)]
    pub tags: Vec<String>,
}


// Types for enhanced rule system with tree-sitter queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPattern {
    pub match_pattern: String,
    pub is_negative: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledRule {
    pub id: String,
    pub code: String,
    pub severity: Severity,
    pub language: String,
    pub description: String,
    pub suggestion: String,
    pub category: String,
    pub patterns: Vec<QueryPattern>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub has_context: bool,
    #[serde(default)]
    pub has_dataflow: bool,
}

impl AnalysisMode {
    pub fn as_lang_str(&self) -> &'static str {
        match self {
            AnalysisMode::Sdp => "sdp",
            AnalysisMode::Sql => "sql",
            AnalysisMode::Python => "python",
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct AnalysisResultPy {
    #[pyo3(get)]
    pub mode: String,
    #[pyo3(get)]
    pub graph: Option<PyGraph>,
    #[pyo3(get)]
    pub pipeline: Option<PyPipeline>,
    #[pyo3(get)]
    pub findings: Vec<Finding>,
    #[pyo3(get)]
    pub cells: Vec<Cell>,
    #[pyo3(get)]
    pub path: Option<String>,
}

#[pyclass]
#[derive(Clone)]
pub struct PyGraph {
    #[pyo3(get)]
    pub nodes: Vec<PyCostNode>,
    #[pyo3(get)]
    pub edges: Vec<PyCostEdge>,
}

impl PyGraph {
    pub fn from_cost_graph(g: crate::graph::CostGraph) -> Self {
        PyGraph {
            nodes: g.nodes.into_iter().map(|n| n.into()).collect(),
            edges: g.edges.into_iter().map(|e| e.into()).collect(),
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyPipeline {
    #[pyo3(get)]
    pub tables: Vec<PyPipelineTable>,
}

impl PyPipeline {
    pub fn from_pipeline(g: crate::graph::PipelineGraph) -> Self {
        PyPipeline {
            tables: g.tables.into_iter().map(|t| t.into()).collect(),
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyCostNode {
    #[pyo3(get)]
    pub id: String,
    #[pyo3(get)]
    pub kind: String,
    #[pyo3(get)]
    pub scaling_type: String,
    #[pyo3(get)]
    pub photon_eligible: bool,
    #[pyo3(get)]
    pub shuffle_required: bool,
    #[pyo3(get)]
    pub driver_bound: bool,
    #[pyo3(get)]
    pub tables_referenced: Vec<String>,
    #[pyo3(get)]
    pub estimated_input_bytes: Option<u64>,
    #[pyo3(get)]
    pub estimated_cost_usd: Option<f64>,
    #[pyo3(get)]
    pub line_number: Option<u32>,
    #[pyo3(get)]
    pub source_code: Option<String>,
}

impl From<CostNode> for PyCostNode {
    fn from(n: CostNode) -> Self {
        PyCostNode {
            id: n.id,
            kind: n.kind.to_string(),
            scaling_type: n.scaling_type.to_string(),
            photon_eligible: n.photon_eligible,
            shuffle_required: n.shuffle_required,
            driver_bound: n.driver_bound,
            tables_referenced: n.tables_referenced,
            estimated_input_bytes: n.estimated_input_bytes,
            estimated_cost_usd: n.estimated_cost_usd,
            line_number: n.line_number,
            source_code: n.source_code,
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyCostEdge {
    #[pyo3(get)]
    pub source: String,
    #[pyo3(get)]
    pub target: String,
    #[pyo3(get)]
    pub edge_type: String,
}

impl From<CostEdge> for PyCostEdge {
    fn from(e: CostEdge) -> Self {
        PyCostEdge {
            source: e.source,
            target: e.target,
            edge_type: e.edge_type,
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyPipelineTable {
    #[pyo3(get)]
    pub id: String,
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub kind: String,
    #[pyo3(get)]
    pub source_type: String,
    #[pyo3(get)]
    pub inner_nodes: Vec<PyCostNode>,
    #[pyo3(get)]
    pub expectations: Vec<String>,
    #[pyo3(get)]
    pub is_incremental: bool,
}

impl From<PipelineTable> for PyPipelineTable {
    fn from(t: PipelineTable) -> Self {
        PyPipelineTable {
            id: t.id,
            name: t.name,
            kind: t.kind.to_string(),
            source_type: t.source_type.to_string(),
            inner_nodes: t.inner_nodes.into_iter().map(|n| n.into()).collect(),
            expectations: t.expectations,
            is_incremental: t.is_incremental,
        }
    }
}
