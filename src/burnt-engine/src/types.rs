use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
pub enum DltSignal {
    Import,
    Decorator(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonParseResult {
    pub tree: (),
    pub sql_fragments: Vec<SqlFragment>,
    pub dlt_signals: Vec<DltSignal>,
    pub findings: Vec<Finding>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

impl std::fmt::Display for OperationKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationKind::Read => write!(f, "read"),
            OperationKind::Transform => write!(f, "transform"),
            OperationKind::Shuffle => write!(f, "shuffle"),
            OperationKind::Action => write!(f, "action"),
            OperationKind::Write => write!(f, "write"),
            OperationKind::UdfCall => write!(f, "udf_call"),
            OperationKind::Maintenance => write!(f, "maintenance"),
            OperationKind::Unknown => write!(f, "unknown"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ScalingBehavior {
    Linear,
    LinearWithCliff,
    Quadratic,
    StepFailure,
    Maintenance,
}

impl std::fmt::Display for ScalingBehavior {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalingBehavior::Linear => write!(f, "linear"),
            ScalingBehavior::LinearWithCliff => write!(f, "linear_with_cliff"),
            ScalingBehavior::Quadratic => write!(f, "quadratic"),
            ScalingBehavior::StepFailure => write!(f, "step_failure"),
            ScalingBehavior::Maintenance => write!(f, "maintenance"),
        }
    }
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DltTableKind {
    StreamingTable,
    MaterializedView,
    TemporaryView,
}

impl std::fmt::Display for DltTableKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DltTableKind::StreamingTable => write!(f, "streaming_table"),
            DltTableKind::MaterializedView => write!(f, "materialized_view"),
            DltTableKind::TemporaryView => write!(f, "temporary_view"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DltSourceType {
    CloudFiles,
    Kafka,
    DltRead,
    DpRead,
    LiveRef,
    Unknown,
}

impl std::fmt::Display for DltSourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DltSourceType::CloudFiles => write!(f, "cloud_files"),
            DltSourceType::Kafka => write!(f, "kafka"),
            DltSourceType::DltRead => write!(f, "dlt_read"),
            DltSourceType::DpRead => write!(f, "dp_read"),
            DltSourceType::LiveRef => write!(f, "live_ref"),
            DltSourceType::Unknown => write!(f, "unknown"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineTable {
    pub id: String,
    pub name: String,
    pub kind: DltTableKind,
    pub source_type: DltSourceType,
    pub inner_nodes: Vec<CostNode>,
    pub expectations: Vec<String>,
    pub is_incremental: bool,
}

// Core types for Task 01
#[pyclass]
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

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AnalysisMode {
    Python,
    Sql,
    Dlt,
}

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
    Info,
}

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
    pub tier: u8,
}

// 128-bit bitset for rule matching
#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleTable {
    #[pyo3(get)]
    bits: [u64; 2],
}

#[pymethods]
impl RuleTable {
    #[new]
    pub fn new() -> Self {
        Self { bits: [0, 0] }
    }

    pub fn set(&mut self, index: usize) {
        if index < 128 {
            let word = index / 64;
            let bit = index % 64;
            self.bits[word] |= 1 << bit;
        }
    }

    pub fn clear(&mut self, index: usize) {
        if index < 128 {
            let word = index / 64;
            let bit = index % 64;
            self.bits[word] &= !(1 << bit);
        }
    }

    pub fn get(&self, index: usize) -> bool {
        if index < 128 {
            let word = index / 64;
            let bit = index % 64;
            (self.bits[word] >> bit) & 1 == 1
        } else {
            false
        }
    }
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
    pub tier: u8,
    pub patterns: Vec<QueryPattern>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionPhase {
    Syntax = 0,
    SimplePatterns = 1,
    ContextRules = 2,
    SemanticRules = 3,
    DltRules = 4,
    CrossCell = 5,
    Finalize = 6,
}

impl std::fmt::Display for ExecutionPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionPhase::Syntax => write!(f, "syntax"),
            ExecutionPhase::SimplePatterns => write!(f, "simple_patterns"),
            ExecutionPhase::ContextRules => write!(f, "context_rules"),
            ExecutionPhase::SemanticRules => write!(f, "semantic_rules"),
            ExecutionPhase::DltRules => write!(f, "dlt_rules"),
            ExecutionPhase::CrossCell => write!(f, "cross_cell"),
            ExecutionPhase::Finalize => write!(f, "finalize"),
        }
    }
}
