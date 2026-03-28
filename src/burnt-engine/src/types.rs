use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostNode {
    pub id: String,
    pub kind: String,
    pub scaling_type: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineTable {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub source_type: String,
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
