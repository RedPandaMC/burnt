use pyo3::prelude::*;

use crate::types::{CostEdge, CostNode, PipelineTable};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CostGraph {
    pub nodes: Vec<CostNode>,
    pub edges: Vec<CostEdge>,
    pub mode: String,
    pub confidence: String,
}

impl CostGraph {
    pub fn from_python(_source: &str) -> Result<Self, PyErr> {
        Ok(CostGraph {
            nodes: vec![],
            edges: vec![],
            mode: "python".to_string(),
            confidence: "low".to_string(),
        })
    }
    
    pub fn from_sql(_source: &str) -> Result<Self, PyErr> {
        Ok(CostGraph {
            nodes: vec![],
            edges: vec![],
            mode: "sql".to_string(),
            confidence: "low".to_string(),
        })
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
            kind: n.kind,
            scaling_type: n.scaling_type,
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
pub struct CostGraphPy {
    #[pyo3(get)]
    pub nodes: Vec<PyCostNode>,
    #[pyo3(get)]
    pub edges: Vec<PyCostEdge>,
    #[pyo3(get)]
    pub mode: String,
    #[pyo3(get)]
    pub confidence: String,
}

impl From<CostGraph> for CostGraphPy {
    fn from(g: CostGraph) -> Self {
        CostGraphPy {
            nodes: g.nodes.into_iter().map(|n| n.into()).collect(),
            edges: g.edges.into_iter().map(|e| e.into()).collect(),
            mode: g.mode,
            confidence: g.confidence,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PipelineGraph {
    pub tables: Vec<PipelineTable>,
    pub mode: String,
    pub confidence: String,
}

impl PipelineGraph {
    pub fn from_dlt(_source: &str) -> Self {
        PipelineGraph {
            tables: vec![],
            mode: "dlt".to_string(),
            confidence: "low".to_string(),
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

#[pyclass]
#[derive(Clone)]
pub struct PipelineGraphPy {
    #[pyo3(get)]
    pub tables: Vec<PyPipelineTable>,
    #[pyo3(get)]
    pub mode: String,
    #[pyo3(get)]
    pub confidence: String,
}

impl From<PipelineTable> for PyPipelineTable {
    fn from(t: PipelineTable) -> Self {
        PyPipelineTable {
            id: t.id,
            name: t.name,
            kind: t.kind,
            source_type: t.source_type,
            inner_nodes: t.inner_nodes.into_iter().map(|n| n.into()).collect(),
            expectations: t.expectations,
            is_incremental: t.is_incremental,
        }
    }
}

impl From<PipelineGraph> for PipelineGraphPy {
    fn from(g: PipelineGraph) -> Self {
        PipelineGraphPy {
            tables: g.tables.into_iter().map(|t| t.into()).collect(),
            mode: g.mode,
            confidence: g.confidence,
        }
    }
}