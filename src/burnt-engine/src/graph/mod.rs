use pyo3::prelude::*;

use crate::types::{CostEdge, CostNode, Finding, PipelineTable, PyCostEdge, PyCostNode, PyPipelineTable};

pub mod sdp;
pub mod python;
pub mod sql;

use sdp::SdpGraphBuilder;
use python::PythonGraphBuilder;
use sql::SqlGraphBuilder;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CostGraph {
    pub nodes: Vec<CostNode>,
    pub edges: Vec<CostEdge>,
    /// Semantic findings (e.g. shadow-variable warnings) from graph construction.
    pub findings: Vec<Finding>,
    pub mode: String,
    pub confidence: String,
}

impl CostGraph {
    pub fn from_python(source: &str) -> Result<Self, PyErr> {
        let mut builder = PythonGraphBuilder::new();
        let (nodes, edges, findings) = builder.build_from_source(source);

        Ok(CostGraph {
            nodes,
            edges,
            findings,
            mode: "python".to_string(),
            confidence: "low".to_string(),
        })
    }

    pub fn from_sql(source: &str) -> Result<Self, PyErr> {
        let mut builder = SqlGraphBuilder::new();
        let (nodes, edges) = builder.build_from_source(source);

        Ok(CostGraph {
            nodes,
            edges,
            findings: Vec::new(),
            mode: "sql".to_string(),
            confidence: "low".to_string(),
        })
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PipelineGraph {
    pub tables: Vec<PipelineTable>,
    pub mode: String,
    pub confidence: String,
}

impl PipelineGraph {
    pub fn from_sdp(source: &str) -> Self {
        let mut builder = SdpGraphBuilder::new();
        let (tables, _edges) = builder.build_from_source(source);

        PipelineGraph {
            tables,
            mode: "sdp".to_string(),
            confidence: "low".to_string(),
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

impl From<PipelineGraph> for PipelineGraphPy {
    fn from(g: PipelineGraph) -> Self {
        PipelineGraphPy {
            tables: g.tables.into_iter().map(|t| t.into()).collect(),
            mode: g.mode,
            confidence: g.confidence,
        }
    }
}
