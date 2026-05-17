use pyo3::prelude::*;

use crate::types::{
    Edge, Node, Finding, PipelineTable, PyEdge, PyNode, PyPipelineTable,
};

pub mod python;
pub mod sdp;
pub mod sql;

use python::PythonGraphBuilder;
use sdp::SdpGraphBuilder;
use sql::SqlGraphBuilder;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Graph {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    /// Semantic findings (e.g. shadow-variable warnings) from graph construction.
    pub findings: Vec<Finding>,
    pub mode: String,
    pub confidence: String,
}

impl Graph {
    pub fn from_python(source: &str) -> Result<Self, PyErr> {
        let (nodes, edges, findings) = PythonGraphBuilder::new().build_from_source(source);

        let mut graph = Graph {
            nodes,
            edges,
            findings,
            mode: "python".to_string(),
            confidence: "low".to_string(),
        };
        crate::resolved::populate_dag_facts(&mut graph);
        Ok(graph)
    }

    pub fn from_sql(source: &str) -> Result<Self, PyErr> {
        let (nodes, edges) = SqlGraphBuilder::new().build_from_source(source);

        let mut graph = Graph {
            nodes,
            edges,
            findings: Vec::new(),
            mode: "sql".to_string(),
            confidence: "low".to_string(),
        };
        crate::resolved::populate_dag_facts(&mut graph);
        Ok(graph)
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
        let (tables, _edges) = SdpGraphBuilder::new().build_from_source(source);

        PipelineGraph {
            tables,
            mode: "sdp".to_string(),
            confidence: "low".to_string(),
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyGraph {
    #[pyo3(get)]
    pub nodes: Vec<PyNode>,
    #[pyo3(get)]
    pub edges: Vec<PyEdge>,
    #[pyo3(get)]
    pub mode: String,
    #[pyo3(get)]
    pub confidence: String,
}

impl From<Graph> for PyGraph {
    fn from(g: Graph) -> Self {
        PyGraph {
            nodes: g.nodes.into_iter().map(|n| n.into()).collect(),
            edges: g.edges.into_iter().map(|e| e.into()).collect(),
            mode: g.mode,
            confidence: g.confidence,
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyPipeline {
    #[pyo3(get)]
    pub tables: Vec<PyPipelineTable>,
    #[pyo3(get)]
    pub mode: String,
    #[pyo3(get)]
    pub confidence: String,
}

impl From<PipelineGraph> for PyPipeline {
    fn from(g: PipelineGraph) -> Self {
        PyPipeline {
            tables: g.tables.into_iter().map(|t| t.into()).collect(),
            mode: g.mode,
            confidence: g.confidence,
        }
    }
}
