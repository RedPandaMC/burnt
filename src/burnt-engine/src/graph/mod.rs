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
        use crate::parse::import_map::ImportMap;
        use tree_sitter::Parser;

        let (nodes, edges, findings) = PythonGraphBuilder::new().build_from_source(source);

        let mut graph = Graph {
            nodes,
            edges,
            findings,
            mode: "python".to_string(),
            confidence: "low".to_string(),
        };
        crate::resolved::populate_dag_facts(&mut graph);

        // Populate ScopeFacts.namespace by classifying each node's call site
        // against the file's ImportMap. Done at finalise time so the builder
        // stays import-map-free.
        let mut parser = Parser::new();
        if parser
            .set_language(&tree_sitter_python::LANGUAGE.into())
            .is_ok()
        {
            if let Some(tree) = parser.parse(source, None) {
                let imap = ImportMap::build(source, tree.root_node());
                populate_python_namespaces(&mut graph, &imap);
            }
        }

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

/// Resolve each Python node's namespace via the file's `ImportMap` and
/// stash it on `node.scope.namespace`. Keeps the DSL out of `ImportMap`
/// lookups at rule-execution time.
fn populate_python_namespaces(graph: &mut Graph, imap: &crate::parse::import_map::ImportMap) {
    use crate::resolved::Namespace;
    for node in &mut graph.nodes {
        let Some(source_code) = node.source_code.as_deref() else {
            continue;
        };
        // Prefer the AST `method_chain` when available — more precise than
        // text-based heuristics.
        let head = node
            .ast
            .as_ref()
            .and_then(|s| match &s.root {
                crate::resolved::AstNode::Call(c) => c.method_chain.first().cloned(),
                _ => None,
            })
            .or_else(|| {
                // Fall back to text-based extraction of the head identifier.
                imap.extract_call_parts(source_code)
                    .map(|(ns, _)| ns.to_string())
            });

        let Some(head) = head else { continue };
        node.scope.namespace = Some(classify_namespace(&head, imap));
    }
}

fn classify_namespace(
    head: &str,
    imap: &crate::parse::import_map::ImportMap,
) -> crate::resolved::Namespace {
    use crate::resolved::Namespace;
    if head == "spark" || imap.is_spark_ns(head) {
        return Namespace::Spark;
    }
    if imap.is_pipeline_ns(head) {
        // The pipeline namespace can resolve to either `dlt` or `dp`; check
        // the underlying module to disambiguate.
        match imap.resolve(head) {
            Some("dp") => Namespace::Dp,
            _ => Namespace::Dlt,
        }
    } else if let Some(module) = imap.resolve(head) {
        Namespace::UserDefined(module.to_string())
    } else {
        Namespace::Unknown
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
