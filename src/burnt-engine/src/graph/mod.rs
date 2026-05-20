use pyo3::prelude::*;

use crate::types::{Edge, Finding, Node, PipelineTable, PyEdge, PyNode, PyPipelineTable};

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

        let (nodes, edges, findings) = PythonGraphBuilder::new()
            .build_from_source(source)
            .map_err(PyErr::from)?;

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

        graph.validate();
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
        graph.validate();
        Ok(graph)
    }

    #[cfg(debug_assertions)]
    pub fn validate(&self) {
        let node_ids: std::collections::HashSet<_> = self.nodes.iter().map(|n| &n.id).collect();

        for edge in &self.edges {
            if !node_ids.contains(&edge.source) {
                panic!(
                    "Graph invariant violation: dangling edge source {} (type={}, target={})",
                    edge.source, edge.edge_type, edge.target
                );
            }
            if !node_ids.contains(&edge.target) {
                panic!(
                    "Graph invariant violation: dangling edge target {} (type={}, source={})",
                    edge.target, edge.edge_type, edge.source
                );
            }
        }

        let mut seen_ids: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for node in &self.nodes {
            if !seen_ids.insert(node.id.as_str()) {
                panic!("Graph invariant violation: duplicate node id {}", node.id);
            }
        }

        if let Some(root) = &self.nodes.first() {
            if !self.nodes.iter().any(|n| n.id == root.id) {
                panic!("Graph invariant violation: invalid root id {}", root.id);
            }
        }

        if let Some(cycle) = detect_cycles(self) {
            panic!("Graph invariant violation: cycle detected {:?}", cycle);
        }
    }

    #[cfg(not(debug_assertions))]
    pub fn validate(&self) {}
}

fn detect_cycles(graph: &Graph) -> Option<Vec<String>> {
    let mut adjacency: std::collections::HashMap<&str, Vec<&str>> =
        std::collections::HashMap::new();
    for node in &graph.nodes {
        adjacency.entry(node.id.as_str()).or_default();
    }
    for edge in &graph.edges {
        adjacency
            .entry(edge.source.as_str())
            .or_default()
            .push(edge.target.as_str());
    }

    let mut visited: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut stack: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut path: Vec<String> = Vec::new();

    fn dfs(
        node: &str,
        adjacency: &std::collections::HashMap<&str, Vec<&str>>,
        visited: &mut std::collections::HashSet<String>,
        stack: &mut std::collections::HashSet<String>,
        path: &mut Vec<String>,
    ) -> Option<Vec<String>> {
        let node_owned = node.to_string();
        if stack.contains(&node_owned) {
            let cycle_start = path.iter().position(|n| n == node)?;
            return Some(path[cycle_start..].to_vec());
        }
        if visited.contains(&node_owned) {
            return None;
        }
        visited.insert(node_owned.clone());
        stack.insert(node_owned.clone());
        path.push(node_owned.clone());
        if let Some(neighbors) = adjacency.get(node) {
            for neighbor in neighbors {
                if let Some(cycle) = dfs(neighbor, adjacency, visited, stack, path) {
                    return Some(cycle);
                }
            }
        }
        path.pop();
        stack.remove(&node_owned);
        None
    }

    for node in &graph.nodes {
        if !visited.contains(&node.id) {
            if let Some(cycle) = dfs(
                node.id.as_str(),
                &adjacency,
                &mut visited,
                &mut stack,
                &mut path,
            ) {
                return Some(cycle);
            }
        }
    }
    None
}

/// Resolve each Python node's namespace via the file's `ImportMap` and
/// stash it on `node.scope.namespace`. Keeps the DSL out of `ImportMap`
/// lookups at rule-execution time.
fn populate_python_namespaces(graph: &mut Graph, imap: &crate::parse::import_map::ImportMap) {
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
