//! Per-node scope facts: namespace, bindings, DAG ancestry, source order.
//!
//! These captures pull the inputs today's Context and Dataflow rules consume
//! from outside the graph (`ImportMap`, ad-hoc string searches, post-hoc DAG
//! walks) directly onto the node. Together with `AstShape` they make the
//! resolved graph the single query surface — rules never need the
//! `RuleContext` side-channel the legacy Context pipeline threaded around.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::graph::Graph;
use crate::resolved::ids::StaticNodeId;

/// Symbolic namespace of a call's leading identifier, resolved by
/// [`ImportMap`](crate::parse::import_map::ImportMap).
///
/// Replaces the runtime `ImportMap` lookups today's Context rules do; once
/// `ScopeFacts.namespace` is populated, the DSL can ask
/// `:scope/namespace Spark` without touching `ImportMap` at rule-execution
/// time. `UserDefined(name)` carries the resolved variable for cases the
/// matcher wants to reason about specifically.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum Namespace {
    Spark,
    Dlt,
    Dp,
    PandasOnSpark,
    UserDefined(String),
    Unknown,
}

/// Bag of facts about the scope a node lives in.
///
/// All fields default to empty / `None`; builders populate as they learn.
/// Pre-computing `ancestors` / `descendants` once per build is cheaper than
/// re-walking edges for every DSL query that needs them.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[non_exhaustive]
pub struct ScopeFacts {
    /// Resolved namespace of the call's leading identifier
    /// (e.g. `spark`, `dlt`). `None` until the builder populates.
    pub namespace: Option<Namespace>,
    /// Variables in scope at the call site, mapped to the static node
    /// that defined them. Drives `#binds @cap "var-name"` predicates.
    pub bindings: HashMap<String, StaticNodeId>,
    /// Variables this node reads (RHS identifiers). Drives sequencing
    /// predicates like `#reads @cap "var-name"`.
    pub reads: Vec<String>,
    /// Variables this node writes (LHS / for-loop target).
    pub writes: Vec<String>,
    /// Source-order index — gives the DSL a stable "before / after"
    /// ordering for sequencing predicates without re-walking edges.
    pub source_order: u32,
    /// DAG ancestors (transitive). Pre-computed during builder finalise
    /// so DSL `(ancestors @x)` traversal is O(1) lookup.
    pub ancestors: Vec<StaticNodeId>,
    /// DAG descendants (transitive). Symmetric to `ancestors`.
    pub descendants: Vec<StaticNodeId>,
    /// True when the call node is syntactically inside a `for` or `while`
    /// loop body. Populated by the Python builder via tree-sitter ancestor
    /// walk. Used by BD016 (write in loop) and BP020 (withColumn in loop).
    pub in_for_loop: bool,
}

impl ScopeFacts {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// Walk a `Graph`'s edges and populate `ScopeFacts.ancestors` /
/// `descendants` on every node by transitive closure.
///
/// O(V·E) in the worst case — fine for typical graphs (≤200 nodes, ≤500
/// edges); a quadratic blowup on pathological cases would still complete
/// in microseconds for the file sizes burnt sees. The algorithm is
/// intentionally simple to keep this commit's surface small.
pub fn populate_dag_facts(graph: &mut Graph) {
    // Index nodes by id for O(1) edge resolution.
    let mut id_to_idx: HashMap<String, usize> = HashMap::new();
    for (i, node) in graph.nodes.iter().enumerate() {
        id_to_idx.insert(node.id.clone(), i);
    }

    // Adjacency lists.
    let n = graph.nodes.len();
    let mut out: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut inn: Vec<Vec<usize>> = vec![Vec::new(); n];
    for edge in &graph.edges {
        let (Some(&s), Some(&t)) = (id_to_idx.get(&edge.source), id_to_idx.get(&edge.target))
        else {
            continue;
        };
        out[s].push(t);
        inn[t].push(s);
    }

    // Source-order indices (1-based to match line_number conventions).
    for (i, node) in graph.nodes.iter_mut().enumerate() {
        node.scope.source_order = u32::try_from(i + 1).unwrap_or(u32::MAX);
    }

    // Transitive closure via BFS from each node — O(V·E) but trivially
    // correct on small graphs.
    for i in 0..n {
        let descendants = transitive_closure(&out, i);
        let ancestors = transitive_closure(&inn, i);
        graph.nodes[i].scope.descendants = descendants
            .into_iter()
            .map(|j| StaticNodeId::new(graph.nodes[j].id.clone()))
            .collect();
        graph.nodes[i].scope.ancestors = ancestors
            .into_iter()
            .map(|j| StaticNodeId::new(graph.nodes[j].id.clone()))
            .collect();
    }
}

fn transitive_closure(adj: &[Vec<usize>], start: usize) -> Vec<usize> {
    use std::collections::HashSet;
    let mut visited: HashSet<usize> = HashSet::new();
    let mut stack: Vec<usize> = adj[start].clone();
    let mut out: Vec<usize> = Vec::new();
    while let Some(u) = stack.pop() {
        if !visited.insert(u) {
            continue;
        }
        out.push(u);
        stack.extend_from_slice(&adj[u]);
    }
    out.sort_unstable();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Edge, Node, OperationKind, ScalingBehavior};

    fn mk_node(id: &str) -> Node {
        Node {
            id: id.to_string(),
            kind: OperationKind::Read,
            scaling_type: ScalingBehavior::Linear,
            photon_eligible: false,
            shuffle_required: false,
            driver_bound: false,
            tables_referenced: Vec::new(),
            estimated_input_bytes: None,
            estimated_cost_usd: None,
            line_number: None,
            source_code: None,
            ast: None,
            scope: ScopeFacts::default(),
        }
    }

    fn mk_edge(s: &str, t: &str) -> Edge {
        Edge {
            source: s.to_string(),
            target: t.to_string(),
            edge_type: "data_flow".to_string(),
        }
    }

    #[test]
    fn default_scope_facts_is_empty() {
        let s = ScopeFacts::default();
        assert!(s.namespace.is_none());
        assert!(s.bindings.is_empty());
        assert!(s.reads.is_empty());
        assert!(s.writes.is_empty());
        assert_eq!(s.source_order, 0);
        assert!(s.ancestors.is_empty());
        assert!(s.descendants.is_empty());
    }

    #[test]
    fn populate_dag_facts_computes_transitive_closure() {
        // a -> b -> c, plus a -> c shortcut.
        let mut g = Graph {
            nodes: vec![mk_node("a"), mk_node("b"), mk_node("c")],
            edges: vec![mk_edge("a", "b"), mk_edge("b", "c"), mk_edge("a", "c")],
            findings: Vec::new(),
            mode: "python".into(),
            confidence: "low".into(),
        };
        populate_dag_facts(&mut g);

        let descendants_of = |id: &str| -> Vec<String> {
            g.nodes
                .iter()
                .find(|n| n.id == id)
                .unwrap()
                .scope
                .descendants
                .iter()
                .map(|x| x.as_str().to_string())
                .collect()
        };
        let ancestors_of = |id: &str| -> Vec<String> {
            g.nodes
                .iter()
                .find(|n| n.id == id)
                .unwrap()
                .scope
                .ancestors
                .iter()
                .map(|x| x.as_str().to_string())
                .collect()
        };

        let mut a_desc = descendants_of("a");
        a_desc.sort();
        assert_eq!(a_desc, vec!["b".to_string(), "c".to_string()]);

        let mut c_anc = ancestors_of("c");
        c_anc.sort();
        assert_eq!(c_anc, vec!["a".to_string(), "b".to_string()]);

        assert!(ancestors_of("a").is_empty());
        assert!(descendants_of("c").is_empty());
    }

    #[test]
    fn populate_dag_facts_assigns_source_order() {
        let mut g = Graph {
            nodes: vec![mk_node("x"), mk_node("y"), mk_node("z")],
            edges: Vec::new(),
            findings: Vec::new(),
            mode: "python".into(),
            confidence: "low".into(),
        };
        populate_dag_facts(&mut g);
        assert_eq!(g.nodes[0].scope.source_order, 1);
        assert_eq!(g.nodes[1].scope.source_order, 2);
        assert_eq!(g.nodes[2].scope.source_order, 3);
    }

    #[test]
    fn populate_dag_facts_handles_disconnected_components() {
        let mut g = Graph {
            nodes: vec![mk_node("a"), mk_node("b"), mk_node("c"), mk_node("d")],
            edges: vec![mk_edge("a", "b"), mk_edge("c", "d")],
            findings: Vec::new(),
            mode: "python".into(),
            confidence: "low".into(),
        };
        populate_dag_facts(&mut g);
        let scope_of = |id: &str| -> &ScopeFacts {
            &g.nodes.iter().find(|n| n.id == id).unwrap().scope
        };
        assert_eq!(scope_of("a").descendants.len(), 1);
        assert_eq!(scope_of("a").descendants[0].as_str(), "b");
        assert!(scope_of("a").ancestors.is_empty());
        assert_eq!(scope_of("c").descendants.len(), 1);
        assert_eq!(scope_of("c").descendants[0].as_str(), "d");
    }

    #[test]
    fn namespace_variants_round_trip_serde() {
        let cases = vec![
            Namespace::Spark,
            Namespace::Dlt,
            Namespace::Dp,
            Namespace::PandasOnSpark,
            Namespace::UserDefined("my_module".into()),
            Namespace::Unknown,
        ];
        for ns in cases {
            let s = serde_json::to_string(&ns).unwrap();
            let parsed: Namespace = serde_json::from_str(&s).unwrap();
            assert_eq!(parsed, ns);
        }
    }
}
