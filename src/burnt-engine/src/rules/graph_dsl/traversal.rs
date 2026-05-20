//! Graph + AST traversal helpers used by quantifier and value-extraction
//! predicates.
//!
//! Traversal subjects in the DSL are expressed as predicate-form calls
//! that return a `CaptureValue::List`. The matcher dispatches them
//! through the predicate registry like any other predicate — the only
//! distinguisher is the returned `PredResult::Value` shape.

use std::sync::Arc;

use crate::resolved::ast_shape::{AstArg, AstNode, CallNode};
use crate::resolved::ids::StaticNodeId;
use crate::resolved::ResolvedGraph;
use crate::rules::graph_dsl::value::CaptureValue;

/// Return the DAG descendants of a node as a list of `Node` captures.
/// Reads from the pre-computed `ScopeFacts.descendants` set so this is
/// O(d) where d is the descendant count — no graph re-walk.
pub fn descendants_of(resolved: &ResolvedGraph, id: &StaticNodeId) -> Vec<CaptureValue> {
    resolved
        .graph()
        .nodes
        .iter()
        .find(|n| n.id == id.as_str())
        .map(|n| {
            n.scope
                .descendants
                .iter()
                .map(|d| CaptureValue::Node(d.clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// Symmetric to [`descendants_of`].
pub fn ancestors_of(resolved: &ResolvedGraph, id: &StaticNodeId) -> Vec<CaptureValue> {
    resolved
        .graph()
        .nodes
        .iter()
        .find(|n| n.id == id.as_str())
        .map(|n| {
            n.scope
                .ancestors
                .iter()
                .map(|a| CaptureValue::Node(a.clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// Siblings of a node — nodes that share a parent in the DAG. Implemented
/// as the union of every parent's other descendants. Cheap for typical
/// fan-out shapes (1–2 parents per node).
pub fn siblings_of(resolved: &ResolvedGraph, id: &StaticNodeId) -> Vec<CaptureValue> {
    let Some(node) = resolved.graph().nodes.iter().find(|n| n.id == id.as_str()) else {
        return Vec::new();
    };
    let parents: Vec<&StaticNodeId> = node
        .scope
        .ancestors
        .iter()
        .filter(|a| {
            let parent_node = resolved.graph().nodes.iter().find(|n| n.id == a.as_str());
            // A direct parent has the current node as a *child*, i.e. it
            // appears in the parent's descendants but doesn't have any
            // other ancestor pointing at it that's "between" us. The
            // simplest correct approximation: direct ancestor whose
            // descendants include only us via 1 hop. Without an edge
            // index we approximate by treating every ancestor as a
            // potential parent and deduping at the end.
            parent_node
                .map(|p| {
                    p.scope
                        .descendants
                        .iter()
                        .any(|d| d.as_str() == id.as_str())
                })
                .unwrap_or(false)
        })
        .collect();

    let mut sibs: std::collections::HashSet<String> = std::collections::HashSet::new();
    for parent in parents {
        for desc in resolved
            .graph()
            .nodes
            .iter()
            .find(|n| n.id == parent.as_str())
            .map(|p| p.scope.descendants.iter())
            .into_iter()
            .flatten()
        {
            if desc.as_str() != id.as_str() {
                sibs.insert(desc.as_str().to_string());
            }
        }
    }
    sibs.into_iter()
        .map(|s| CaptureValue::Node(StaticNodeId::new(s)))
        .collect()
}

/// Return the *receiver* of an `AstArg::Call` — the leading element of
/// its method chain. For `df.collect()`, the receiver is `"df"`.
pub fn receiver_of_call(arg: &AstArg) -> Option<String> {
    let AstArg::Call(call) = arg else {
        return None;
    };
    let len = call.method_chain.len();
    if len < 2 {
        return None;
    }
    Some(call.method_chain[..len - 1].join("."))
}

/// Return every method name in the callee chain *below* the given call —
/// for `df.filter().select().collect()` invoked on the outermost call,
/// returns `["filter", "select"]` (collect itself is excluded).
pub fn callees_of_call(arg: &AstArg) -> Vec<CaptureValue> {
    let AstArg::Call(call) = arg else {
        return Vec::new();
    };
    let mut out = Vec::new();
    walk_chained_callees(call, &mut out);
    out.into_iter()
        .map(|s| CaptureValue::String(Arc::from(s)))
        .collect()
}

fn walk_chained_callees(call: &CallNode, out: &mut Vec<String>) {
    // Each step of the chain (minus the leaf) is a callee. `method_chain`
    // already encodes them; arg-call nesting is for nested function
    // application (e.g. `f(g(x))`) and is captured separately.
    let len = call.method_chain.len();
    if len < 2 {
        return;
    }
    for name in &call.method_chain[1..len - 1] {
        out.push(name.clone());
    }
    // Walk into nested AstArg::Call positions for deeply chained shapes.
    for arg in &call.args {
        if let AstArg::Call(nested) = arg {
            walk_chained_callees(nested, out);
        }
    }
}

/// Inspect an `AstNode` and return the kwarg names it carries. Used by
/// `#kwargs/has` and `#kwargs/missing` predicates.
pub fn kwarg_names(node: &AstNode) -> Vec<String> {
    match node {
        AstNode::Call(c) => c.kwargs.iter().map(|(k, _)| k.clone()).collect(),
        AstNode::Decorator(d) => d.kwargs.iter().map(|(k, _)| k.clone()).collect(),
        _ => Vec::new(),
    }
}

/// True iff `node` is a Call whose method-chain leading receiver equals
/// `expected` (e.g. expected="df" matches `df.filter()`).
pub fn call_receiver_matches(node: &AstNode, expected: &str) -> bool {
    let AstNode::Call(c) = node else {
        return false;
    };
    c.method_chain.first().map(String::as_str) == Some(expected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolved::scope_facts::populate_dag_facts;
    use crate::resolved::scope_facts::ScopeFacts;
    use crate::resolved::ResolvedGraphBuilder;
    use crate::types::{Edge, Node, OperationKind, ScalingBehavior};

    fn mk_node(id: &str) -> Node {
        Node {
            id: id.into(),
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

    fn mk_resolved(nodes: Vec<Node>, edges: Vec<Edge>) -> crate::resolved::ResolvedGraph {
        let mut g = crate::graph::Graph {
            nodes,
            edges,
            findings: Vec::new(),
            mode: "python".into(),
            confidence: "low".into(),
        };
        populate_dag_facts(&mut g);
        ResolvedGraphBuilder::new(g).build()
    }

    fn edge(src: &str, tgt: &str) -> Edge {
        Edge {
            source: src.into(),
            target: tgt.into(),
            edge_type: "data_flow".into(),
        }
    }

    #[test]
    fn descendants_of_returns_transitive_set() {
        // a → b → c
        let resolved = mk_resolved(
            vec![mk_node("a"), mk_node("b"), mk_node("c")],
            vec![edge("a", "b"), edge("b", "c")],
        );
        let id = StaticNodeId::new("a".to_string());
        let descs = descendants_of(&resolved, &id);
        let ids: Vec<String> = descs
            .iter()
            .filter_map(|v| match v {
                CaptureValue::Node(id) => Some(id.as_str().to_string()),
                _ => None,
            })
            .collect();
        assert!(ids.contains(&"b".to_string()));
        assert!(ids.contains(&"c".to_string()));
        assert_eq!(ids.len(), 2);
    }

    #[test]
    fn ancestors_of_returns_transitive_set() {
        let resolved = mk_resolved(
            vec![mk_node("a"), mk_node("b"), mk_node("c")],
            vec![edge("a", "b"), edge("b", "c")],
        );
        let id = StaticNodeId::new("c".to_string());
        let ancs = ancestors_of(&resolved, &id);
        assert_eq!(ancs.len(), 2);
    }

    #[test]
    fn receiver_of_call_extracts_leading_chain() {
        let arg = AstArg::Call(Box::new(CallNode {
            method_chain: vec!["df".into(), "filter".into(), "collect".into()],
            args: Vec::new(),
            kwargs: Vec::new(),
            line: 1,
            column: 1,
        }));
        assert_eq!(receiver_of_call(&arg), Some("df.filter".into()));

        // Single-element chain has no receiver.
        let arg2 = AstArg::Call(Box::new(CallNode {
            method_chain: vec!["foo".into()],
            args: Vec::new(),
            kwargs: Vec::new(),
            line: 1,
            column: 1,
        }));
        assert_eq!(receiver_of_call(&arg2), None);
    }

    #[test]
    fn kwarg_names_lists_keys_for_call_and_decorator() {
        let call = AstNode::Call(CallNode {
            method_chain: vec!["f".into()],
            args: Vec::new(),
            kwargs: vec![
                ("a".into(), AstArg::Identifier("x".into())),
                ("b".into(), AstArg::Identifier("y".into())),
            ],
            line: 1,
            column: 1,
        });
        assert_eq!(kwarg_names(&call), vec!["a", "b"]);
    }

    #[test]
    fn call_receiver_matches_leading_chain_token() {
        let call = AstNode::Call(CallNode {
            method_chain: vec!["spark".into(), "read".into(), "parquet".into()],
            args: Vec::new(),
            kwargs: Vec::new(),
            line: 1,
            column: 1,
        });
        assert!(call_receiver_matches(&call, "spark"));
        assert!(!call_receiver_matches(&call, "df"));
    }
}
