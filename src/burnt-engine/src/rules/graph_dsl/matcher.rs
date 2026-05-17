//! Pattern matcher over `ResolvedGraph`.
//!
//! The matcher takes a parsed [`Pattern`] and a `ResolvedGraph` and
//! yields zero or more [`DslMatch`]es — one per successful structural
//! match that survives every predicate in the pattern body. Captures
//! accumulated during the walk flow into the eventual `Finding` via
//! the `[graph.finding]` template.
//!
//! # Head dispatch
//!
//! The first thing the matcher does is select candidates by the
//! pattern's outermost head (`op:<Kind>`, `edge:<Kind>`, `overlay:<Kind>`,
//! `fact:<Name>`). Each candidate is then walked through the pattern's
//! body items in source order. A pattern can bind any number of
//! captures and run any number of predicates; if any predicate returns
//! `false` the match is pruned. `SetFinding` results from `#when`
//! predicates accumulate into a single `FindingMutation` on the match.
//!
//! # AST descent
//!
//! When the body contains a nested `ast/<Kind>` sub-pattern, the matcher
//! drops into the candidate's `AstShape.root` and tries to match the
//! variant + props. The Phase A `AstShape` design keeps the tree narrow
//! (Call / Decorator / Assignment / FunctionDef / SqlStatement /
//! SqlExpression) so a single switch covers it.

use std::sync::Arc;

use crate::graph::Graph;
use crate::resolved::ast_shape::{AstNode, CallNode};
use crate::resolved::ids::StaticNodeId;
use crate::resolved::ResolvedGraph;
use crate::rules::graph_dsl::context::{FindingMutation, MatchCtx};
use crate::rules::graph_dsl::ir::{
    Head, Pattern, PatternBody, PredArg, Predicate, Prefix, Value,
};
use crate::rules::graph_dsl::predicate::{evaluate_predicate, PredResult};
use crate::rules::graph_dsl::value::{ast_arg_kind, CaptureMap, CaptureValue};
use crate::types::{Node, OperationKind};

/// One successful pattern match — captures + accumulated finding
/// mutations from any `#when` predicates that fired.
#[derive(Debug, Clone)]
pub struct DslMatch {
    pub captures: CaptureMap,
    pub mutation: FindingMutation,
    /// The static node id of the candidate the match started from.
    /// Always present (every match is anchored on a `Node`).
    pub anchor: StaticNodeId,
}

/// Run a pattern against the resolved graph and collect every match.
///
/// `exclude`, when provided, is run as a parallel pattern over the
/// same candidate set; any anchor that matches `exclude` is removed
/// from the result. This mirrors the legacy `[query.exclude]`
/// semantics in a graph-pattern shape.
pub fn run_pattern(
    pattern: &Pattern,
    exclude: Option<&Pattern>,
    resolved: &ResolvedGraph,
) -> Vec<DslMatch> {
    let mut matches = match_pattern_at_root(pattern, resolved);
    if let Some(excl) = exclude {
        let excluded_anchors: std::collections::HashSet<String> =
            match_pattern_at_root(excl, resolved)
                .into_iter()
                .map(|m| m.anchor.as_str().to_string())
                .collect();
        matches.retain(|m| !excluded_anchors.contains(m.anchor.as_str()));
    }
    matches
}

// ---------------------------------------------------------------------
// Top-level dispatch by head prefix
// ---------------------------------------------------------------------

fn match_pattern_at_root(pattern: &Pattern, resolved: &ResolvedGraph) -> Vec<DslMatch> {
    match pattern.head.prefix {
        Prefix::Op => match_op_pattern(pattern, resolved),
        Prefix::Edge => match_edge_pattern(pattern, resolved),
        Prefix::Overlay => match_overlay_pattern(pattern, resolved),
        Prefix::Fact => match_fact_pattern(pattern, resolved),
        // ast/<Kind> patterns are only valid as nested sub-patterns;
        // they can't be top-level heads because they have no anchor.
        Prefix::Ast => Vec::new(),
    }
}

fn match_op_pattern(pattern: &Pattern, resolved: &ResolvedGraph) -> Vec<DslMatch> {
    let target_kind = parse_op_kind(&pattern.head.kind);
    let mut out = Vec::new();
    for node in &resolved.graph().nodes {
        if let Some(ref k) = target_kind {
            if &node.kind != k {
                continue;
            }
        }
        let mut captures: CaptureMap = CaptureMap::new();
        captures.insert(
            "__current".into(),
            CaptureValue::Node(StaticNodeId::new(node.id.clone())),
        );
        let mut mutation = FindingMutation::default();
        if !match_op_props(pattern, node, &mut captures) {
            continue;
        }
        if !match_body_items(pattern, node, resolved, &mut captures, &mut mutation) {
            continue;
        }
        out.push(DslMatch {
            captures,
            mutation,
            anchor: StaticNodeId::new(node.id.clone()),
        });
    }
    out
}

fn match_edge_pattern(pattern: &Pattern, resolved: &ResolvedGraph) -> Vec<DslMatch> {
    let target_kind = pattern.head.kind.as_str();
    let mut out = Vec::new();
    for edge in &resolved.graph().edges {
        if !edge_kind_matches(&edge.edge_type, target_kind) {
            continue;
        }
        let mut captures: CaptureMap = CaptureMap::new();
        let from_id = StaticNodeId::new(edge.source.clone());
        let to_id = StaticNodeId::new(edge.target.clone());
        captures.insert(
            "__current".into(),
            CaptureValue::Edge {
                from: from_id.clone(),
                to: to_id.clone(),
                kind: Arc::from(edge.edge_type.as_str()),
            },
        );

        let mut ok = true;
        for (key, value) in &pattern.props {
            match key.as_str() {
                "from" => {
                    if let Value::CaptureRef(name) = value {
                        captures.insert(name.clone(), CaptureValue::Node(from_id.clone()));
                    }
                }
                "to" => {
                    if let Value::CaptureRef(name) = value {
                        captures.insert(name.clone(), CaptureValue::Node(to_id.clone()));
                    }
                }
                _ => {
                    ok = false;
                    break;
                }
            }
        }
        if !ok {
            continue;
        }

        let mut mutation = FindingMutation::default();
        // Edge patterns rarely have body items; just run any predicates.
        let mut body_ok = true;
        for item in &pattern.body {
            if let PatternBody::Predicate(p) = item {
                if !run_predicate(p, resolved, &mut captures, &mut mutation) {
                    body_ok = false;
                    break;
                }
            }
        }
        if body_ok {
            out.push(DslMatch {
                captures,
                mutation,
                anchor: from_id,
            });
        }
    }
    out
}

fn match_overlay_pattern(pattern: &Pattern, resolved: &ResolvedGraph) -> Vec<DslMatch> {
    let kind = pattern.head.kind.as_str();
    let mut out = Vec::new();
    for node in &resolved.graph().nodes {
        let Some(overlay) = resolved.overlay(&node.id) else {
            continue;
        };
        let has_match = match kind {
            "Stage" => !overlay.stages.is_empty(),
            "PlanSubtree" => overlay.plan_subtree.is_some(),
            "TableSpec" => node
                .tables_referenced
                .iter()
                .any(|t| resolved.table_spec(&t.fqn()).is_some()),
            _ => false,
        };
        if !has_match {
            continue;
        }
        let mut captures: CaptureMap = CaptureMap::new();
        captures.insert(
            "__current".into(),
            CaptureValue::Node(StaticNodeId::new(node.id.clone())),
        );
        let mut mutation = FindingMutation::default();
        if !match_body_items(pattern, node, resolved, &mut captures, &mut mutation) {
            continue;
        }
        out.push(DslMatch {
            captures,
            mutation,
            anchor: StaticNodeId::new(node.id.clone()),
        });
    }
    out
}

fn match_fact_pattern(pattern: &Pattern, resolved: &ResolvedGraph) -> Vec<DslMatch> {
    // Graph-level facts are one-shot — they match against the graph as
    // a whole and produce zero or one DslMatch. The matched fact value
    // is bound under `__fact` for predicates to inspect.
    let kind = pattern.head.kind.as_str();
    let fact_value = match kind {
        "Mode" => Some(CaptureValue::String(resolved.graph().mode.clone().into())),
        "Confidence" => Some(CaptureValue::String(resolved.graph().confidence.clone().into())),
        "NodeCount" => Some(CaptureValue::Number(resolved.graph().nodes.len() as f64)),
        "EdgeCount" => Some(CaptureValue::Number(resolved.graph().edges.len() as f64)),
        "UnmatchedStages" => Some(CaptureValue::Number(
            resolved.unmatched().stages.len() as f64,
        )),
        _ => None,
    };
    let Some(value) = fact_value else {
        return Vec::new();
    };
    let mut captures: CaptureMap = CaptureMap::new();
    captures.insert("__fact".into(), value);
    let mut mutation = FindingMutation::default();
    // Facts don't have a graph node as anchor; fabricate a stable
    // synthetic id so downstream code can still attribute the finding.
    let anchor = StaticNodeId::new(format!("__fact:{}", pattern.head.kind));

    // Run any predicates over the bound __fact.
    for item in &pattern.body {
        if let PatternBody::Predicate(p) = item {
            if !run_predicate(p, resolved, &mut captures, &mut mutation) {
                return Vec::new();
            }
        }
    }
    vec![DslMatch {
        captures,
        mutation,
        anchor,
    }]
}

// ---------------------------------------------------------------------
// Per-prefix props + body
// ---------------------------------------------------------------------

fn match_op_props(pattern: &Pattern, node: &Node, captures: &mut CaptureMap) -> bool {
    for (key, value) in &pattern.props {
        let ok = match key.as_str() {
            "kind" => value_to_string(value)
                .map(|s| parse_op_kind(&s) == Some(node.kind.clone()))
                .unwrap_or(false),
            "shuffle-required" => match value {
                Value::Bool(b) => node.shuffle_required == *b,
                _ => false,
            },
            "photon-eligible" => match value {
                Value::Bool(b) => node.photon_eligible == *b,
                _ => false,
            },
            "driver-bound" => match value {
                Value::Bool(b) => node.driver_bound == *b,
                _ => false,
            },
            "line" | "line-number" => {
                if let (Value::Number(n), Some(line)) = (value, node.line_number) {
                    (line as f64 - n).abs() < f64::EPSILON
                } else {
                    false
                }
            }
            "tables-include" => value_to_string(value)
                .map(|target| node.tables_referenced.iter().any(|t| t.fqn() == target))
                .unwrap_or(false),
            "scope/namespace" => {
                let want = value_to_string(value).unwrap_or_default();
                let ns = node.scope.namespace.as_ref().map(namespace_name);
                ns.as_deref() == Some(want.as_str())
            }
            _ => {
                // Unknown prop — bind the capture for diagnostic but don't fail.
                // This keeps forward-compat for future prop kinds.
                if let Value::CaptureRef(name) = value {
                    captures.insert(
                        name.clone(),
                        CaptureValue::Node(StaticNodeId::new(node.id.clone())),
                    );
                }
                true
            }
        };
        if !ok {
            return false;
        }
    }
    true
}

fn match_body_items(
    pattern: &Pattern,
    node: &Node,
    resolved: &ResolvedGraph,
    captures: &mut CaptureMap,
    mutation: &mut FindingMutation,
) -> bool {
    for item in &pattern.body {
        match item {
            PatternBody::Capture(c) => {
                captures.insert(
                    Arc::from(c.name.as_str()),
                    CaptureValue::Node(StaticNodeId::new(node.id.clone())),
                );
            }
            PatternBody::Predicate(p) => {
                if !run_predicate(p, resolved, captures, mutation) {
                    return false;
                }
            }
            PatternBody::Sub(sub) => {
                if !match_sub_pattern(sub, node, resolved, captures, mutation) {
                    return false;
                }
            }
        }
    }
    true
}

fn match_sub_pattern(
    sub: &Pattern,
    parent_node: &Node,
    resolved: &ResolvedGraph,
    captures: &mut CaptureMap,
    mutation: &mut FindingMutation,
) -> bool {
    match sub.head.prefix {
        Prefix::Ast => match_ast_sub_pattern(sub, parent_node, resolved, captures, mutation),
        // Nested op/edge/overlay/fact patterns don't make semantic sense
        // beneath a node anchor. Treat as no-match rather than error so
        // future extensions stay non-breaking.
        _ => false,
    }
}

fn match_ast_sub_pattern(
    sub: &Pattern,
    parent_node: &Node,
    resolved: &ResolvedGraph,
    captures: &mut CaptureMap,
    mutation: &mut FindingMutation,
) -> bool {
    let Some(shape) = parent_node.ast.as_ref() else {
        return false;
    };
    let kind = sub.head.kind.as_str();
    match (kind, &shape.root) {
        ("Call", AstNode::Call(call)) => match_call_node(sub, call, resolved, captures, mutation),
        ("Decorator", AstNode::Decorator(_)) => match_props_only(sub, resolved, captures, mutation),
        ("Assignment", AstNode::Assignment(a)) => {
            if let AstNode::Call(c) = a.rhs.as_ref() {
                match_call_node(sub, c, resolved, captures, mutation)
            } else {
                false
            }
        }
        ("FunctionDef", AstNode::FunctionDef(_)) => {
            match_props_only(sub, resolved, captures, mutation)
        }
        ("SqlStatement", AstNode::SqlStatement(_)) => {
            match_props_only(sub, resolved, captures, mutation)
        }
        ("SqlExpression", AstNode::SqlExpression(_)) => {
            match_props_only(sub, resolved, captures, mutation)
        }
        _ => false,
    }
}

fn match_call_node(
    sub: &Pattern,
    call: &CallNode,
    resolved: &ResolvedGraph,
    captures: &mut CaptureMap,
    mutation: &mut FindingMutation,
) -> bool {
    // Props on an ast/Call: :method, :method-chain, :arg/N, :kwarg/<name>, :receiver
    for (key, value) in &sub.props {
        let key_str = key.as_str();
        let ok = if key_str == "method" {
            value_to_string(value).map(|s| call.method() == s).unwrap_or(false)
        } else if key_str == "method-chain" {
            match value {
                Value::List(items) => method_chain_matches(call, items),
                _ => false,
            }
        } else if let Some(idx) = key_str.strip_prefix("arg/").and_then(|s| s.parse::<usize>().ok()) {
            match value {
                Value::CaptureRef(name) => {
                    if let Some(arg) = call.args.get(idx) {
                        captures.insert(name.clone(), CaptureValue::AstArg(Box::new(arg.clone())));
                        true
                    } else {
                        false
                    }
                }
                _ => {
                    let want = value_to_string(value).unwrap_or_default();
                    call.args
                        .get(idx)
                        .map(|a| ast_arg_kind(a) == want)
                        .unwrap_or(false)
                }
            }
        } else if let Some(name) = key_str.strip_prefix("kwarg/") {
            match value {
                Value::CaptureRef(cap_name) => match call.kwarg(name) {
                    Some(arg) => {
                        captures.insert(
                            cap_name.clone(),
                            CaptureValue::AstArg(Box::new(arg.clone())),
                        );
                        true
                    }
                    None => false,
                },
                _ => {
                    let want = value_to_string(value).unwrap_or_default();
                    call.kwarg(name)
                        .map(|a| ast_arg_kind(a) == want)
                        .unwrap_or(false)
                }
            }
        } else if key_str == "receiver" {
            // :receiver matches the immediate parent of the leaf call in
            // the chain (e.g. `df` in `df.collect()`). Implemented as a
            // string prefix check on the method chain for now.
            value_to_string(value)
                .map(|s| call.method_chain.first().map(String::as_str) == Some(s.as_str()))
                .unwrap_or(false)
        } else {
            // Unknown prop on Call — non-fatal, ignored.
            true
        };
        if !ok {
            return false;
        }
    }

    // Body items on Call: captures bind the call to a name, predicates
    // get the call as `__current`.
    for item in &sub.body {
        match item {
            PatternBody::Capture(c) => {
                // For call captures we keep the parent node id (call AST
                // doesn't have its own node id). The captured value is a
                // boxed AstArg::Call so predicates can introspect the
                // structure.
                captures.insert(
                    Arc::from(c.name.as_str()),
                    CaptureValue::AstArg(Box::new(crate::resolved::ast_shape::AstArg::Call(
                        Box::new(call.clone()),
                    ))),
                );
            }
            PatternBody::Predicate(p) => {
                if !run_predicate(p, resolved, captures, mutation) {
                    return false;
                }
            }
            PatternBody::Sub(_) => {
                // Nested sub-patterns under Call don't yet recurse into
                // CallNode.args[i] structurally; predicates like #kind
                // handle that for now.
            }
        }
    }
    true
}

fn match_props_only(
    sub: &Pattern,
    resolved: &ResolvedGraph,
    captures: &mut CaptureMap,
    mutation: &mut FindingMutation,
) -> bool {
    // Decorator/FunctionDef/SqlStatement/SqlExpression have shape-specific
    // props that are easier to express via predicates today. Run the body
    // items uniformly so rules can attach captures and predicates.
    for item in &sub.body {
        match item {
            PatternBody::Capture(_) => {
                // No anchor to bind to without finer-grained AST refs;
                // safe no-op.
            }
            PatternBody::Predicate(p) => {
                if !run_predicate(p, resolved, captures, mutation) {
                    return false;
                }
            }
            PatternBody::Sub(_) => {}
        }
    }
    true
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

fn run_predicate(
    p: &Predicate,
    resolved: &ResolvedGraph,
    captures: &mut CaptureMap,
    mutation: &mut FindingMutation,
) -> bool {
    let ctx = MatchCtx::new(resolved, captures);
    match evaluate_predicate(p, &ctx) {
        PredResult::Bool(b) => b,
        PredResult::Skip => true,
        PredResult::Value(v) => {
            // Some predicates double as bindings via :as @cap. Look
            // backwards through args for a `:as` keyword followed by a
            // capture ref and bind the value there.
            for (i, arg) in p.args.iter().enumerate() {
                if let PredArg::Value(Value::Ident(s)) = arg {
                    if s.as_ref() == ":as" {
                        if let Some(PredArg::Value(Value::CaptureRef(name))) = p.args.get(i + 1) {
                            captures.insert(name.clone(), v.clone());
                            break;
                        }
                    }
                }
            }
            true
        }
        PredResult::SetFinding(m) => {
            mutation.merge(m);
            true
        }
    }
}

fn parse_op_kind(name: &str) -> Option<OperationKind> {
    match name {
        "Read" => Some(OperationKind::Read),
        "Transform" => Some(OperationKind::Transform),
        "Shuffle" => Some(OperationKind::Shuffle),
        "Action" => Some(OperationKind::Action),
        "Write" => Some(OperationKind::Write),
        "UdfCall" => Some(OperationKind::UdfCall),
        "Maintenance" => Some(OperationKind::Maintenance),
        "Unknown" => Some(OperationKind::Unknown),
        _ => None,
    }
}

fn namespace_name(ns: &crate::resolved::Namespace) -> &'static str {
    use crate::resolved::Namespace;
    match ns {
        Namespace::Spark => "Spark",
        Namespace::Dlt => "Dlt",
        Namespace::Dp => "Dp",
        Namespace::PandasOnSpark => "PandasOnSpark",
        Namespace::UserDefined(_) => "UserDefined",
        Namespace::Unknown => "Unknown",
    }
}

fn value_to_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.to_string()),
        Value::Ident(s) => Some(s.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn method_chain_matches(call: &CallNode, expected: &[Value]) -> bool {
    if expected.len() != call.method_chain.len() {
        return false;
    }
    for (i, want) in expected.iter().enumerate() {
        let want_s = match value_to_string(want) {
            Some(s) => s,
            None => return false,
        };
        if call.method_chain[i] != want_s {
            return false;
        }
    }
    true
}

fn edge_kind_matches(actual: &str, expected: &str) -> bool {
    // Edge kinds are stored as strings in `Edge.edge_type`. Allow either
    // direct equality or a snake_case → CamelCase fold (so `data_flow`
    // matches `DataFlow`).
    if actual == expected {
        return true;
    }
    let folded: String = expected
        .chars()
        .flat_map(|c| {
            if c.is_uppercase() {
                vec!['_', c.to_ascii_lowercase()]
            } else {
                vec![c]
            }
        })
        .collect();
    let folded = folded.trim_start_matches('_');
    actual == folded
}

/// Build a minimal `ResolvedGraph` directly from a `Graph` for tests.
/// Re-exported because two test modules need the same fixture path.
#[cfg(test)]
pub(super) fn build_resolved_for_test(graph: Graph) -> ResolvedGraph {
    crate::resolved::ResolvedGraphBuilder::new(graph).build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolved::ast_shape::{AstShape, CallNode, LitKind};
    use crate::resolved::scope_facts::{Namespace, ScopeFacts};
    use crate::rules::graph_dsl::parser::parse_pattern;
    use crate::types::{Edge, Node, ScalingBehavior};

    fn mk_node(id: &str, kind: OperationKind, line: u32) -> Node {
        Node {
            id: id.into(),
            kind,
            scaling_type: ScalingBehavior::Linear,
            photon_eligible: false,
            shuffle_required: false,
            driver_bound: false,
            tables_referenced: Vec::new(),
            estimated_input_bytes: None,
            estimated_cost_usd: None,
            line_number: Some(line),
            source_code: None,
            ast: None,
            scope: ScopeFacts::default(),
        }
    }

    fn mk_call_node(id: &str, method_chain: Vec<&str>, line: u32) -> Node {
        let mut n = mk_node(id, OperationKind::Action, line);
        n.ast = Some(AstShape::new(AstNode::Call(CallNode {
            method_chain: method_chain.into_iter().map(String::from).collect(),
            args: Vec::new(),
            kwargs: Vec::new(),
            line,
            column: 1,
        })));
        n
    }

    fn mk_graph(nodes: Vec<Node>) -> Graph {
        Graph {
            nodes,
            edges: Vec::new(),
            findings: Vec::new(),
            mode: "python".into(),
            confidence: "low".into(),
        }
    }

    #[test]
    fn op_pattern_filters_by_kind() {
        let resolved = build_resolved_for_test(mk_graph(vec![
            mk_node("a", OperationKind::Read, 1),
            mk_node("b", OperationKind::Write, 2),
            mk_node("c", OperationKind::Read, 3),
        ]));
        let pattern = parse_pattern("(op:Read)").unwrap();
        let matches = run_pattern(&pattern, None, &resolved);
        let anchors: Vec<String> = matches
            .iter()
            .map(|m| m.anchor.as_str().to_string())
            .collect();
        assert_eq!(anchors.len(), 2);
        assert!(anchors.contains(&"a".to_string()));
        assert!(anchors.contains(&"c".to_string()));
    }

    #[test]
    fn ast_sub_pattern_matches_method() {
        let resolved = build_resolved_for_test(mk_graph(vec![
            mk_call_node("a", vec!["df", "collect"], 1),
            mk_call_node("b", vec!["df", "take"], 2),
        ]));
        let pattern =
            parse_pattern(r#"(op:Action (ast/Call :method "collect"))"#).unwrap();
        let matches = run_pattern(&pattern, None, &resolved);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].anchor.as_str(), "a");
    }

    #[test]
    fn capture_binds_to_node() {
        let resolved = build_resolved_for_test(mk_graph(vec![mk_call_node(
            "a",
            vec!["df", "collect"],
            5,
        )]));
        let pattern = parse_pattern(r#"(op:Action @call)"#).unwrap();
        let matches = run_pattern(&pattern, None, &resolved);
        assert_eq!(matches.len(), 1);
        let captured = matches[0].captures.get("call").unwrap();
        assert!(matches!(captured, CaptureValue::Node(_)));
    }

    #[test]
    fn predicate_eq_prunes_non_matches() {
        let resolved = build_resolved_for_test(mk_graph(vec![
            mk_call_node("a", vec!["df", "collect"], 1),
            mk_call_node("b", vec!["df", "take"], 2),
        ]));
        let pattern = parse_pattern(
            r#"(op:Action
                 (ast/Call :method "collect")
                 (#eq? @__current @__current))"#,
        )
        .unwrap();
        let matches = run_pattern(&pattern, None, &resolved);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].anchor.as_str(), "a");
    }

    #[test]
    fn exclude_pattern_removes_anchor() {
        let resolved = build_resolved_for_test(mk_graph(vec![
            mk_call_node("a", vec!["df", "collect"], 1),
            mk_call_node("b", vec!["df", "collect"], 2),
        ]));
        let detect = parse_pattern(r#"(op:Action (ast/Call :method "collect"))"#).unwrap();
        let exclude = parse_pattern(r#"(op:Action :line 1)"#).unwrap();
        let matches = run_pattern(&detect, Some(&exclude), &resolved);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].anchor.as_str(), "b");
    }

    #[test]
    fn method_chain_list_prop_matches() {
        let resolved = build_resolved_for_test(mk_graph(vec![
            mk_call_node("a", vec!["spark", "sql"], 1),
            mk_call_node("b", vec!["spark", "read", "parquet"], 2),
        ]));
        let pattern =
            parse_pattern(r#"(op:Action (ast/Call :method-chain ["spark" "sql"]))"#).unwrap();
        let matches = run_pattern(&pattern, None, &resolved);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].anchor.as_str(), "a");
    }

    #[test]
    fn arg_capture_binds_ast_arg() {
        let mut node = mk_call_node("a", vec!["spark", "sql"], 1);
        if let Some(shape) = node.ast.as_mut() {
            if let AstNode::Call(c) = &mut shape.root {
                c.args.push(crate::resolved::ast_shape::AstArg::Literal(
                    LitKind::String("SELECT 1".into()),
                ));
            }
        }
        let resolved = build_resolved_for_test(mk_graph(vec![node]));
        let pattern = parse_pattern(r#"(op:Action (ast/Call :method "sql" :arg/0 @arg))"#).unwrap();
        let matches = run_pattern(&pattern, None, &resolved);
        assert_eq!(matches.len(), 1);
        assert!(matches!(
            matches[0].captures.get("arg"),
            Some(CaptureValue::AstArg(_))
        ));
    }

    #[test]
    fn fact_pattern_binds_graph_level_fact() {
        let resolved = build_resolved_for_test(mk_graph(vec![
            mk_node("a", OperationKind::Read, 1),
            mk_node("b", OperationKind::Write, 2),
        ]));
        let pattern = parse_pattern("(fact:NodeCount)").unwrap();
        let matches = run_pattern(&pattern, None, &resolved);
        assert_eq!(matches.len(), 1);
        let fact = matches[0].captures.get("__fact").unwrap();
        match fact {
            CaptureValue::Number(n) => assert_eq!(*n, 2.0),
            other => panic!("expected number, got {other:?}"),
        }
    }

    #[test]
    fn scope_namespace_prop_filters() {
        let mut n = mk_call_node("a", vec!["spark", "read", "parquet"], 1);
        n.scope.namespace = Some(Namespace::Spark);
        let mut other = mk_call_node("b", vec!["user", "fn"], 2);
        other.scope.namespace = Some(Namespace::UserDefined("user".into()));
        let resolved = build_resolved_for_test(mk_graph(vec![n, other]));

        let pattern = parse_pattern(r#"(op:Action :scope/namespace "Spark")"#).unwrap();
        let matches = run_pattern(&pattern, None, &resolved);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].anchor.as_str(), "a");
    }

    #[test]
    fn when_predicate_attaches_mutation() {
        let resolved = build_resolved_for_test(mk_graph(vec![mk_call_node(
            "a",
            vec!["df", "collect"],
            5,
        )]));
        let pattern = parse_pattern(
            r#"(op:Action
                 (ast/Call :method "collect")
                 (#when (#eq? "x" "x") :confidence "High"))"#,
        )
        .unwrap();
        let matches = run_pattern(&pattern, None, &resolved);
        assert_eq!(matches.len(), 1);
        assert!(matches!(
            matches[0].mutation.confidence,
            Some(crate::types::Confidence::High)
        ));
    }

    #[test]
    fn not_receiver_of_filters_call_chains() {
        // BP008 shape: collect() preceded by limit() is *not* flagged.
        // collect() alone is flagged.
        let resolved = build_resolved_for_test(mk_graph(vec![
            mk_call_node("safe", vec!["df", "limit", "collect"], 1),
            mk_call_node("risky", vec!["df", "collect"], 2),
        ]));
        let pattern = parse_pattern(
            r#"(op:Action
                 (ast/Call :method "collect" @call)
                 (#not-receiver-of @call "limit"))"#,
        )
        .unwrap();
        let matches = run_pattern(&pattern, None, &resolved);
        let anchors: Vec<String> = matches
            .iter()
            .map(|m| m.anchor.as_str().to_string())
            .collect();
        assert_eq!(anchors.len(), 1);
        assert!(anchors.contains(&"risky".to_string()));
    }

    #[test]
    fn kwargs_missing_detects_absent_options() {
        // BP021 shape: spark.read.jdbc(...) without partitionColumn/lowerBound/...
        let mut with_options = mk_call_node("ok", vec!["spark", "read", "jdbc"], 1);
        if let Some(shape) = with_options.ast.as_mut() {
            if let AstNode::Call(c) = &mut shape.root {
                c.kwargs.push((
                    "partitionColumn".into(),
                    crate::resolved::ast_shape::AstArg::Identifier("id".into()),
                ));
            }
        }
        let missing_options = mk_call_node("flagged", vec!["spark", "read", "jdbc"], 2);

        let resolved = build_resolved_for_test(mk_graph(vec![with_options, missing_options]));
        let pattern = parse_pattern(
            r#"(op:Action
                 (ast/Call :method "jdbc" @call)
                 (#kwargs/missing @call ["partitionColumn"]))"#,
        )
        .unwrap();
        let matches = run_pattern(&pattern, None, &resolved);
        let anchors: Vec<String> = matches
            .iter()
            .map(|m| m.anchor.as_str().to_string())
            .collect();
        assert_eq!(anchors.len(), 1);
        assert!(anchors.contains(&"flagged".to_string()));
    }

    #[test]
    fn exists_with_nested_pattern() {
        let resolved = build_resolved_for_test(mk_graph(vec![
            mk_call_node("a", vec!["df", "collect"], 1),
            mk_call_node("b", vec!["df", "write"], 2),
        ]));
        // Use #exists to assert a Write op exists in the graph.
        let pattern = parse_pattern(
            r#"(op:Action
                 (ast/Call :method "collect")
                 (#exists (op:Action (ast/Call :method "write"))))"#,
        )
        .unwrap();
        let matches = run_pattern(&pattern, None, &resolved);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].anchor.as_str(), "a");
    }

    #[test]
    fn edge_pattern_binds_endpoints() {
        let mut graph = mk_graph(vec![
            mk_node("a", OperationKind::Read, 1),
            mk_node("b", OperationKind::Write, 2),
        ]);
        graph.edges.push(Edge {
            source: "a".into(),
            target: "b".into(),
            edge_type: "data_flow".into(),
        });
        let resolved = build_resolved_for_test(graph);
        let pattern = parse_pattern(r#"(edge:DataFlow :from @src :to @dst)"#).unwrap();
        let matches = run_pattern(&pattern, None, &resolved);
        assert_eq!(matches.len(), 1);
        assert!(matches!(
            matches[0].captures.get("src"),
            Some(CaptureValue::Node(_))
        ));
        assert!(matches!(
            matches[0].captures.get("dst"),
            Some(CaptureValue::Node(_))
        ));
    }
}
