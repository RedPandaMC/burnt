//! Capture map + capture values: what a `@name` binding can hold.
//!
//! The matcher accumulates captures as it walks a pattern. Each capture
//! can bind to a structural target (a graph `Node`, an `AstNode`, an
//! `Edge`) or to a value extracted by a predicate (`(method-of @call)`
//! binds a string). One representation handles both.

use std::collections::HashMap;
use std::sync::Arc;

use crate::resolved::ast_shape::{AstArg, AstNode};
use crate::resolved::ids::StaticNodeId;

/// A bound value behind a `@name` capture. The matcher stores these in
/// the per-match `CaptureMap`; predicates pull them out by capture name.
#[derive(Debug, Clone)]
pub enum CaptureValue {
    /// Captured operation `Node` by id. Resolved through the
    /// `ResolvedGraph` when a predicate needs the actual struct.
    Node(StaticNodeId),
    /// Captured AST subtree position. Carries the path from the owning
    /// node's `AstShape.root` so the predicate can re-traverse without
    /// keeping a borrow.
    AstNode(AstNodeRef),
    /// Captured argument of an `AstNode::Call` / `Decorator` — accessed
    /// via `(value-of @cap)` and friends.
    AstArg(Box<AstArg>),
    /// Captured edge endpoints by node id.
    Edge {
        from: StaticNodeId,
        to: StaticNodeId,
        kind: Arc<str>,
    },
    /// Extracted string value — produced by `(method-of @call)`,
    /// `(fqn-of @tref)`, etc.
    String(Arc<str>),
    /// Numeric value from `(observed-bytes-of @x)`, `(line-of @cap)`,
    /// etc. Held as f64 so size-equal-number comparisons work without
    /// a separate Int variant.
    Number(f64),
    Bool(bool),
    /// `Vec<CaptureValue>` for quantifier subjects (`(descendants @x)`
    /// yields a list bound under a single name).
    List(Vec<CaptureValue>),
    /// Explicit absence — distinguishes "captured nothing" from "never
    /// bound" (which is a missing entry in the map).
    Nil,
}

impl CaptureValue {
    /// Try to interpret as a node id. Useful when a predicate wants to
    /// resolve back to a `Node` via the resolved graph.
    #[must_use]
    pub fn as_node_id(&self) -> Option<&StaticNodeId> {
        match self {
            Self::Node(id) => Some(id),
            _ => None,
        }
    }

    /// Try to interpret as a string. Covers both extracted strings and
    /// AstArg literals — the matcher uses this for `#eq?` / `#match?`
    /// against captured values.
    #[must_use]
    pub fn as_str_value(&self) -> Option<String> {
        match self {
            Self::String(s) => Some(s.to_string()),
            Self::AstArg(a) => ast_arg_as_string(a),
            Self::Bool(b) => Some(b.to_string()),
            Self::Number(n) => Some(n.to_string()),
            _ => None,
        }
    }

    /// Try to interpret as a number. Used by numeric predicates.
    #[must_use]
    pub fn as_number(&self) -> Option<f64> {
        match self {
            Self::Number(n) => Some(*n),
            Self::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    /// Iterate over a list capture; single-value captures are treated
    /// as a one-element list for uniformity in quantifiers.
    pub fn iter_items(&self) -> Box<dyn Iterator<Item = &CaptureValue> + '_> {
        match self {
            Self::List(items) => Box::new(items.iter()),
            other => Box::new(std::iter::once(other)),
        }
    }
}

/// Reference into an `AstShape` — the static node id that owns the
/// `AstShape`, plus the path from `AstShape.root` to the target
/// `AstNode`. Path indices are 0-based.
#[derive(Debug, Clone)]
pub struct AstNodeRef {
    pub owner: StaticNodeId,
    pub path: Vec<AstPathStep>,
}

/// One step down an `AstShape` tree. The matcher pushes a step every
/// time it descends into a sub-pattern; predicates use it to re-locate
/// the target without traversing the whole tree.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum AstPathStep {
    /// Pass through `AstShape.root`.
    Root,
    /// `AstNode::Assignment.rhs` — the only owned sub-node.
    AssignmentRhs,
    /// `AstNode::FunctionDef.body[i]`.
    FunctionDefBody(usize),
    /// `AstNode::FunctionDef.decorators[i]` (cast to AstNode::Decorator).
    FunctionDefDecorator(usize),
}

/// Capture map keyed by capture name. Cheap to clone — `Arc<str>` keys
/// + `Clone` values — so the matcher can branch and try alternatives.
pub type CaptureMap = HashMap<Arc<str>, CaptureValue>;

fn ast_arg_as_string(arg: &AstArg) -> Option<String> {
    use crate::resolved::ast_shape::LitKind;
    match arg {
        AstArg::Literal(LitKind::String(s)) => Some(s.clone()),
        AstArg::Literal(LitKind::Int(n)) => Some(n.to_string()),
        AstArg::Literal(LitKind::Float(n)) => Some(n.to_string()),
        AstArg::Literal(LitKind::Bool(b)) => Some(b.to_string()),
        AstArg::Literal(LitKind::None) => Some("None".into()),
        AstArg::Identifier(s) => Some(s.clone()),
        AstArg::Attribute(parts) => Some(parts.join(".")),
        _ => None,
    }
}

/// Identify the discriminant of an [`AstNode`] as a stable string. Used
/// by `#kind` predicates to test variant equality without exposing the
/// enum across the FFI boundary.
#[must_use]
pub fn ast_node_kind(node: &AstNode) -> &'static str {
    match node {
        AstNode::Call(_) => "Call",
        AstNode::Decorator(_) => "Decorator",
        AstNode::Assignment(_) => "Assignment",
        AstNode::FunctionDef(_) => "FunctionDef",
        AstNode::SqlStatement(_) => "SqlStatement",
        AstNode::SqlExpression(_) => "SqlExpression",
    }
}

/// Same idea, for [`AstArg`] variants — `#kind @arg "FString"` style.
#[must_use]
pub fn ast_arg_kind(arg: &AstArg) -> &'static str {
    match arg {
        AstArg::Literal(_) => "Literal",
        AstArg::Identifier(_) => "Identifier",
        AstArg::Attribute(_) => "Attribute",
        AstArg::Call(_) => "Call",
        AstArg::FString { .. } => "FString",
        AstArg::PercentFormat { .. } => "PercentFormat",
        AstArg::DotFormat { .. } => "DotFormat",
        AstArg::BinaryOp { .. } => "BinaryOp",
        AstArg::Comprehension { .. } => "Comprehension",
        AstArg::Unknown { .. } => "Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolved::ast_shape::LitKind;

    #[test]
    fn capture_value_string_round_trip() {
        let v = CaptureValue::String(Arc::from("hello"));
        assert_eq!(v.as_str_value(), Some("hello".to_string()));
    }

    #[test]
    fn capture_value_number_from_bool() {
        assert_eq!(CaptureValue::Bool(true).as_number(), Some(1.0));
        assert_eq!(CaptureValue::Bool(false).as_number(), Some(0.0));
    }

    #[test]
    fn capture_value_iter_single_item_uniform() {
        let one = CaptureValue::String(Arc::from("x"));
        let many = CaptureValue::List(vec![
            CaptureValue::String(Arc::from("a")),
            CaptureValue::String(Arc::from("b")),
        ]);
        assert_eq!(one.iter_items().count(), 1);
        assert_eq!(many.iter_items().count(), 2);
    }

    #[test]
    fn ast_arg_as_string_handles_literals_and_idents() {
        let arg = AstArg::Literal(LitKind::String("abc".into()));
        assert_eq!(ast_arg_as_string(&arg), Some("abc".into()));
        let arg = AstArg::Identifier("df".into());
        assert_eq!(ast_arg_as_string(&arg), Some("df".into()));
        let arg = AstArg::Attribute(vec!["spark".into(), "read".into()]);
        assert_eq!(ast_arg_as_string(&arg), Some("spark.read".into()));
    }

    #[test]
    fn ast_arg_kind_for_each_variant() {
        assert_eq!(ast_arg_kind(&AstArg::Literal(LitKind::None)), "Literal");
        assert_eq!(ast_arg_kind(&AstArg::Identifier("x".into())), "Identifier");
        assert_eq!(
            ast_arg_kind(&AstArg::FString { parts: Vec::new() }),
            "FString"
        );
    }
}
