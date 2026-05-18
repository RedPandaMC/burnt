//! Symbolic AST of a single operation node, captured by the graph builders
//! at parse time.
//!
//! Per the architectural directive that "AST ≡ ResolvedGraph", the
//! tree-sitter `Tree` consumed by the graph builders is discarded after
//! construction. `AstShape` is the only AST surface rules will ever see.
//!
//! The variant set is intentionally tight — it covers every shape today's
//! rules touch (method-name match, identifier match, f-string / `+` /
//! `%` / `.format()` detection for BN002, decorator inspection for DLT
//! rules, SQL subquery / predicate for the BQ-family). All public structs
//! and enums are `#[non_exhaustive]` so adding variants in a later commit
//! is non-breaking.

use serde::{Deserialize, Serialize};

/// Top-level AST payload attached to a `Node`. Owned, cloneable, serialisable.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AstShape {
    pub root: AstNode,
}

impl AstShape {
    #[must_use]
    pub fn new(root: AstNode) -> Self {
        Self { root }
    }
}

/// One node in the symbolic AST.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AstNode {
    /// Top-level operation call (e.g. `df.write.parquet("...")`).
    Call(CallNode),

    /// Decorator (`@dlt.table(name="foo")`).
    Decorator(DecoratorNode),

    /// Assignment binding (`x = spark.read.parquet(...)`).
    Assignment(AssignmentNode),

    /// Function definition body root (DLT/SDP table functions).
    FunctionDef(FunctionDefNode),

    /// SQL statement root (for SQL-builder nodes). Mirrors `sqlparser`'s
    /// canonical shape — see [`SqlExpr`] for the predicate side.
    SqlStatement(SqlStatementNode),

    /// A single SQL expression — currently only what BQ-family rules need:
    /// binary ops, subqueries, IN/NOT IN, comparisons.
    SqlExpression(SqlExpr),
}

/// `df.write.parquet("...")` and friends. The dotted method chain is the
/// primary match surface; positional and keyword arguments are captured
/// structurally so rules can ask "does kwarg X exist" or "is arg 0 an
/// f-string" without re-walking source.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CallNode {
    /// e.g. `["df", "write", "parquet"]`. Leftmost element is the root
    /// receiver; rightmost is the called method.
    pub method_chain: Vec<String>,
    /// Positional arguments in source order.
    pub args: Vec<AstArg>,
    /// Keyword arguments in source order. Order is preserved for rules
    /// that care; equality checks should hit `find` rather than indexing.
    pub kwargs: Vec<(String, AstArg)>,
    pub line: u32,
    pub column: u32,
}

impl CallNode {
    /// The leaf method name (`parquet` from `df.write.parquet`).
    /// Returns the empty string for unparseable chains.
    #[must_use]
    pub fn method(&self) -> &str {
        self.method_chain.last().map(String::as_str).unwrap_or("")
    }

    /// True if the chain starts with the given receiver token
    /// (e.g. `starts_with("spark")` matches `spark.read.parquet`).
    #[must_use]
    pub fn starts_with(&self, head: &str) -> bool {
        self.method_chain.first().map(String::as_str) == Some(head)
    }

    /// Look up a kwarg by name.
    #[must_use]
    pub fn kwarg(&self, name: &str) -> Option<&AstArg> {
        self.kwargs
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DecoratorNode {
    /// e.g. `["dlt", "table"]` for `@dlt.table(name="foo")`.
    pub path: Vec<String>,
    pub args: Vec<AstArg>,
    pub kwargs: Vec<(String, AstArg)>,
    pub line: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AssignmentNode {
    pub target: String,
    pub rhs: Box<AstNode>,
    pub line: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct FunctionDefNode {
    pub name: String,
    pub decorators: Vec<DecoratorNode>,
    /// Each body item is one statement — typically a `Call` or `Assignment`.
    pub body: Vec<AstNode>,
    pub line: u32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum SqlStatementKind {
    Select,
    CreateTable,
    CreateView,
    Insert,
    Merge,
    Explain,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SqlStatementNode {
    pub kind: SqlStatementKind,
    /// Tables read from (FROM + JOIN sources). FQNs only — the structured
    /// `TableRef`s live on `Node.tables_referenced`.
    pub from: Vec<String>,
    /// Target table for `CreateTable`, `Insert`, `Merge` statements.
    pub target: Option<String>,
    /// WHERE / HAVING / ON predicates extracted at parse time. Rules
    /// inspect these via `ast/SqlExpr` patterns.
    pub predicates: Vec<SqlExpr>,
    pub line: u32,
}

/// Subset of `sqlparser::ast::Expr` we capture for rule matching.
///
/// Stays narrow on purpose — only shapes that current rules need (and
/// that BN002 / BQ001 / BQ004 will reach for) are first-class. Anything
/// else falls through to [`SqlExpr::Other`] with a rendered fallback.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum SqlExpr {
    /// `<lhs> IN (<subquery>)` or `<lhs> IN (<list>)`.
    InList {
        lhs: String,
        items: Vec<String>,
        negated: bool,
    },
    /// `<lhs> NOT IN (SELECT …)` is the canonical BQ001 shape. Carries
    /// the subquery text so the rule can pattern-match it.
    InSubquery {
        lhs: String,
        subquery: String,
        negated: bool,
    },
    /// Comparison: `<lhs> <op> <rhs>` (`=`, `<>`, `<`, `<=`, `>`, `>=`).
    Comparison {
        lhs: String,
        op: String,
        rhs: String,
    },
    /// Boolean combinator: `<lhs> <op> <rhs>` (`AND`, `OR`).
    Logical {
        op: String,
        lhs: Box<SqlExpr>,
        rhs: Box<SqlExpr>,
    },
    /// `NOT <inner>`.
    Not(Box<SqlExpr>),
    /// Rendered fallback for shapes we don't yet model. Carries the SQL
    /// text so rules can fall back to a `#match?` regex if needed.
    Other(String),
}

/// Argument shape inside a `CallNode` or `DecoratorNode`.
///
/// The point of breaking these out is so BN002-style rules can match
/// `(ast/Call :method-chain ["spark" "sql"] :arg/0 (#kind FString))`
/// without parsing the call site themselves. Each variant captures the
/// minimum data the matcher needs to evaluate predicates against.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AstArg {
    Literal(LitKind),
    /// Bare identifier (`x` in `f(x)`).
    Identifier(String),
    /// Dotted attribute (`obj.field.sub`).
    Attribute(Vec<String>),
    /// Nested call (`f(g(x))`).
    Call(Box<CallNode>),
    /// f-string with literal segments and interpolation placeholders.
    FString {
        parts: Vec<FStringPart>,
    },
    /// `"...%s..." % (x,)` — Python percent formatting.
    PercentFormat {
        template: String,
        args: Vec<AstArg>,
    },
    /// `"...".format(x, key=y)` — Python str.format.
    DotFormat {
        template: String,
        args: Vec<AstArg>,
        kwargs: Vec<(String, AstArg)>,
    },
    /// Binary operator: `x + y`, `x | y`, `x and y`.
    BinaryOp {
        op: String,
        lhs: Box<AstArg>,
        rhs: Box<AstArg>,
    },
    /// Comprehension expression. `kind` distinguishes list / set / dict /
    /// generator; the captured target+iter are the most-common shape rules
    /// inspect (e.g. detecting `for row in df.collect()`).
    Comprehension {
        kind: ComprehensionKind,
        target: String,
        iter: Box<AstArg>,
    },
    /// AST shape the builder couldn't classify — explicit, never silent.
    /// `repr` carries the raw source text so rules can fall back to
    /// regex matching.
    Unknown {
        repr: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum LitKind {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum FStringPart {
    /// Literal text between `{}` placeholders.
    Text(String),
    /// `{expr}` placeholder. The full expression text is preserved so the
    /// matcher can inspect dynamic-SQL shapes (BN002 territory).
    Interpolation { expr: String },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum ComprehensionKind {
    List,
    Set,
    Dict,
    Generator,
}

// ---------------------------------------------------------------------------
// Small constructors used by the graph builders.
// ---------------------------------------------------------------------------

impl AstNode {
    /// Wrap a `CallNode` as a top-level `AstNode::Call`.
    #[must_use]
    pub fn call(call: CallNode) -> Self {
        Self::Call(call)
    }

    /// Wrap an `AssignmentNode` as a top-level `AstNode::Assignment`.
    #[must_use]
    pub fn assignment(node: AssignmentNode) -> Self {
        Self::Assignment(node)
    }
}

impl CallNode {
    /// Convenience for the common `method_chain == [a]` case used by
    /// degenerate calls (`foo()` rather than `obj.foo()`).
    #[must_use]
    pub fn bare(name: impl Into<String>, line: u32, column: u32) -> Self {
        Self {
            method_chain: vec![name.into()],
            args: Vec::new(),
            kwargs: Vec::new(),
            line,
            column,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_method_returns_last_chain_element() {
        let c = CallNode {
            method_chain: vec!["df".into(), "write".into(), "parquet".into()],
            args: Vec::new(),
            kwargs: Vec::new(),
            line: 1,
            column: 0,
        };
        assert_eq!(c.method(), "parquet");
        assert!(c.starts_with("df"));
        assert!(!c.starts_with("spark"));
    }

    #[test]
    fn call_method_empty_for_empty_chain() {
        let c = CallNode {
            method_chain: Vec::new(),
            args: Vec::new(),
            kwargs: Vec::new(),
            line: 1,
            column: 0,
        };
        assert_eq!(c.method(), "");
        assert!(!c.starts_with("df"));
    }

    #[test]
    fn call_kwarg_lookup_finds_named_arg() {
        let c = CallNode {
            method_chain: vec!["foo".into()],
            args: Vec::new(),
            kwargs: vec![
                ("name".into(), AstArg::Literal(LitKind::String("x".into()))),
                ("limit".into(), AstArg::Literal(LitKind::Int(10))),
            ],
            line: 1,
            column: 0,
        };
        assert!(matches!(
            c.kwarg("name"),
            Some(AstArg::Literal(LitKind::String(s))) if s == "x"
        ));
        assert!(matches!(
            c.kwarg("limit"),
            Some(AstArg::Literal(LitKind::Int(10)))
        ));
        assert!(c.kwarg("missing").is_none());
    }

    #[test]
    fn fstring_part_round_trip_serde() {
        let parts = vec![
            FStringPart::Text("SELECT FROM ".into()),
            FStringPart::Interpolation {
                expr: "t".into(),
            },
        ];
        let serialised = serde_json::to_string(&parts).unwrap();
        let parsed: Vec<FStringPart> = serde_json::from_str(&serialised).unwrap();
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn ast_shape_wraps_root_node() {
        let call = CallNode {
            method_chain: vec!["spark".into(), "sql".into()],
            args: vec![AstArg::Literal(LitKind::String("SELECT 1".into()))],
            kwargs: Vec::new(),
            line: 4,
            column: 0,
        };
        let shape = AstShape::new(AstNode::call(call));
        assert!(matches!(shape.root, AstNode::Call(_)));
    }

    #[test]
    fn sql_expr_in_subquery_keeps_negated_flag() {
        let e = SqlExpr::InSubquery {
            lhs: "id".into(),
            subquery: "SELECT id FROM banned".into(),
            negated: true,
        };
        if let SqlExpr::InSubquery {
            lhs,
            subquery,
            negated,
        } = e
        {
            assert_eq!(lhs, "id");
            assert_eq!(subquery, "SELECT id FROM banned");
            assert!(negated);
        } else {
            panic!("expected InSubquery");
        }
    }
}
