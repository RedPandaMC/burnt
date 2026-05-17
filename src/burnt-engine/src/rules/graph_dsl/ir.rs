//! Parsed pattern intermediate representation.
//!
//! The DSL parser turns S-expression source into a [`Pattern`] tree. Every
//! variant retains its source position so build-time errors and runtime
//! `Finding`s can attribute back to the rule's TOML line/column.
//!
//! The IR is intentionally narrow: predicates, values, and nested patterns
//! are the only composites. Everything else (head prefix, capture binding,
//! property assertion) flattens to a small enum.

use std::sync::Arc;

/// One parsed pattern — the root of the IR for either the rule's `detect`
/// expression, its optional `exclude` expression, or a nested sub-pattern
/// inside another `Pattern`.
#[derive(Debug, Clone)]
pub struct Pattern {
    /// What the pattern matches against (operation node, AST subtree,
    /// edge, overlay, fact).
    pub head: Head,
    /// Property assertions on the head (e.g. `:method "collect"`).
    /// Order preserved for diagnostic stability.
    pub props: Vec<(String, Value)>,
    /// Inner contents: nested patterns, predicates, and captures.
    pub body: Vec<PatternBody>,
    /// 1-indexed position of the opening `(` in the pattern source.
    pub line: u32,
    pub column: u32,
}

/// Anything that can appear inside a pattern's body, in source order.
///
/// Source order matters for diagnostic stability and for one predicate
/// (`#when`) that may want to reference a capture defined earlier in
/// the body.
#[derive(Debug, Clone)]
pub enum PatternBody {
    /// Nested structural match.
    Sub(Pattern),
    /// `@name` — bind the current node to `name`.
    Capture(Capture),
    /// `(#foo ...)` — boolean / value-extracting / finding-mutating
    /// predicate.
    Predicate(Predicate),
}

/// `head` of a pattern. The five recognised prefixes split the operand
/// alphabet: operation nodes, AST subtrees, edges, overlay payloads,
/// and graph-level facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Head {
    pub prefix: Prefix,
    /// Kind identifier after the prefix (e.g. `"Read"` for `op:Read`,
    /// `"Call"` for `ast/Call`). Kept as a String so the IR doesn't have
    /// to enumerate every Node/AstNode variant — the matcher validates
    /// the kind against the actual type when it runs.
    pub kind: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Prefix {
    /// `op:<Kind>` — selects a `Node` by `OperationKind` (Read, Write,
    /// Shuffle, Action, Transform, Maintenance, UdfCall, Unknown).
    Op,
    /// `ast/<Kind>` — walks the current node's `AstShape` matching the
    /// `AstNode` variant (Call, Decorator, Assignment, FunctionDef,
    /// SqlStatement, SqlExpression).
    Ast,
    /// `edge:<Kind>` — matches an `Edge` by `edge_type`. Binds endpoints
    /// via `:from @a :to @b` props.
    Edge,
    /// `overlay:<Kind>` — matches a present overlay (Stage, PlanSubtree,
    /// TableSpec). `:where (this)` constrains to the current node.
    Overlay,
    /// `fact:<Name>` — graph-level fact match (Mode, Confidence,
    /// NodeCount, …). One-shot at the top of the pattern.
    Fact,
}

impl Prefix {
    /// Parse the lowercase prefix slice from a head token like `"op"` /
    /// `"ast"` / `"edge"` / `"overlay"` / `"fact"`.
    #[must_use]
    pub fn from_token(token: &str) -> Option<Self> {
        match token {
            "op" => Some(Self::Op),
            "ast" => Some(Self::Ast),
            "edge" => Some(Self::Edge),
            "overlay" => Some(Self::Overlay),
            "fact" => Some(Self::Fact),
            _ => None,
        }
    }

    /// The separator the prefix uses in head tokens. `op:Read` /
    /// `edge:DataFlow` use `:`; `ast/Call` uses `/`. Distinguishing the
    /// two visually keeps `:Foo` props from looking like heads.
    #[must_use]
    pub fn separator(self) -> char {
        match self {
            Self::Ast => '/',
            _ => ':',
        }
    }
}

/// `@name` — binds the current node to `name`. The binding is visible
/// later in the pattern body (siblings + predicates) and to the
/// `[graph.finding]` template.
#[derive(Debug, Clone)]
pub struct Capture {
    pub name: String,
    pub line: u32,
    pub column: u32,
}

/// `(#predicate args…)` — applied after structural match succeeds.
///
/// Predicates compose: `#and`, `#or`, `#not`, `#when` take inner
/// predicates as args. Value-extracting predicates (`(method-of @x)`)
/// return a `PredValue` instead of a boolean — the registry decides at
/// evaluation time.
#[derive(Debug, Clone)]
pub struct Predicate {
    /// Bare predicate name without the leading `#`. The matcher's
    /// registry stores them this way.
    pub name: String,
    pub args: Vec<PredArg>,
    pub line: u32,
    pub column: u32,
}

/// One argument to a predicate.
#[derive(Debug, Clone)]
pub enum PredArg {
    Value(Value),
    /// Nested predicate (composition: `#or`, `#and`, etc.).
    Predicate(Predicate),
    /// Nested pattern used as a subject of a quantifier
    /// (`(#count (ast/Call :method "withColumn") :as @n)`).
    Pattern(Pattern),
}

/// Literal or capture-reference value used inside props and predicates.
///
/// `Arc<str>` for strings keeps the IR cheap to clone — every pattern
/// is built once at compile time and run many times against many nodes,
/// so cloning the IR for each match shouldn't allocate per string.
#[derive(Debug, Clone)]
pub enum Value {
    String(Arc<str>),
    Number(f64),
    Bool(bool),
    Ident(Arc<str>),
    /// `@name` reference — resolved against the capture map at match time.
    CaptureRef(Arc<str>),
    /// Byte-size literal (`1Gi`, `1MiB`, `500MB`). Parsed to bytes at
    /// IR-construction time so the matcher does pure integer compares.
    Size(u64),
    /// Duration literal in milliseconds. Parsed eagerly for the same
    /// reason as `Size`.
    DurationMs(u64),
    /// Literal list — used by `:kwargs/missing [...]` shape and similar
    /// set-membership tests.
    List(Vec<Value>),
}
