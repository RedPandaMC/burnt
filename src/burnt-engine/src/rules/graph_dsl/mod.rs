//! Graph-query DSL: S-expression patterns over `ResolvedGraph`.
//!
//! After the rule-system rework this is the only path by which a rule
//! describes what it matches. The DSL keeps tree-sitter's S-expression
//! *syntax* (parens, field labels `:foo`, captures `@name`, predicates
//! `#foo`) but the operand alphabet is the resolved graph
//! (`ResolvedGraph` + `AstShape` + `ScopeFacts` + overlays + facts),
//! not raw tree-sitter nodes. The matcher is hand-rolled — `tree_sitter::Query`
//! is not used by the rule layer.
//!
//! # Module layout
//!
//! ```text
//! graph_dsl/
//!   mod.rs       // re-exports the public surface
//!   error.rs     // ParseError + ParseErrorKind with source positions
//!   lexer.rs     // tokenizer
//!   ir.rs        // parsed Pattern IR (Pattern, Head, Predicate, Capture, Value)
//!   parser.rs    // recursive-descent parser
//! ```
//!
//! Commits 6–9 add the predicate engine, matcher, finding emission, and
//! TOML/pipeline wiring on top of the IR defined here.

pub mod context;
pub mod error;
mod ir;
mod lexer;
pub mod parser;
pub mod predicate;
pub mod value;

pub use context::{FindingMutation, MatchCtx};
pub use error::{ParseError, ParseErrorKind};
pub use ir::{Capture, Head, Pattern, PatternBody, PredArg, Predicate, Prefix, Value};
pub use parser::parse_pattern;
pub use predicate::{evaluate_predicate, lookup, registered_names, registry_size, PredResult, PredicateFn};
pub use value::{ast_arg_kind, ast_node_kind, AstNodeRef, AstPathStep, CaptureMap, CaptureValue};
