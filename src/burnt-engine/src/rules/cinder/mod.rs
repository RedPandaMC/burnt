//! Cinder Pattern Language (CPL) Compiler
//!
//! Transforms human-readable CPL patterns into tree-sitter S-expressions.
//! CPL allows users to write code-like snippets with $METAVARIABLES instead of
//! writing raw tree-sitter LISP syntax.

mod compiler;
mod parser;

pub use compiler::CinderCompiler;
pub use parser::CinderPattern;
