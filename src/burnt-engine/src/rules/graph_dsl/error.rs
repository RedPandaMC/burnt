//! Parse-error type for the graph-query DSL.
//!
//! Errors carry source positions (1-indexed `line`/`column`) so `build.rs`
//! can render `error: <toml_path>:<line>:<col>: <reason>` and the offending
//! pattern surfaces in the build log rather than as an opaque "rule failed
//! to parse" footnote.

use std::fmt;

/// One-shot DSL parse error. Always carries position; never aggregated —
/// the parser stops at the first error and reports it.
#[derive(Debug, Clone)]
pub struct ParseError {
    pub kind: ParseErrorKind,
    /// 1-indexed line within the pattern body. The TOML field offset is
    /// added by `build.rs` at error-printing time.
    pub line: u32,
    /// 1-indexed column within the pattern body.
    pub column: u32,
}

impl ParseError {
    #[must_use]
    pub fn new(kind: ParseErrorKind, line: u32, column: u32) -> Self {
        Self { kind, line, column }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}: {}", self.line, self.column, self.kind)
    }
}

impl std::error::Error for ParseError {}

/// Discriminator for the kind of parse failure. Each variant carries
/// enough context to render an actionable message without the original
/// source string.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ParseErrorKind {
    /// Unexpected end of input mid-pattern.
    UnexpectedEof,
    /// Unbalanced parenthesis (open without close).
    UnbalancedOpenParen,
    /// Unbalanced parenthesis (close without open).
    UnbalancedCloseParen,
    /// String literal not terminated before EOF or newline.
    UnterminatedString,
    /// A `:keyword` argument with no following value.
    MissingPropValue { name: String },
    /// Head must look like `op:Foo`, `ast/Foo`, etc.
    InvalidHead { token: String },
    /// Head prefix isn't one of the five recognised forms.
    UnknownPrefix { prefix: String },
    /// Predicate without a `#` head, e.g. `(eq? @x "y")`.
    InvalidPredicate { token: String },
    /// Identifier expected, got something else.
    ExpectedIdent { got: String },
    /// Number literal couldn't be parsed.
    InvalidNumber { token: String },
    /// Size literal (e.g. `1Gi`) couldn't be parsed.
    InvalidSize { token: String },
    /// Duration literal (e.g. `1s`) couldn't be parsed.
    InvalidDuration { token: String },
    /// Empty pattern body — every rule must have at least one form.
    EmptyPattern,
    /// Generic — kept open for unforeseen tokens during early DSL evolution.
    UnexpectedToken { token: String },
}

impl fmt::Display for ParseErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof => write!(f, "unexpected end of pattern"),
            Self::UnbalancedOpenParen => write!(f, "'(' without matching ')'"),
            Self::UnbalancedCloseParen => write!(f, "')' without matching '('"),
            Self::UnterminatedString => write!(f, "string literal not terminated"),
            Self::MissingPropValue { name } => {
                write!(f, "property ':{name}' has no value")
            }
            Self::InvalidHead { token } => {
                write!(
                    f,
                    "invalid head '{token}' — expected 'op:<Kind>', 'ast/<Kind>', 'edge:<Kind>', 'overlay:<Kind>', or 'fact:<Name>'"
                )
            }
            Self::UnknownPrefix { prefix } => {
                write!(
                    f,
                    "unknown head prefix '{prefix}' — must be one of: op, ast, edge, overlay, fact"
                )
            }
            Self::InvalidPredicate { token } => {
                write!(f, "predicate must start with '#', got '{token}'")
            }
            Self::ExpectedIdent { got } => {
                write!(f, "expected identifier, got '{got}'")
            }
            Self::InvalidNumber { token } => {
                write!(f, "invalid number literal '{token}'")
            }
            Self::InvalidSize { token } => {
                write!(
                    f,
                    "invalid size literal '{token}' — accepted suffixes: B, KiB, MiB, GiB, TiB, KB, MB, GB, TB, Ki, Mi, Gi, Ti"
                )
            }
            Self::InvalidDuration { token } => {
                write!(
                    f,
                    "invalid duration literal '{token}' — accepted suffixes: ms, s, m, h"
                )
            }
            Self::EmptyPattern => write!(f, "empty pattern"),
            Self::UnexpectedToken { token } => write!(f, "unexpected token '{token}'"),
        }
    }
}
