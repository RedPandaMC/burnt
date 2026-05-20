//! Unified error type for burnt-engine.
//!
//! Every fallible public API returns `Result<_, EngineError>` so callers
//! (including the PyO3 boundary in `lib.rs`) can match on error kind
//! without inspecting strings.  Internal code may use `Result<_, String>`
//! temporarily, but public surfaces must use `EngineError`.
//!
//! Mapping to Python exceptions lives in `crate::py_exceptions`.

/// Top-level error enum for all engine failure modes.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// A source file could not be parsed (syntax error, grammar failure).
    #[error("parse error: {0}")]
    Parse(String),

    /// A file-system operation failed (wraps [`std::io::Error`]).
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// The requested file does not exist.
    #[error("file not found: {0}")]
    FileNotFound(String),

    /// The file has an unsupported extension or format.
    #[error("unsupported file format: {0}")]
    UnsupportedFormat(String),

    /// A compiled rule failed during evaluation.
    #[error("rule evaluation error: {0}")]
    Rule(String),

    /// A catalog lookup failed (table not found or API unreachable).
    #[error("catalog error: {0}")]
    Catalog(String),

    /// An HTTP request returned a non-2xx status.
    #[error("HTTP {status} for {url}")]
    HttpStatus { status: u16, url: String },

    /// An HTTP client-side error (connection refused, timeout, TLS).
    #[error("HTTP client error: {0}")]
    HttpClient(String),

    /// Graph construction failed (invalid AST, missing data).
    #[error("graph build error: {0}")]
    GraphBuild(String),

    /// Catch-all for internal invariant violations.
    #[error("internal error: {0}")]
    Internal(String),
}
