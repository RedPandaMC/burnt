//! Typed Python exception hierarchy for burnt-engine.
//!
//! Exceptions are registered as subclasses of `BurntEngineError` so
//! Python callers can catch specific error categories:
//!
//! ```python
//! from burnt._engine import ParseError, CatalogError
//! try:
//!     findings = engine.run_rules(source)
//! except ParseError:
//!     ...  # syntax error in source
//! except CatalogError:
//!     ...  # Unity Catalog unavailable
//! ```
//!
//! The single `From<EngineError>` impl in this module maps every
//! `EngineError` variant to the appropriate Python exception class,
//! replacing the scattered `PyRuntimeError::new_err` / `PyIOError::new_err`
//! calls that previously lived in `lib.rs`.

#![allow(unexpected_cfgs)]

use crate::error::EngineError;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::PyErr;

create_exception!(_engine, BurntEngineError, PyException);
create_exception!(_engine, ParseError, BurntEngineError);
create_exception!(_engine, RuleError, BurntEngineError);
create_exception!(_engine, CatalogError, BurntEngineError);
create_exception!(_engine, IoError, BurntEngineError);

impl From<EngineError> for PyErr {
    fn from(e: EngineError) -> Self {
        match &e {
            EngineError::Parse(_) | EngineError::GraphBuild(_) => {
                ParseError::new_err(e.to_string())
            }
            EngineError::FileNotFound(_)
            | EngineError::UnsupportedFormat(_)
            | EngineError::Io(_)
            | EngineError::HttpStatus { .. }
            | EngineError::HttpClient(_) => IoError::new_err(e.to_string()),
            EngineError::Rule(_) => RuleError::new_err(e.to_string()),
            EngineError::Catalog(_) => CatalogError::new_err(e.to_string()),
            EngineError::Internal(_) => BurntEngineError::new_err(e.to_string()),
        }
    }
}
