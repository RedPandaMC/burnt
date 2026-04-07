#![allow(dead_code)]
//! CPL Pattern Parser
//!
//! Parses Cinder Pattern Language (CPL) strings into an intermediate representation.
//! CPL uses actual code syntax with $METAVARIABLES for pattern matching.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CinderPattern {
    pub detect: String,
    pub exclude: Option<String>,
}

impl CinderPattern {
    pub fn new(detect: impl Into<String>) -> Self {
        Self {
            detect: detect.into(),
            exclude: None,
        }
    }

    pub fn with_exclude(detect: impl Into<String>, exclude: impl Into<String>) -> Self {
        Self {
            detect: detect.into(),
            exclude: Some(exclude.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextPattern {
    pub inside: Option<String>,
    pub not_inside: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataflowPattern {
    pub source: String,
    pub sink: String,
    pub cross_cell: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_creation() {
        let p = CinderPattern::new("$DF.collect()");
        assert_eq!(p.detect, "$DF.collect()");
        assert!(p.exclude.is_none());
    }

    #[test]
    fn test_pattern_with_exclude() {
        let p = CinderPattern::with_exclude("$DF.collect()", "$DF.limit($N).collect()");
        assert_eq!(p.detect, "$DF.collect()");
        assert_eq!(p.exclude, Some("$DF.limit($N).collect()".to_string()));
    }
}
