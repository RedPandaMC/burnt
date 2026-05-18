//! Matcher context shared with predicates.
//!
//! The matcher (commit 7) hands a `MatchCtx` to every predicate
//! invocation. Predicates inspect the resolved graph + captures and
//! either return a boolean (most), a value (extractors like
//! `(method-of @x)`), or a finding mutation (`#when ... :confidence`).

use crate::resolved::ResolvedGraph;
use crate::rules::graph_dsl::value::CaptureMap;
use crate::types::{Confidence, Severity};

/// Everything a predicate needs to make its decision.
pub struct MatchCtx<'a> {
    pub resolved: &'a ResolvedGraph,
    pub captures: &'a CaptureMap,
}

impl<'a> MatchCtx<'a> {
    #[must_use]
    pub fn new(resolved: &'a ResolvedGraph, captures: &'a CaptureMap) -> Self {
        Self { resolved, captures }
    }
}

/// Mutation a `#when ... :setter <value>` predicate emits when its
/// trigger condition fires. The matcher folds these into the emitted
/// `Finding` after structural + predicate match succeeds.
#[derive(Debug, Clone, Default)]
pub struct FindingMutation {
    pub severity: Option<Severity>,
    pub confidence: Option<Confidence>,
    /// Appended to the rule's base message, separated by `" — "`. Used
    /// by BN002 to inject observed-bytes data: `"… — observed 1.2 GiB"`.
    pub message_suffix: Option<String>,
    /// Override the message entirely. Less commonly used.
    pub message: Option<String>,
}

impl FindingMutation {
    /// Merge another mutation into this one. Later wins for scalar
    /// fields; suffixes append.
    pub fn merge(&mut self, other: FindingMutation) {
        if let Some(s) = other.severity {
            self.severity = Some(s);
        }
        if let Some(c) = other.confidence {
            self.confidence = Some(c);
        }
        if let Some(msg) = other.message {
            self.message = Some(msg);
        }
        if let Some(suffix) = other.message_suffix {
            match self.message_suffix.as_mut() {
                Some(existing) => {
                    existing.push_str(" — ");
                    existing.push_str(&suffix);
                }
                None => self.message_suffix = Some(suffix),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finding_mutation_merge_overwrites_scalars() {
        let mut m = FindingMutation {
            severity: Some(Severity::Warning),
            confidence: Some(Confidence::Low),
            ..Default::default()
        };
        m.merge(FindingMutation {
            severity: Some(Severity::Error),
            confidence: Some(Confidence::High),
            ..Default::default()
        });
        assert!(matches!(m.severity, Some(Severity::Error)));
        assert!(matches!(m.confidence, Some(Confidence::High)));
    }

    #[test]
    fn finding_mutation_merge_appends_suffixes() {
        let mut m = FindingMutation {
            message_suffix: Some("a".into()),
            ..Default::default()
        };
        m.merge(FindingMutation {
            message_suffix: Some("b".into()),
            ..Default::default()
        });
        assert_eq!(m.message_suffix.as_deref(), Some("a — b"));
    }
}
