use crate::rules::context_structs::RuleContext;
use crate::rules::finding::make_finding;
use crate::types::{Confidence, Finding, Severity};

pub(super) fn check_correlated_subquery(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let upper = source.to_uppercase();
    if !upper.contains("NOT IN") || !upper.contains("SELECT") {
        return vec![];
    }

    // Fire when NOT IN is followed (within ~300 chars) by a SELECT..WHERE with a dotted column ref
    let not_in_positions: Vec<_> = upper.match_indices("NOT IN").collect();
    for (pos, _) in not_in_positions {
        let window = &source[pos..std::cmp::min(pos + 300, source.len())];
        let window_upper = window.to_uppercase();
        if window_upper.contains("SELECT") && window_upper.contains("WHERE") {
            let has_dot_ref = window.split_whitespace().any(|tok| {
                let t = tok.trim_matches(|c: char| !c.is_alphanumeric() && c != '.' && c != '_');
                let parts: Vec<&str> = t.split('.').collect();
                parts.len() == 2
                    && parts
                        .iter()
                        .all(|p| !p.is_empty() && p.chars().all(|c| c.is_alphanumeric() || c == '_'))
            });
            if has_dot_ref {
                return vec![make_finding(
                    "BQ004",
                    Severity::Error,
                    "Correlated subquery references outer columns — Spark may execute as a nested loop join",
                    "Rewrite as a join or use window functions",
                    1,
                    Confidence::Medium,
                )];
            }
        }
    }

    vec![]
}
