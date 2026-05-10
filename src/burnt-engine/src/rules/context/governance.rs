use crate::rules::context_structs::RuleContext;
use crate::rules::finding::make_finding;
use crate::types::{Confidence, Finding, Severity};

pub(super) fn check_two_part_table_name(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let upper = source.to_uppercase();
    for keyword in &["FROM ", "JOIN ", "INTO ", "TABLE "] {
        let mut search_pos = 0;
        while let Some(kw_pos) = upper[search_pos..].find(keyword) {
            let abs_pos = search_pos + kw_pos;
            let after = upper[abs_pos + keyword.len()..].trim_start();
            let token: &str = after.split_whitespace().next().unwrap_or("");
            let dot_count = token.chars().filter(|&c| c == '.').count();
            if dot_count == 1 && !token.is_empty() {
                let first_part = token.split('.').next().unwrap_or("");
                if !first_part.is_empty()
                    && first_part.chars().all(|c| c.is_alphanumeric() || c == '_')
                {
                    return vec![make_finding(
                        "BU001",
                        Severity::Warning,
                        "Two-part table name omits the Unity Catalog prefix — resolves to the default catalog",
                        "Use three-part naming: catalog.schema.table for explicit catalog resolution",
                        1,
                        Confidence::Medium,
                    )];
                }
            }
            search_pos = abs_pos + keyword.len();
        }
    }
    vec![]
}
