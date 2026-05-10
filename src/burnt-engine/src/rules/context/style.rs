use crate::rules::context_structs::RuleContext;
use crate::rules::finding::make_finding;
use crate::types::{Confidence, Finding, Severity};

pub(super) fn check_cell_no_comment(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let mut findings = Vec::new();
    let lines: Vec<&str> = source.lines().collect();

    let mut in_cell = false;
    let mut cell_start_line = 0;
    let mut has_comment = false;
    let cell_markers = ["# cell", "#%%", "# %%", "# In["];

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();

        let mut is_marker = false;
        for marker in &cell_markers {
            if trimmed.starts_with(marker) {
                if in_cell && !has_comment && cell_start_line < i {
                    findings.push(make_finding(
                        "BP001",
                        Severity::Info,
                        "Cell has no comments",
                        "Add comments for clarity",
                        (cell_start_line + 1) as u32,
                        Confidence::Low,
                    ));
                }
                in_cell = true;
                cell_start_line = i;
                has_comment = false;
                is_marker = true;
                break;
            }
        }

        if !is_marker && in_cell && (trimmed.starts_with('#') && !trimmed.starts_with("# MAGIC")) {
            has_comment = true;
        }
    }

    findings
}

pub(super) fn check_long_line(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    const MAX: usize = 120;
    source
        .lines()
        .enumerate()
        .filter(|(_, line)| line.len() > MAX)
        .map(|(i, _)| Finding {
            rule_id: "BP002".to_string(),
            code: "BP002".to_string(),
            severity: Severity::Info,
            message: format!("Line exceeds {} characters", MAX),
            suggestion: Some("Break line for readability".to_string()),
            line_number: Some((i + 1) as u32),
            column: Some(MAX as u32),
            confidence: Confidence::High,
        })
        .collect()
}
