use crate::types::{Confidence, Finding, Severity};

pub fn make_finding(
    code: &str,
    severity: Severity,
    message: &str,
    suggestion: &str,
    line: u32,
    confidence: Confidence,
) -> Finding {
    Finding {
        rule_id: code.to_string(),
        code: code.to_string(),
        severity,
        message: message.to_string(),
        suggestion: Some(suggestion.to_string()),
        line_number: Some(line),
        column: None,
        confidence,
    }
}
