//! Single-pipeline execution path for the graph-DSL rule layer.
//!
//! Today this coexists with the legacy Pattern / Context / Dataflow
//! passes. The cutover commit (12) deletes those entries and makes
//! this the only path. The internal API is shaped now to make that
//! migration mechanical: one function `run_graph_rules(source,
//! language)` takes source text, builds the resolved graph internally
//! (via `ResolvedGraphBuilder` — no session), runs every rule whose
//! `[graph]` block matches, and returns the findings.

use std::sync::{Arc, OnceLock, RwLock};

use crate::resolved::{ResolvedGraph, ResolvedGraphBuilder};
use crate::rules::finding::make_finding;
use crate::rules::graph_dsl::context::FindingMutation;
use crate::rules::graph_dsl::matcher::{run_pattern, DslMatch};
use crate::rules::graph_dsl::parser::parse_pattern;
use crate::rules::graph_dsl::value::CaptureValue;
use crate::rules::graph_dsl::Pattern;
use crate::types::{CompiledRule, Confidence, Finding, Severity};

/// Cached parsed-pattern store keyed by rule code. Built lazily on first
/// rule execution; once populated, lookups are O(1) and don't re-parse.
fn pattern_cache() -> &'static RwLock<std::collections::HashMap<String, Arc<CompiledPatterns>>> {
    static CACHE: OnceLock<
        RwLock<std::collections::HashMap<String, Arc<CompiledPatterns>>>,
    > = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(std::collections::HashMap::new()))
}

struct CompiledPatterns {
    detect: Pattern,
    exclude: Option<Pattern>,
}

fn compile_for(rule: &CompiledRule) -> Option<Arc<CompiledPatterns>> {
    {
        let cache = pattern_cache().read().ok()?;
        if let Some(p) = cache.get(&rule.code) {
            return Some(p.clone());
        }
    }
    let detect = match parse_pattern(&rule.graph_detect) {
        Ok(p) => p,
        Err(e) => {
            eprintln!(
                "burnt: rule {} has invalid [graph].detect: {e}",
                rule.code
            );
            return None;
        }
    };
    let exclude = match rule.graph_exclude.as_deref() {
        Some(src) if !src.is_empty() => match parse_pattern(src) {
            Ok(p) => Some(p),
            Err(e) => {
                eprintln!(
                    "burnt: rule {} has invalid [graph].exclude: {e}",
                    rule.code
                );
                return None;
            }
        },
        _ => None,
    };
    let entry = Arc::new(CompiledPatterns { detect, exclude });
    if let Ok(mut cache) = pattern_cache().write() {
        cache.insert(rule.code.clone(), entry.clone());
    }
    Some(entry)
}

/// Run every rule with a `[graph]` block against `source`.
///
/// Builds the resolved graph internally with no session overlay
/// (static-only path). Per-rule patterns are compiled lazily and
/// cached for the lifetime of the process.
#[must_use]
pub fn run_graph_rules(
    source: &str,
    language: &str,
    rules: &[CompiledRule],
) -> Vec<Finding> {
    let Some(resolved) = resolve_for_source(source, language) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for rule in rules {
        if !rule.has_graph {
            continue;
        }
        if !lang_matches(&rule.language, language) {
            continue;
        }
        let Some(patterns) = compile_for(rule) else {
            continue;
        };
        let matches = run_pattern(&patterns.detect, patterns.exclude.as_ref(), &resolved);
        for m in matches {
            out.push(build_finding(rule, &m, &resolved));
        }
    }
    out
}

fn lang_matches(rule_lang: &str, source_lang: &str) -> bool {
    if rule_lang == "any" {
        return true;
    }
    let rl = rule_lang.to_ascii_lowercase();
    let sl = source_lang.to_ascii_lowercase();
    rl == sl || (rl == "notebook" && sl == "python")
}

fn resolve_for_source(source: &str, language: &str) -> Option<ResolvedGraph> {
    let graph = match language.to_ascii_lowercase().as_str() {
        "python" | "py" | "notebook" => crate::graph::Graph::from_python(source).ok()?,
        "sql" => crate::graph::Graph::from_sql(source).ok()?,
        _ => return None,
    };
    Some(ResolvedGraphBuilder::new(graph).with_source(source).build())
}

fn build_finding(rule: &CompiledRule, m: &DslMatch, resolved: &ResolvedGraph) -> Finding {
    let line = resolve_line(rule, m, resolved);
    let severity = mutation_severity(&m.mutation).unwrap_or_else(|| {
        rule.graph_finding_severity
            .as_deref()
            .and_then(parse_severity)
            .unwrap_or_else(|| rule.severity.clone())
    });
    let confidence = mutation_confidence(&m.mutation).unwrap_or_else(|| {
        rule.graph_finding_confidence
            .as_deref()
            .and_then(parse_confidence)
            .unwrap_or(Confidence::High)
    });
    let mut message = rule
        .graph_finding_message
        .as_ref()
        .cloned()
        .or_else(|| {
            if rule.description.is_empty() {
                None
            } else {
                Some(rule.description.clone())
            }
        })
        .unwrap_or_else(|| rule.code.clone());
    if let Some(suffix) = m.mutation.message_suffix.as_ref() {
        message.push_str(" — ");
        message.push_str(suffix);
    }
    if let Some(override_msg) = m.mutation.message.as_ref() {
        message = override_msg.clone();
    }
    let suggestion = rule
        .graph_finding_suggestion
        .clone()
        .unwrap_or_else(|| rule.suggestion.clone());

    make_finding(&rule.code, severity, &message, &suggestion, line, confidence)
}

fn resolve_line(rule: &CompiledRule, m: &DslMatch, resolved: &ResolvedGraph) -> u32 {
    // Try the [graph.finding].line template, otherwise fall back to the
    // anchor node's line_number.
    if let Some(template) = rule.graph_finding_line.as_deref() {
        if let Some(line) = resolve_line_template(template, m, resolved) {
            return line;
        }
    }
    resolved
        .graph()
        .nodes
        .iter()
        .find(|n| n.id == m.anchor.as_str())
        .and_then(|n| n.line_number)
        .unwrap_or(1)
}

fn resolve_line_template(template: &str, m: &DslMatch, resolved: &ResolvedGraph) -> Option<u32> {
    // Accepted forms today:
    //   "@cap.line"      → the captured node's line_number
    //   "@cap"           → same
    //   "<number>"       → literal
    let trimmed = template.trim();
    if let Ok(n) = trimmed.parse::<u32>() {
        return Some(n);
    }
    let cap_part = trimmed
        .strip_prefix('@')?
        .split('.')
        .next()
        .unwrap_or_default();
    let cap = m.captures.get(cap_part)?;
    match cap {
        CaptureValue::Node(id) => resolved
            .graph()
            .nodes
            .iter()
            .find(|n| n.id == id.as_str())
            .and_then(|n| n.line_number),
        CaptureValue::Number(n) => Some(*n as u32),
        _ => None,
    }
}

fn parse_severity(s: &str) -> Option<Severity> {
    match s.to_ascii_lowercase().as_str() {
        "error" => Some(Severity::Error),
        "warning" => Some(Severity::Warning),
        "info" => Some(Severity::Info),
        _ => None,
    }
}

fn parse_confidence(s: &str) -> Option<Confidence> {
    match s.to_ascii_lowercase().as_str() {
        "high" => Some(Confidence::High),
        "medium" => Some(Confidence::Medium),
        "low" => Some(Confidence::Low),
        "none" => Some(Confidence::None),
        _ => None,
    }
}

fn mutation_severity(m: &FindingMutation) -> Option<Severity> {
    m.severity.clone()
}

fn mutation_confidence(m: &FindingMutation) -> Option<Confidence> {
    m.confidence.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_rule(
        code: &str,
        detect: &str,
        exclude: Option<&str>,
        language: &str,
    ) -> CompiledRule {
        CompiledRule {
            id: code.to_lowercase(),
            code: code.into(),
            severity: Severity::Warning,
            language: language.into(),
            description: format!("test rule {code}"),
            suggestion: "fix it".into(),
            category: "Test".into(),
            tags: Vec::new(),
            has_graph: true,
            graph_detect: detect.into(),
            graph_exclude: exclude.map(String::from),
            graph_finding_severity: None,
            graph_finding_confidence: None,
            graph_finding_message: None,
            graph_finding_suggestion: None,
            graph_finding_line: None,
        }
    }

    #[test]
    fn python_source_drives_op_kind_match() {
        let source = r#"
df = spark.read.parquet("s3://b/k")
df.collect()
"#;
        let rule = make_rule(
            "TEST001",
            r#"(op:Action (ast/Call :method "collect"))"#,
            None,
            "python",
        );
        let findings = run_graph_rules(source, "python", std::slice::from_ref(&rule));
        assert!(!findings.is_empty(), "expected at least one finding");
        assert_eq!(findings[0].code, "TEST001");
    }

    #[test]
    fn rule_with_invalid_dsl_returns_no_findings_and_does_not_panic() {
        let rule = make_rule("BAD", "(not balanced", None, "python");
        let findings = run_graph_rules("df.collect()", "python", std::slice::from_ref(&rule));
        assert!(findings.is_empty());
    }

    #[test]
    fn language_filter_skips_non_matching_rules() {
        let rule = make_rule(
            "PYRULE",
            r#"(op:Action)"#,
            None,
            "python",
        );
        let findings = run_graph_rules("SELECT 1", "sql", std::slice::from_ref(&rule));
        assert!(findings.is_empty());
    }

    #[test]
    fn anchor_line_drives_finding_line_when_no_template() {
        let source = "\n\ndf.collect()\n";
        let rule = make_rule(
            "LINE-TEST",
            r#"(op:Action (ast/Call :method "collect"))"#,
            None,
            "python",
        );
        let findings = run_graph_rules(source, "python", std::slice::from_ref(&rule));
        assert!(!findings.is_empty());
        assert!(findings[0].line_number.is_some());
    }
}
