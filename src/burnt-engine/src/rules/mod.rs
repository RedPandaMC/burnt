use crate::types::{CompiledRule, Confidence, Finding as TypesFinding, RuleEntry};
use pyo3::prelude::*;
use std::sync::OnceLock;

mod context;
mod dataflow;
mod notebook_queries;
mod query;
mod registry {
    include!(concat!(env!("OUT_DIR"), "/registry.rs"));
}
#[allow(unused)]
mod generated_tests {
    include!(concat!(env!("OUT_DIR"), "/generated_tests.rs"));
}

pub use notebook_queries::NotebookQueryEngine;
pub use query::{QueryEngine, QueryError};

#[pyclass]
#[derive(Clone)]
pub struct Rule {
    #[pyo3(get)]
    pub id: String,
    #[pyo3(get)]
    pub code: String,
    #[pyo3(get)]
    pub severity: String,
    #[pyo3(get)]
    pub language: String,
    #[pyo3(get)]
    pub description: String,
    #[pyo3(get)]
    pub suggestion: String,
    #[pyo3(get)]
    pub category: String,
}

pub struct RulePipeline {
    rules: Vec<CompiledRule>,
    query_engine: QueryEngine,
}

impl RulePipeline {
    pub fn new() -> Self {
        Self {
            rules: registry::load_compiled_rules(),
            query_engine: QueryEngine::new(),
        }
    }

    pub fn execute(&self, source: &str, language: &str) -> Vec<TypesFinding> {
        let mut findings = Vec::new();

        let mut pattern_findings = self.execute_pattern_rules(source, language);
        findings.append(&mut pattern_findings);

        let mut context_findings = self.execute_context_rules(source, language);
        findings.append(&mut context_findings);

        let mut dataflow_findings = self.execute_dataflow_rules(source, language);
        findings.append(&mut dataflow_findings);

        findings
    }

    fn execute_pattern_rules(&self, source: &str, language: &str) -> Vec<TypesFinding> {
        let mut findings = Vec::new();

        for rule in &self.rules {
            if lang_matches(&rule.language, language) {
                if let Ok(Some((line, col))) = self.test_rule_patterns(source, language, rule) {
                    findings.push(TypesFinding {
                        rule_id: rule.id.clone(),
                        code: rule.code.clone(),
                        severity: rule.severity.clone(),
                        message: rule.description.clone(),
                        suggestion: Some(rule.suggestion.clone()),
                        line_number: Some(line),
                        column: Some(col),
                        confidence: Confidence::Medium,
                    });
                }
            }
        }

        findings
    }

    fn execute_context_rules(&self, source: &str, language: &str) -> Vec<TypesFinding> {
        let mut findings = Vec::new();

        for rule in &self.rules {
            if rule.has_context && lang_matches(&rule.language, language) {
                let ctx_findings = context::analyze_context_for_rule(
                    &rule.code,
                    source,
                    &context::ContextConfig {
                        rule_code: rule.code.clone(),
                        context_type: String::new(),
                    },
                );
                findings.extend(ctx_findings);
            }
        }

        findings
    }

    fn execute_dataflow_rules(&self, source: &str, language: &str) -> Vec<TypesFinding> {
        let mut findings = Vec::new();

        for rule in &self.rules {
            if rule.has_dataflow && lang_matches(&rule.language, language) {
                findings.extend(dataflow::analyze_dataflow_for_rule(&rule.code, source));
            }
        }

        findings
    }

    fn test_rule_patterns(
        &self,
        source: &str,
        language: &str,
        rule: &CompiledRule,
    ) -> Result<Option<(u32, u32)>, QueryError> {
        if rule.patterns.is_empty() {
            return Ok(None);
        }

        let tree = self.query_engine.parse_source(source, language)?;

        let mut first_match_pos: Option<(u32, u32)> = None;
        let mut negative_matched = false;

        for pattern in &rule.patterns {
            let query = match self
                .query_engine
                .create_query(&pattern.match_pattern, language)
            {
                Ok(q) => q,
                Err(e) => {
                    eprintln!("Error compiling pattern for rule {}: {}", rule.code, e);
                    continue;
                }
            };

            let matches = self.query_engine.execute_query(&tree, &query, source);

            if !matches.is_empty() {
                if pattern.is_negative {
                    negative_matched = true;
                } else if first_match_pos.is_none() {
                    let pos = matches[0]
                        .captures
                        .first()
                        .map(|c| {
                            (
                                c.start_position.row as u32 + 1,
                                c.start_position.column as u32 + 1,
                            )
                        })
                        .unwrap_or((1, 1));
                    first_match_pos = Some(pos);
                }
            }
        }

        if negative_matched {
            return Ok(None);
        }

        Ok(first_match_pos)
    }
}

impl Default for RulePipeline {
    fn default() -> Self {
        Self::new()
    }
}

fn lang_matches(rule_lang: &str, query_lang: &str) -> bool {
    let rl = rule_lang.to_lowercase();
    rl == query_lang.to_lowercase() || rl == "all" || rl == "notebook"
}

static PIPELINE: OnceLock<RulePipeline> = OnceLock::new();

pub fn run(source: &str, language: &str) -> Result<Vec<TypesFinding>, String> {
    let pipeline = PIPELINE.get_or_init(RulePipeline::new);
    Ok(pipeline.execute(source, language))
}

pub fn list_all() -> Vec<RuleEntry> {
    registry::load_registry()
}

#[pyfunction]
pub fn get_registry_count() -> usize {
    registry::load_registry().len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_engine_integration() {
        let engine = QueryEngine::new();
        let _result = engine.test_pattern(
            "df.collect()",
            "python",
            r#"(call function: (attribute object: (_) attribute: (identifier) @method_name) (#eq? @method_name "collect"))"#,
        );
        println!("Query engine integration test completed");
    }

    #[test]
    fn test_rule_pipeline_fires_bp008() {
        let pipeline = RulePipeline::new();
        let findings = pipeline.execute_pattern_rules("df.collect()", "python");
        assert!(findings.iter().any(|f| f.code == "BP008"));
    }
}
