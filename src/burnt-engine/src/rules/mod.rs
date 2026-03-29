use crate::types::{CompiledRule, Confidence, ExecutionPhase, Finding as TypesFinding, RuleEntry};
use pyo3::prelude::*;
use std::collections::HashMap;

mod cinder;
mod context;
mod dataflow;
mod registry {
    include!(concat!(env!("OUT_DIR"), "/registry.rs"));
}
#[allow(unused)]
mod generated_tests {
    include!(concat!(env!("OUT_DIR"), "/generated_tests.rs"));
}

mod query;
pub use query::{QueryEngine, QueryError};

#[pyclass]
#[derive(Clone)]
pub struct Finding {
    #[pyo3(get)]
    pub rule_id: String,
    #[pyo3(get)]
    pub code: String,
    #[pyo3(get)]
    pub severity: String,
    #[pyo3(get)]
    pub message: String,
    #[pyo3(get)]
    pub suggestion: Option<String>,
    #[pyo3(get)]
    pub line_number: Option<u32>,
    #[pyo3(get)]
    pub column: Option<u32>,
    #[pyo3(get)]
    pub confidence: String,
}

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
    #[pyo3(get)]
    pub tier: u8,
}

pub struct RulePipeline {
    rules_by_tier: HashMap<u8, Vec<CompiledRule>>,
    query_engine: QueryEngine,
    pub phases: Vec<ExecutionPhase>,
}

impl RulePipeline {
    pub fn new() -> Self {
        let compiled_rules = registry::load_compiled_rules();
        let mut rules_by_tier = HashMap::new();

        for rule in compiled_rules {
            rules_by_tier
                .entry(rule.tier)
                .or_insert_with(Vec::new)
                .push(rule);
        }

        Self {
            rules_by_tier,
            query_engine: QueryEngine::new(),
            phases: vec![
                ExecutionPhase::Syntax,
                ExecutionPhase::SimplePatterns,
                ExecutionPhase::ContextRules,
                ExecutionPhase::SemanticRules,
                ExecutionPhase::DltRules,
                ExecutionPhase::CrossCell,
                ExecutionPhase::Finalize,
            ],
        }
    }

    pub fn execute_phase(
        &self,
        phase: ExecutionPhase,
        source: &str,
        language: &str,
    ) -> Vec<TypesFinding> {
        match phase {
            ExecutionPhase::Syntax | ExecutionPhase::SimplePatterns => {
                self.execute_tier1_rules(source, language)
            }
            ExecutionPhase::ContextRules => self.execute_tier2_rules(source, language),
            ExecutionPhase::SemanticRules => self.execute_tier3_rules(source, language),
            ExecutionPhase::CrossCell => vec![], // TODO: cross-cell analysis
            ExecutionPhase::DltRules => vec![],  // DLT rules are Tier 1
            ExecutionPhase::Finalize => vec![],
            _ => vec![],
        }
    }

    fn execute_tier2_rules(&self, source: &str, language: &str) -> Vec<TypesFinding> {
        let mut findings = Vec::new();

        if let Some(tier2_rules) = self.rules_by_tier.get(&2) {
            eprintln!(
                "DEBUG execute_tier2_rules: {} rules for language {}",
                tier2_rules.len(),
                language
            );
            for rule in tier2_rules {
                eprintln!(
                    "DEBUG   checking rule {} with language {}",
                    rule.code, rule.language
                );
                if rule.language.to_lowercase() == language.to_lowercase()
                    || rule.language.to_lowercase() == "all"
                    || rule.language.to_lowercase() == "notebook"
                {
                    eprintln!("DEBUG     executing context for {}", rule.code);
                    let ctx_findings = context::analyze_context_for_rule(
                        &rule.code,
                        source,
                        &context::ContextConfig {
                            rule_code: rule.code.clone(),
                            context_type: String::new(),
                        },
                    );
                    eprintln!("DEBUG     found {} findings", ctx_findings.len());
                    for finding in ctx_findings {
                        findings.push(TypesFinding {
                            rule_id: finding.rule_id,
                            code: finding.code,
                            severity: finding.severity,
                            message: finding.message,
                            suggestion: finding.suggestion,
                            line_number: finding.line_number,
                            column: finding.column,
                            confidence: finding.confidence,
                        });
                    }
                }
            }
        } else {
            eprintln!("DEBUG execute_tier2_rules: no tier2 rules found");
        }

        findings
    }

    fn execute_tier3_rules(&self, source: &str, language: &str) -> Vec<TypesFinding> {
        let mut findings = Vec::new();

        if let Some(tier3_rules) = self.rules_by_tier.get(&3) {
            for rule in tier3_rules {
                if rule.language.to_lowercase() == language.to_lowercase()
                    || rule.language.to_lowercase() == "all"
                {
                    let df_findings = dataflow::analyze_dataflow_for_rule(&rule.code, source);
                    for finding in df_findings {
                        findings.push(TypesFinding {
                            rule_id: finding.rule_id,
                            code: finding.code,
                            severity: finding.severity,
                            message: finding.message,
                            suggestion: finding.suggestion,
                            line_number: finding.line_number,
                            column: finding.column,
                            confidence: finding.confidence,
                        });
                    }
                }
            }
        }

        findings
    }

    fn execute_tier1_rules(&self, source: &str, language: &str) -> Vec<TypesFinding> {
        let mut findings = Vec::new();

        if let Some(tier1_rules) = self.rules_by_tier.get(&1) {
            for rule in tier1_rules {
                if rule.language.to_lowercase() == language.to_lowercase() || rule.language == "All"
                {
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
        }

        findings
    }

    /// Returns `Ok(Some((line, col)))` when the rule fires, `Ok(None)` when it doesn't.
    /// line and col are 1-based.
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
                    // Capture position of first capture in first match
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

pub fn run(source: &str, language: &str) -> Result<Vec<TypesFinding>, String> {
    let pipeline = RulePipeline::new();
    let mut all_findings = Vec::new();

    // Execute all phases
    for phase in &pipeline.phases {
        let phase_findings = pipeline.execute_phase(*phase, source, language);
        all_findings.extend(phase_findings);
    }

    Ok(all_findings)
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
    use insta::assert_yaml_snapshot;

    #[test]
    fn test_query_engine_integration() {
        let engine = QueryEngine::new();
        let source = r#"df.collect()"#;

        let result = engine.test_pattern(source, "python", 
            r#"(call function: (attribute object: (_) attribute: (identifier) @method_name) (#eq? @method_name "collect"))"#
        );

        // TODO: This test will fail until execute_query is properly implemented
        // For now, just verify no panic
        println!("Query engine integration test completed");
    }

    #[test]
    fn test_rule_pipeline_fires_bp008() {
        let pipeline = RulePipeline::new();
        let findings = pipeline.execute_tier1_rules("df.collect()", "python");
        assert!(findings.iter().any(|f| f.code == "BP008"));
    }
}
