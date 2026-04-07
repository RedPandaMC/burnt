use crate::types::{CompiledRule, Confidence, ExecutionPhase, Finding as TypesFinding, RuleEntry};
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::OnceLock;

#[allow(unused_imports)]
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
        let mut rules_by_tier: HashMap<u8, Vec<CompiledRule>> = HashMap::new();

        for rule in compiled_rules {
            rules_by_tier
                .entry(rule.tier)
                .or_default()
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
            ExecutionPhase::Syntax => vec![],
            ExecutionPhase::SimplePatterns => self.execute_tier1_rules(source, language),
            ExecutionPhase::ContextRules => self.execute_tier2_rules(source, language),
            ExecutionPhase::SemanticRules => self.execute_tier3_rules(source, language),
            ExecutionPhase::CrossCell => vec![], // TODO: cross-cell analysis
            ExecutionPhase::DltRules => vec![],  // DLT rules are Tier 1
            ExecutionPhase::Finalize => vec![],
        }
    }

    fn execute_tier2_rules(&self, source: &str, language: &str) -> Vec<TypesFinding> {
        let mut findings = Vec::new();

        if let Some(tier2_rules) = self.rules_by_tier.get(&2) {
            for rule in tier2_rules {
                if lang_matches(&rule.language, language) {
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
        }

        findings
    }

    fn execute_tier3_rules(&self, source: &str, language: &str) -> Vec<TypesFinding> {
        let mut findings = Vec::new();

        if let Some(tier3_rules) = self.rules_by_tier.get(&3) {
            for rule in tier3_rules {
                if lang_matches(&rule.language, language) {
                    findings.extend(dataflow::analyze_dataflow_for_rule(&rule.code, source));
                }
            }
        }

        findings
    }

    fn execute_tier1_rules(&self, source: &str, language: &str) -> Vec<TypesFinding> {
        let mut findings = Vec::new();

        if let Some(tier1_rules) = self.rules_by_tier.get(&1) {
            for rule in tier1_rules {
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
    let mut all_findings = Vec::new();

    for phase in &pipeline.phases {
        all_findings.extend(pipeline.execute_phase(*phase, source, language));
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
        let findings = pipeline.execute_tier1_rules("df.collect()", "python");
        assert!(findings.iter().any(|f| f.code == "BP008"));
    }
}
