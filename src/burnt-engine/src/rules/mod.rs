use crate::parse::import_map::ImportMap;
use crate::types::{CompiledRule, Confidence, Finding as TypesFinding, RuleEntry};
use pyo3::prelude::*;
use std::sync::OnceLock;
use tree_sitter::Parser;

pub mod rule;
mod context;
pub(crate) mod context_structs;
mod dataflow;
pub(crate) mod finding;
pub mod graph_dsl;
pub mod graph_pipeline;
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
pub use rule::{AnalysisCtx, LanguageFilter, Rule, RuleMeta};

/// PyO3-exposed rule descriptor. Exposed to Python as `Rule`.
#[pyclass(name = "Rule")]
#[derive(Clone)]
pub struct PyRuleInfo {
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

        let tracker = if language == "python" || language == "sdp" {
            let mut parser = Parser::new();
            parser
                .set_language(&tree_sitter_python::LANGUAGE.into())
                .ok();
            parser
                .parse(source, None)
                .map(|tree| ImportMap::build(source, tree.root_node()))
        } else {
            None
        };

        let mut context_findings = self.execute_context_rules(source, language, tracker.as_ref());
        findings.append(&mut context_findings);

        let mut dataflow_findings = self.execute_dataflow_rules(source, language);
        findings.append(&mut dataflow_findings);

        // Fourth pass: graph-DSL rules. Coexists with the three legacy
        // passes for the migration bridge state; commit 12 deletes the
        // others and makes this the only path.
        let mut graph_findings = graph_pipeline::run_graph_rules(source, language, &self.rules);
        findings.append(&mut graph_findings);

        findings
    }

    fn execute_pattern_rules(&self, source: &str, language: &str) -> Vec<TypesFinding> {
        let mut findings = Vec::new();

        for rule in &self.rules {
            // Migration gating: rules that own a [graph] block fire only
            // through the graph_pipeline pass below. The legacy [query]
            // entry stays in the TOML during the bridge state but is
            // skipped here to avoid duplicate findings.
            if rule.has_graph {
                continue;
            }
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

    fn execute_context_rules(
        &self,
        source: &str,
        language: &str,
        tracker: Option<&ImportMap>,
    ) -> Vec<TypesFinding> {
        let mut findings = Vec::new();

        for rule in &self.rules {
            if rule.has_graph {
                continue;
            }
            if rule.has_context && lang_matches(&rule.language, language) {
                findings.extend(context::analyze_context_for_rule(
                    &rule.code, source, tracker,
                ));
            }
        }

        findings
    }

    fn execute_dataflow_rules(&self, source: &str, language: &str) -> Vec<TypesFinding> {
        let mut findings = Vec::new();

        for rule in &self.rules {
            if rule.has_graph {
                continue;
            }
            if rule.has_dataflow && lang_matches(&rule.language, language) {
                findings.extend(dataflow::check_dataflow_rules(&rule.code, source));
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

// ── RuleEngine — trait-based extensible engine ───────────────────────────────

/// A trait-object–based rule engine. Register rules via [`RuleEngine::add`].
/// Coexists with [`RulePipeline`] during the migration period; rules migrated
/// to the [`Rule`] trait are registered here and run in addition to the
/// data-driven pipeline.
pub struct RuleEngine {
    rules: Vec<Box<dyn Rule>>,
}

impl RuleEngine {
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
        }
    }

    /// Register a rule implementation.
    pub fn add<R: Rule + 'static>(&mut self, rule: R) {
        self.rules.push(Box::new(rule));
    }

    /// Run all registered rules against `source` in `language`.
    pub fn run(&self, source: &str, language: &str) -> Vec<TypesFinding> {
        if self.rules.is_empty() {
            return Vec::new();
        }

        let mut parser = Parser::new();
        let tree = if language == "python" || language == "sdp" {
            parser
                .set_language(&tree_sitter_python::LANGUAGE.into())
                .ok();
            parser.parse(source, None)
        } else {
            None
        };

        let import_map = tree
            .as_ref()
            .map(|t| ImportMap::build(source, t.root_node()))
            .unwrap_or_default();

        let ctx = AnalysisCtx::new(source, language, &import_map, tree.as_ref());

        self.rules
            .iter()
            .filter(|r| r.language().matches(language))
            .flat_map(|r| r.check(&ctx))
            .collect()
    }
}

impl Default for RuleEngine {
    fn default() -> Self {
        Self::new()
    }
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
        // BP008 has migrated to the [graph] block, so it now fires through
        // graph_pipeline rather than execute_pattern_rules. The pipeline
        // entry point execute() composes all passes — verify BP008 still
        // surfaces end-to-end.
        let pipeline = RulePipeline::new();
        let findings = pipeline.execute("df.collect()", "python");
        assert!(findings.iter().any(|f| f.code == "BP008"));
    }
}
