use crate::parse::import_map::ImportMap;
use crate::types::{CompiledRule, Finding as TypesFinding, RuleEntry};
use pyo3::prelude::*;
use std::sync::OnceLock;
use tree_sitter::Parser;

pub(crate) mod finding;
pub mod graph_dsl;
pub mod graph_pipeline;
mod registry {
    include!(concat!(env!("OUT_DIR"), "/registry.rs"));
}
#[allow(unused)]
mod generated_tests {
    include!(concat!(env!("OUT_DIR"), "/generated_tests.rs"));
}

pub mod rule;
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
}

impl RulePipeline {
    pub fn new() -> Self {
        Self {
            rules: registry::load_compiled_rules(),
        }
    }

    pub fn execute(&self, source: &str, language: &str) -> Vec<TypesFinding> {
        graph_pipeline::run_graph_rules(source, language, &self.rules)
    }
}

impl Default for RulePipeline {
    fn default() -> Self {
        Self::new()
    }
}

static PIPELINE: OnceLock<RulePipeline> = OnceLock::new();

pub fn run(source: &str, language: &str) -> Result<Vec<TypesFinding>, String> {
    let pipeline = PIPELINE.get_or_init(RulePipeline::new);
    Ok(pipeline.execute(source, language))
}

// ── RuleEngine — trait-based extensible engine ───────────────────────────────

pub struct RuleEngine {
    rules: Vec<Box<dyn Rule>>,
}

impl RuleEngine {
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    pub fn add<R: Rule + 'static>(&mut self, rule: R) {
        self.rules.push(Box::new(rule));
    }

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
    fn test_rule_pipeline_fires_bp008() {
        let pipeline = RulePipeline::new();
        let findings = pipeline.execute("df.collect()", "python");
        assert!(findings.iter().any(|f| f.code == "BP008"));
    }
}
