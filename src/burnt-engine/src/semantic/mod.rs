
use crate::types::Finding;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scope {
    pub name: String,
    pub variables: Vec<String>,
    pub parent: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Binding {
    pub name: String,
    pub defined_at_line: u32,
    pub used_at_lines: Vec<u32>,
    pub kind: BindingKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BindingKind {
    Assignment,
    Parameter,
    Import,
    FunctionDef,
    ClassDef,
}

#[derive(Debug, Clone)]
pub enum SourceKind {
    SdpRead,
    DpRead,
    SparkRead,
    SparkReadStream,
    TableRef,
    Constant,
    Udf,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct ChainContext {
    pub actions: Vec<ChainAction>,
    pub has_limit: bool,
    pub has_select: bool,
    pub has_filter: bool,
    pub is_streaming: bool,
    pub source_tables: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ChainAction {
    Read,
    ReadStream,
    Write,
    Transform,
    Collect,
    Show,
    Limit,
    Filter,
    Select,
}

#[derive(Debug, Clone)]
pub struct SemanticModel {
    scopes: Vec<Scope>,
    bindings: HashMap<String, Binding>,
    findings: Vec<Finding>,
}

impl SemanticModel {
    pub fn new() -> Self {
        Self {
            scopes: vec![Scope {
                name: "global".to_string(),
                variables: Vec::new(),
                parent: None,
            }],
            bindings: HashMap::new(),
            findings: Vec::new(),
        }
    }

    pub fn push_scope(&mut self, name: String) {
        let parent = self.scopes.last().map(|s| s.name.clone());
        self.scopes.push(Scope {
            name,
            variables: Vec::new(),
            parent,
        });
    }

    pub fn pop_scope(&mut self) {
        if self.scopes.len() > 1 {
            self.scopes.pop();
        }
    }

    pub fn bind(&mut self, name: String, kind: BindingKind, line: u32) {
        if self.bindings.contains_key(&name) {
            if let Some(existing) = self.bindings.get(&name) {
                self.findings.push(Finding {
                    rule_id: "BNT".to_string(),
                    code: "BN003".to_string(),
                    severity: crate::types::Severity::Warning,
                    message: format!(
                        "Variable '{}' shadows previous binding at line {}",
                        name, existing.defined_at_line
                    ),
                    suggestion: Some("Use a different variable name".to_string()),
                    line_number: Some(line),
                    column: None,
                    confidence: crate::types::Confidence::Medium,
                });
            }
        }

        self.bindings.insert(
            name.clone(),
            Binding {
                name: name.clone(),
                defined_at_line: line,
                used_at_lines: Vec::new(),
                kind,
            },
        );

        if let Some(scope) = self.scopes.last_mut() {
            scope.variables.push(name);
        }
    }

    pub fn record_use(&mut self, name: &str, line: u32) {
        if let Some(binding) = self.bindings.get_mut(name) {
            binding.used_at_lines.push(line);
        }
    }

    pub fn classify_rhs(&self, source: &str) -> SourceKind {
        let source = source.trim();

        if source.starts_with("sdp.read") || source.starts_with("dlt.read_") {
            return SourceKind::SdpRead;
        }
        if source.starts_with("dp.read") || source.starts_with("dp.read_") {
            return SourceKind::DpRead;
        }
        if source.starts_with("spark.readStream") {
            return SourceKind::SparkReadStream;
        }
        if source.starts_with("spark.read") {
            return SourceKind::SparkRead;
        }
        if source.starts_with("udf.") || source.ends_with("_udf") {
            return SourceKind::Udf;
        }

        SourceKind::Unknown
    }

    pub fn build_chain_context(&self, actions: Vec<ChainAction>) -> ChainContext {
        ChainContext {
            has_limit: actions.contains(&ChainAction::Limit),
            has_select: actions.contains(&ChainAction::Select),
            has_filter: actions.contains(&ChainAction::Filter),
            is_streaming: actions.contains(&ChainAction::ReadStream),
            actions,
            source_tables: Vec::new(),
        }
    }

    pub fn get_bindings(&self) -> &HashMap<String, Binding> {
        &self.bindings
    }

    pub fn get_findings(&self) -> &[Finding] {
        &self.findings
    }
}

impl Default for SemanticModel {
    fn default() -> Self {
        Self::new()
    }
}

pub fn analyze_bindings(source: &str) -> Vec<Binding> {
    let model = SemanticModel::new();
    crate::parse::python::parse_python(source);
    model.get_bindings().values().cloned().collect()
}

pub fn analyze_scope(source: &str) -> Vec<Scope> {
    let _ = source;
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bind_and_shadow() {
        let mut model = SemanticModel::new();
        model.bind("x".to_string(), BindingKind::Assignment, 1);
        model.bind("x".to_string(), BindingKind::Assignment, 5);

        assert!(!model.get_findings().is_empty());
        assert_eq!(model.get_findings()[0].code, "BN003");
    }

    #[test]
    fn test_classify_dlt_read() {
        let model = SemanticModel::new();
        assert!(matches!(
            model.classify_rhs("dlt.read('table')"),
            SourceKind::SdpRead
        ));
        assert!(matches!(
            model.classify_rhs("dp.read_csv('file')"),
            SourceKind::DpRead
        ));
        assert!(matches!(
            model.classify_rhs("spark.read.parquet('path')"),
            SourceKind::SparkRead
        ));
        assert!(matches!(
            model.classify_rhs("spark.readStream.format('kafka')"),
            SourceKind::SparkReadStream
        ));
    }

    #[test]
    fn test_chain_context() {
        let model = SemanticModel::new();
        let ctx = model.build_chain_context(vec![
            ChainAction::Read,
            ChainAction::Filter,
            ChainAction::Select,
            ChainAction::Limit,
        ]);

        assert!(ctx.has_limit);
        assert!(ctx.has_select);
        assert!(ctx.has_filter);
        assert!(!ctx.is_streaming);
    }

    #[test]
    fn test_scope_stack() {
        let mut model = SemanticModel::new();
        assert_eq!(model.scopes.len(), 1);

        model.push_scope("function".to_string());
        assert_eq!(model.scopes.len(), 2);

        model.bind("x".to_string(), BindingKind::Parameter, 1);
        assert!(model
            .scopes
            .last()
            .unwrap()
            .variables
            .contains(&"x".to_string()));

        model.pop_scope();
        assert_eq!(model.scopes.len(), 1);
    }
}
