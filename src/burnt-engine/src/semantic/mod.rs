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
pub struct SemanticModel {
    pub(crate) scopes: Vec<Scope>,
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

    #[allow(dead_code)]
    pub fn push_scope(&mut self, name: String) {
        let parent = self.scopes.last().map(|s| s.name.clone());
        self.scopes.push(Scope {
            name,
            variables: Vec::new(),
            parent,
        });
    }

    #[allow(dead_code)]
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

    #[allow(dead_code)]
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
