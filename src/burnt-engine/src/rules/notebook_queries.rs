use crate::rules::query::QueryEngine;
use crate::types::{CellKind, Confidence, Finding, Severity};
use std::collections::HashSet;
use std::path::PathBuf;
use tree_sitter::{Node, Tree};

pub struct NotebookQueryEngine {
    query_engine: QueryEngine,
}

#[derive(Debug, Clone)]
pub struct MagicCell {
    pub kind: CellKind,
    pub line: u32,
    pub byte_offset: u32,
}

#[derive(Debug, Clone)]
pub struct RunDirective {
    pub target: String,
    pub line: u32,
    pub byte_offset: u32,
}

impl NotebookQueryEngine {
    pub fn new() -> Self {
        Self {
            query_engine: QueryEngine::new(),
        }
    }

    pub fn detect_magic_cells(&self, source: &str) -> Vec<MagicCell> {
        let mut cells = Vec::new();
        let mut current_kind: Option<CellKind> = None;
        let mut cell_start_line = 0u32;
        let mut cell_start_byte = 0u32;

        for (i, line) in source.lines().enumerate() {
            let line_offset = i as u32;
            let byte_offset = source.lines().take(i).map(|l| l.len() + 1).sum::<usize>() as u32;

            if let Some(kind) = self.classify_magic_line(line) {
                if let Some(current) = current_kind.take() {
                    cells.push(MagicCell {
                        kind: current,
                        line: cell_start_line,
                        byte_offset: cell_start_byte,
                    });
                }
                current_kind = Some(kind);
                cell_start_line = line_offset;
                cell_start_byte = byte_offset;
            }
        }

        if let Some(kind) = current_kind {
            cells.push(MagicCell {
                kind,
                line: cell_start_line,
                byte_offset: cell_start_byte,
            });
        }

        cells
    }

    fn classify_magic_line(&self, line: &str) -> Option<CellKind> {
        let trimmed = line.trim();

        if trimmed == "# MAGIC" || trimmed.starts_with("# MAGIC ") {
            if trimmed.contains("%python") || trimmed.contains("python") {
                Some(CellKind::Python)
            } else if trimmed.contains("%sql") || trimmed.contains("sql") {
                Some(CellKind::Sql)
            } else {
                Some(CellKind::Python)
            }
        } else if trimmed == "# COMMAND"
            || trimmed.starts_with("# COMMAND ")
            || trimmed.starts_with("# Databricks notebook source:")
        {
            Some(CellKind::RunRef)
        } else {
            None
        }
    }

    pub fn find_run_directives(&self, source: &str) -> Vec<RunDirective> {
        let mut directives = Vec::new();
        let mut byte_offset = 0u32;

        for (i, line) in source.lines().enumerate() {
            let line_offset = i as u32;
            let trimmed = line.trim();

            if let Some(target) = self.extract_run_target(trimmed) {
                directives.push(RunDirective {
                    target,
                    line: line_offset,
                    byte_offset,
                });
            }

            byte_offset += line.len() as u32 + 1;
        }

        directives
    }

    fn extract_run_target(&self, line: &str) -> Option<String> {
        let trimmed = line.trim();
        if let Some(stripped) = trimmed.strip_prefix("%run") {
            let rest = stripped.trim();
            if !rest.is_empty() {
                return Some(rest.to_string());
            }
        }
        None
    }

    pub fn detect_circular_runs(
        &self,
        entry_path: &PathBuf,
        visited: &mut HashSet<PathBuf>,
    ) -> Vec<Finding> {
        let mut findings = Vec::new();

        if visited.contains(entry_path) {
            return vec![Finding {
                rule_id: "BN003".to_string(),
                code: "CircularRun".to_string(),
                severity: Severity::Error,
                message: format!("Circular %run reference detected: {}", entry_path.display()),
                suggestion: Some("Remove circular dependency in notebook chain".to_string()),
                line_number: None,
                column: None,
                confidence: Confidence::High,
            }];
        }

        if let Ok(content) = std::fs::read_to_string(entry_path) {
            visited.insert(entry_path.clone());
            let directives = self.find_run_directives(&content);

            for directive in directives {
                let target_path = entry_path
                    .parent()
                    .unwrap_or(std::path::Path::new("."))
                    .join(&directive.target);
                let canonical = std::fs::canonicalize(&target_path).unwrap_or(target_path.clone());

                if canonical.exists() {
                    findings.extend(self.detect_circular_runs(&canonical, visited));
                }
            }

            visited.remove(entry_path);
        }

        findings
    }

    pub fn check_missing_run_targets(&self, source: &str, base_path: &PathBuf) -> Vec<Finding> {
        let mut findings = Vec::new();
        let directives = self.find_run_directives(source);

        for directive in directives {
            let target_path = base_path
                .parent()
                .unwrap_or(std::path::Path::new("."))
                .join(&directive.target);

            if !target_path.exists() {
                findings.push(Finding {
                    rule_id: "BN001".to_string(),
                    code: "MissingRunTarget".to_string(),
                    severity: Severity::Error,
                    message: format!("Missing %run target file: {}", directive.target),
                    suggestion: Some("Ensure the referenced file exists".to_string()),
                    line_number: Some(directive.line + 1),
                    column: None,
                    confidence: Confidence::High,
                });
            }
        }

        findings
    }
}

impl Default for NotebookQueryEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_magic_python() {
        let engine = NotebookQueryEngine::new();
        assert_eq!(
            engine.classify_magic_line("# MAGIC %python"),
            Some(CellKind::Python)
        );
        assert_eq!(
            engine.classify_magic_line("# MAGIC python"),
            Some(CellKind::Python)
        );
        assert_eq!(
            engine.classify_magic_line("# MAGIC"),
            Some(CellKind::Python)
        );
    }

    #[test]
    fn test_classify_magic_sql() {
        let engine = NotebookQueryEngine::new();
        assert_eq!(
            engine.classify_magic_line("# MAGIC %sql"),
            Some(CellKind::Sql)
        );
        assert_eq!(
            engine.classify_magic_line("# MAGIC sql"),
            Some(CellKind::Sql)
        );
    }

    #[test]
    fn test_classify_magic_runref() {
        let engine = NotebookQueryEngine::new();
        assert_eq!(
            engine.classify_magic_line("# COMMAND ----------"),
            Some(CellKind::RunRef)
        );
    }

    #[test]
    fn test_extract_run_target() {
        let engine = NotebookQueryEngine::new();
        assert_eq!(
            engine.extract_run_target("%run ./other.py"),
            Some("./other.py".to_string())
        );
        assert_eq!(
            engine.extract_run_target("  %run  ./other.py"),
            Some("./other.py".to_string())
        );
        assert_eq!(engine.extract_run_target("import pandas"), None);
    }

    #[test]
    fn test_find_run_directives() {
        let engine = NotebookQueryEngine::new();
        let source = "%run ./notebook1.py\ndf.collect()\n%run ./notebook2.py";
        let directives = engine.find_run_directives(source);

        assert_eq!(directives.len(), 2);
        assert_eq!(directives[0].target, "./notebook1.py");
        assert_eq!(directives[1].target, "./notebook2.py");
    }

    #[test]
    fn test_detect_magic_cells() {
        let engine = NotebookQueryEngine::new();
        let source = "# MAGIC %python\nimport pandas as pd\n# MAGIC %sql\nSELECT 1";
        let cells = engine.detect_magic_cells(source);

        assert_eq!(cells.len(), 2);
        assert_eq!(cells[0].kind, CellKind::Python);
        assert_eq!(cells[1].kind, CellKind::Sql);
    }
}
