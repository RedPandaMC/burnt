use crate::types::{CellKind, Confidence, Finding, Severity};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const RUN_DIRECTIVE: &str = "%run";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotebookCell {
    pub cell_type: String,
    pub source: String,
    pub line_offset: u32,
    pub byte_offset: u32,
}

pub fn parse_notebook(path: &str) -> Vec<NotebookCell> {
    let _ = path;
    vec![]
}

pub fn detect_language(cells: &[NotebookCell]) -> String {
    let sql_cells = cells.iter().filter(|c| c.cell_type == "sql").count();
    let python_cells = cells.iter().filter(|c| c.cell_type == "python").count();

    if sql_cells > 0 && python_cells == 0 {
        "sql".to_string()
    } else if sql_cells > 0 {
        "mixed".to_string()
    } else {
        "python".to_string()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FileFormat {
    DatabricksPython,
    PlainPython,
    PlainSql,
    DatabricksNotebook,
    DatabricksSql,
}

impl FileFormat {
    pub fn from_path(path: &str) -> Option<Self> {
        let path_lower = path.to_lowercase();

        if path_lower.ends_with(".py") {
            if path_lower.contains("databricks") || path_lower.contains("_databricks") {
                Some(FileFormat::DatabricksPython)
            } else {
                Some(FileFormat::PlainPython)
            }
        } else if path_lower.ends_with(".sql") || path_lower.ends_with(".dbsql") {
            if path_lower.contains("databricks") || path_lower.ends_with(".dbsql") {
                Some(FileFormat::DatabricksSql)
            } else {
                Some(FileFormat::PlainSql)
            }
        } else if path_lower.ends_with(".ipynb") {
            Some(FileFormat::DatabricksNotebook)
        } else {
            None
        }
    }
}

pub fn classify_magic(line: &str) -> Option<CellKind> {
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

pub fn parse_file_content(content: &str, format: &FileFormat) -> Vec<(CellKind, String, u32)> {
    let mut cells: Vec<(CellKind, String, u32)> = Vec::new();
    let mut current_cell_kind: Option<CellKind> = None;
    let mut current_cell_lines: Vec<String> = Vec::new();
    let mut current_byte_offset: u32 = 0;
    let mut line_offset: u32 = 0;

    for line in content.lines() {
        let line_byte_offset = current_byte_offset;
        current_byte_offset += line.len() as u32 + 1;

        if let Some(kind) = classify_magic(line) {
            if !current_cell_lines.is_empty() {
                if let Some(cell_kind) = current_cell_kind.take() {
                    cells.push((cell_kind, current_cell_lines.join("\n"), line_offset));
                }
                current_cell_lines = Vec::new();
            }
            current_cell_kind = Some(kind);
            continue;
        }

        match format {
            FileFormat::DatabricksPython | FileFormat::DatabricksNotebook => {
                if current_cell_kind.is_none() {
                    current_cell_kind = Some(CellKind::Python);
                }
            }
            FileFormat::PlainPython => {
                current_cell_kind = Some(CellKind::Python);
            }
            FileFormat::PlainSql | FileFormat::DatabricksSql => {
                current_cell_kind = Some(CellKind::Sql);
            }
        }

        current_cell_lines.push(line.to_string());
        line_offset += 1;
    }

    if !current_cell_lines.is_empty() {
        if let Some(cell_kind) = current_cell_kind {
            cells.push((cell_kind, current_cell_lines.join("\n"), line_offset));
        }
    }

    cells
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_magic_python() {
        assert_eq!(classify_magic("# MAGIC %python"), Some(CellKind::Python));
        assert_eq!(classify_magic("# MAGIC python"), Some(CellKind::Python));
    }

    #[test]
    fn test_classify_magic_sql() {
        assert_eq!(classify_magic("# MAGIC %sql"), Some(CellKind::Sql));
        assert_eq!(classify_magic("# MAGIC sql"), Some(CellKind::Sql));
    }

    #[test]
    fn test_classify_magic_runref() {
        assert_eq!(
            classify_magic("# COMMAND ----------"),
            Some(CellKind::RunRef)
        );
    }

    #[test]
    fn test_file_format_detection() {
        assert_eq!(
            FileFormat::from_path("notebook.py"),
            Some(FileFormat::PlainPython)
        );
        assert_eq!(
            FileFormat::from_path("notebook_databricks.py"),
            Some(FileFormat::DatabricksPython)
        );
        assert_eq!(
            FileFormat::from_path("query.sql"),
            Some(FileFormat::PlainSql)
        );
        assert_eq!(
            FileFormat::from_path("query.DBSQL"),
            Some(FileFormat::DatabricksSql)
        );
        assert_eq!(
            FileFormat::from_path("notebook.ipynb"),
            Some(FileFormat::DatabricksNotebook)
        );
    }

    #[test]
    fn test_parse_plain_python() {
        let content = "import pandas as pd\nprint('hello')";
        let result = parse_file_content(content, &FileFormat::PlainPython);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, CellKind::Python);
    }

    #[test]
    fn test_parse_with_magic() {
        let content = "# MAGIC %python\nimport pandas as pd\n# MAGIC %sql\nSELECT 1";
        let result = parse_file_content(content, &FileFormat::DatabricksNotebook);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, CellKind::Python);
        assert_eq!(result[1].0, CellKind::Sql);
    }
}

pub fn find_run_directive(line: &str) -> Option<String> {
    let trimmed = line.trim();
    if trimmed.starts_with(RUN_DIRECTIVE) {
        let rest = trimmed[RUN_DIRECTIVE.len()..].trim();
        if !rest.is_empty() {
            return Some(rest.to_string());
        }
    }
    None
}

#[derive(Debug, Clone)]
struct ResolutionContext {
    root: PathBuf,
    visited: std::collections::HashSet<PathBuf>,
}

impl ResolutionContext {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            visited: std::collections::HashSet::new(),
        }
    }

    fn is_visited(&self, path: &Path) -> bool {
        self.visited.contains(path)
    }

    fn mark_visited(&mut self, path: &Path) {
        self.visited.insert(path.to_path_buf());
    }
}

pub fn parse_and_resolve(
    path: &str,
    root: Option<&str>,
) -> Result<(Vec<(CellKind, String, u32, Option<PathBuf>)>, Vec<Finding>), String> {
    let root_path = match root {
        Some(r) => PathBuf::from(r),
        None => PathBuf::from("."),
    };

    let target_path = PathBuf::from(path);
    let canonical_path = if target_path.is_absolute() {
        target_path.clone()
    } else {
        root_path.join(&target_path)
    };

    let mut ctx = ResolutionContext::new(root_path);
    resolve_file(&canonical_path, &mut ctx)
}

fn resolve_file(
    path: &Path,
    ctx: &mut ResolutionContext,
) -> Result<(Vec<(CellKind, String, u32, Option<PathBuf>)>, Vec<Finding>), String> {
    if ctx.is_visited(path) {
        let finding = Finding {
            rule_id: "BN003".to_string(),
            code: "CircularRun".to_string(),
            severity: Severity::Error,
            message: format!("Circular %run reference detected: {}", path.display()),
            suggestion: Some("Remove circular dependency in notebook chain".to_string()),
            line_number: None,
            column: None,
            confidence: Confidence::High,
        };
        return Ok((vec![], vec![finding]));
    }

    ctx.mark_visited(path);

    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) => {
            let finding = Finding {
                rule_id: "BN001".to_string(),
                code: "MissingRunTarget".to_string(),
                severity: Severity::Error,
                message: format!("Missing %run target file: {} - {}", path.display(), e),
                suggestion: Some("Ensure the referenced file exists".to_string()),
                line_number: None,
                column: None,
                confidence: Confidence::High,
            };
            return Ok((vec![], vec![finding]));
        }
    };

    let path_str = path.to_string_lossy().to_string();
    let format = FileFormat::from_path(&path_str).unwrap_or(FileFormat::PlainPython);

    let cells = parse_file_content(&content, &format);

    let mut all_cells: Vec<(CellKind, String, u32, Option<PathBuf>)> = Vec::new();
    let mut all_findings: Vec<Finding> = Vec::new();

    let mut line_number: u32 = 0;

    for (kind, source, cell_line_offset) in cells {
        match kind {
            CellKind::RunRef => {
                if let Some(target) = find_run_directive(&source) {
                    let target_path = if Path::new(&target).is_absolute() {
                        PathBuf::from(&target)
                    } else {
                        ctx.root.join(&target)
                    };

                    match resolve_file(&target_path, ctx) {
                        Ok((mut nested_cells, mut nested_findings)) => {
                            let origin = Some(path.to_path_buf());
                            for cell in &mut nested_cells {
                                cell.3 = cell.3.clone().or(origin.clone());
                            }
                            all_cells.append(&mut nested_cells);
                            all_findings.append(&mut nested_findings);
                        }
                        Err(e) => {
                            let finding = Finding {
                                rule_id: "BN001".to_string(),
                                code: "MissingRunTarget".to_string(),
                                severity: Severity::Error,
                                message: e,
                                suggestion: Some("Ensure the referenced file exists".to_string()),
                                line_number: Some(line_number + cell_line_offset),
                                column: None,
                                confidence: Confidence::High,
                            };
                            all_findings.push(finding);
                        }
                    }
                }
            }
            _ => {
                all_cells.push((kind, source.clone(), line_number, Some(path.to_path_buf())));
                line_number += source.lines().count() as u32 + 1;
            }
        }
    }

    Ok((all_cells, all_findings))
}

#[cfg(test)]
mod run_resolution_tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_find_run_directive() {
        assert_eq!(
            find_run_directive("%run ./other.py"),
            Some("./other.py".to_string())
        );
        assert_eq!(
            find_run_directive("  %run  ./other.py"),
            Some("./other.py".to_string())
        );
        assert_eq!(find_run_directive("import pandas"), None);
    }

    #[test]
    fn test_missing_file() {
        let result = parse_and_resolve("nonexistent.py", None);
        assert!(result.is_ok());
        let (_, findings) = result.unwrap();
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].rule_id, "BN001");
    }

    #[test]
    fn test_simple_file() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.py");
        fs::write(&test_file, "import pandas as pd\nprint('hello')").unwrap();

        let result = parse_and_resolve(test_file.to_str().unwrap(), None);
        assert!(result.is_ok());
        let (cells, findings) = result.unwrap();
        assert_eq!(findings.len(), 0);
        assert!(!cells.is_empty());
    }
}
