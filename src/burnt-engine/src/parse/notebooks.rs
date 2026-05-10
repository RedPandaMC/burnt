use crate::types::CellKind;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
        let _line_byte_offset = current_byte_offset;
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
