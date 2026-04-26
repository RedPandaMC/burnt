use crate::parse::notebooks::{parse_file_content, FileFormat};
use crate::types::Cell;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceFile {
    pub path: String,
    pub language: String,
    pub content: String,
    pub cells: Vec<Cell>,
}

pub fn ingest_file(path: &str) -> Result<SourceFile, String> {
    let path_buf = Path::new(path);

    if !path_buf.exists() {
        return Err(format!("File not found: {}", path));
    }

    let content = std::fs::read_to_string(path_buf)
        .map_err(|e| format!("Failed to read file {}: {}", path, e))?;

    let format =
        FileFormat::from_path(path).ok_or_else(|| format!("Unsupported file format: {}", path))?;

    let language = match format {
        FileFormat::PlainPython | FileFormat::DatabricksPython => "python",
        FileFormat::PlainSql | FileFormat::DatabricksSql => "sql",
        FileFormat::DatabricksNotebook => "python",
    }
    .to_string();

    let raw_cells = parse_file_content(&content, &format);

    let cells: Vec<Cell> = raw_cells
        .into_iter()
        .enumerate()
        .map(|(_, (kind, source, line_offset))| Cell {
            kind,
            source,
            byte_offset: 0,
            line_offset,
            origin_path: Some(path_buf.to_path_buf()),
        })
        .collect();

    Ok(SourceFile {
        path: path.to_string(),
        language,
        content,
        cells,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::CellKind;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;

    fn parse_ipynb(content: &str, path: &str) -> Result<ParsedNotebook, String> {
        #[derive(serde::Deserialize)]
        struct NotebookCell {
            cell_type: String,
            source: serde_json::Value,
        }

        #[derive(serde::Deserialize)]
        struct Notebook {
            cells: Vec<NotebookCell>,
        }

        let notebook: Notebook = serde_json::from_str(content)
            .map_err(|e| format!("Failed to parse notebook JSON: {}", e))?;

        let mut cells: Vec<Cell> = Vec::new();
        let mut line_offset: u32 = 0;

        for nb_cell in notebook.cells {
            let source = match nb_cell.source {
                serde_json::Value::Array(arr) => arr
                    .iter()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(""),
                serde_json::Value::String(s) => s,
                _ => String::new(),
            };

            let kind = match nb_cell.cell_type.as_str() {
                "code" => CellKind::Python,
                _ => continue,
            };

            let cell = Cell {
                kind,
                source: source.clone(),
                byte_offset: 0,
                line_offset,
                origin_path: Some(Path::new(path).to_path_buf()),
            };

            line_offset += source.lines().count() as u32 + 1;
            cells.push(cell);
        }

        Ok(ParsedNotebook {
            path: path.to_string(),
            cells,
        })
    }

    struct ParsedNotebook {
        path: String,
        cells: Vec<Cell>,
    }

    #[test]
    fn test_ingest_python_file() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.py");
        fs::write(&test_file, "import pandas as pd\nprint('hello')").unwrap();

        let result = ingest_file(test_file.to_str().unwrap());
        assert!(result.is_ok());

        let sf = result.unwrap();
        assert_eq!(sf.language, "python");
        assert!(!sf.cells.is_empty());
    }

    #[test]
    fn test_ingest_sql_file() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.sql");
        fs::write(&test_file, "SELECT 1").unwrap();

        let result = ingest_file(test_file.to_str().unwrap());
        assert!(result.is_ok());

        let sf = result.unwrap();
        assert_eq!(sf.language, "sql");
    }

    #[test]
    fn test_ingest_missing_file() {
        let result = ingest_file("nonexistent.py");
        assert!(result.is_err());
    }

    #[test]
    fn test_ingest_unsupported_format() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.txt");
        fs::write(&test_file, "hello").unwrap();

        let result = ingest_file(test_file.to_str().unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_ipynb() {
        let content = r###"{
            "cells": [
                {"cell_type": "code", "source": ["print('hello')\n", "print('world')"]},
                {"cell_type": "markdown", "source": ["## Title"]}
            ]
        }"###;

        let result = parse_ipynb(content, "test.ipynb");
        assert!(result.is_ok());

        let nb = result.unwrap();
        assert_eq!(nb.path, "test.ipynb");
        assert_eq!(nb.cells.len(), 1);
        assert_eq!(nb.cells[0].source, "print('hello')\nprint('world')");
    }
}
