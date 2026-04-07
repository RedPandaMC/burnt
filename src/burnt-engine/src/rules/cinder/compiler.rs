#![allow(dead_code)]
//! Cinder Compiler - Transforms CPL patterns to tree-sitter S-expressions
//!
//! This is the core of Cinder. It takes human-readable code patterns like:
//!   "$DF.collect()"
//!   "$DF.limit($N).collect()"
//!   "SELECT * FROM $TABLE"
//!
//! And compiles them into tree-sitter S-expressions that the query engine can execute.

use thiserror::Error;
use tree_sitter::{Language as TreeSitterLanguage, Parser};
use tree_sitter_python::LANGUAGE as PYTHON_LANGUAGE;
use tree_sitter_sequel::LANGUAGE as SQL_LANGUAGE;

#[derive(Error, Debug)]
pub enum CompileError {
    #[error("Failed to parse pattern as {0}: {1}")]
    ParseError(String, String),
    #[error("Unsupported language: {0}")]
    UnsupportedLanguage(String),
    #[error("Invalid pattern: {0}")]
    InvalidPattern(String),
}

pub struct CinderCompiler {
    python_lang: TreeSitterLanguage,
    sql_lang: TreeSitterLanguage,
}

impl CinderCompiler {
    pub fn new() -> Self {
        Self {
            python_lang: PYTHON_LANGUAGE.into(),
            sql_lang: SQL_LANGUAGE.into(),
        }
    }

    pub fn compile(&self, pattern: &str, language: &str) -> Result<String, CompileError> {
        let lang = match language.to_lowercase().as_str() {
            "python" | "pyspark" => &self.python_lang,
            "sql" => &self.sql_lang,
            _ => return Err(CompileError::UnsupportedLanguage(language.to_string())),
        };

        let mut parser = Parser::new();
        parser
            .set_language(lang)
            .map_err(|e| CompileError::ParseError(language.to_string(), e.to_string()))?;

        let tree = parser
            .parse(pattern, None)
            .ok_or_else(|| CompileError::InvalidPattern("Failed to parse pattern".to_string()))?;

        let root = tree.root_node();
        self.ast_to_s_expression(root, pattern)
    }

    fn ast_to_s_expression(
        &self,
        node: tree_sitter::Node,
        source: &str,
    ) -> Result<String, CompileError> {
        let kind = node.kind();

        match kind {
            "identifier" => {
                let text = self.get_node_text(node, source);
                if let Some(rest) = text.strip_prefix('$') {
                    if let Some(literal_value) = text.strip_prefix("$:") {
                        format!("(@{})", literal_value).parse().map_err(|_| {
                            CompileError::InvalidPattern("Invalid literal".to_string())
                        })
                    } else {
                        Ok(format!("(@{})", rest))
                    }
                } else {
                    Ok("(identifier)".to_string())
                }
            }
            "string" | "integer" | "float" => Ok(self.literal_to_s_expression(node, source)),
            _ => self.node_to_s_expression(node, source),
        }
    }

    fn node_to_s_expression(
        &self,
        node: tree_sitter::Node,
        source: &str,
    ) -> Result<String, CompileError> {
        let kind = node.kind();
        let mut parts = vec![format!("({}", kind)];

        let child_count = node.child_count();

        for i in 0..child_count {
            if let Some(child) = node.child(i) {
                let child_kind = child.kind();
                if child_kind == ","
                    || child_kind == "\n"
                    || child_kind == "\""
                    || child_kind == "'"
                {
                    continue;
                }

                if let Ok(child_sexp) = self.ast_to_s_expression(child, source) {
                    parts.push(format!(" {}", child_sexp));
                }
            }
        }

        if kind == "call" {
            parts.push(self.capture_call_function(node, source)?);
        }

        parts.push(")".to_string());
        Ok(parts.join(""))
    }

    fn capture_call_function(
        &self,
        call_node: tree_sitter::Node,
        source: &str,
    ) -> Result<String, CompileError> {
        let mut func_part = String::new();

        for child in call_node.children(&mut call_node.walk()) {
            let child_kind = child.kind();
            if child_kind == "function" {
                func_part = self.ast_to_s_expression(child, source)?;
                break;
            }
        }

        Ok(format!(" {}", func_part))
    }

    fn get_node_text(&self, node: tree_sitter::Node, source: &str) -> String {
        let range = node.byte_range();
        source[range].to_string()
    }

    fn literal_to_s_expression(&self, node: tree_sitter::Node, source: &str) -> String {
        let text = self.get_node_text(node, source);
        if let Some(rest) = text.strip_prefix('$') {
            format!("(@{})", rest)
        } else {
            "(string)".to_string()
        }
    }

    pub fn compile_with_captures(
        &self,
        pattern: &str,
        language: &str,
    ) -> Result<(String, Vec<String>), CompileError> {
        let compiled = self.compile(pattern, language)?;
        let captures = self.extract_captures(pattern);
        Ok((compiled, captures))
    }

    fn extract_captures(&self, pattern: &str) -> Vec<String> {
        let mut captures = Vec::new();
        let mut chars = pattern.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '$' {
                let mut var_name = String::new();
                while let Some(&next) = chars.peek() {
                    if next.is_alphanumeric() || next == '_' {
                        var_name.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
                if !var_name.is_empty() {
                    captures.push(var_name);
                }
            }
        }

        captures
    }
}

impl Default for CinderCompiler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compile_python_collect() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("$DF.collect()", "python");
        assert!(result.is_ok());
        println!("Compiled: {:?}", result.unwrap());
    }

    #[test]
    fn test_compile_sql_select_star() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("SELECT * FROM $TABLE", "sql");
        assert!(result.is_ok());
        println!("Compiled: {:?}", result.unwrap());
    }

    #[test]
    fn test_extract_captures() {
        let compiler = CinderCompiler::new();
        let (_, captures) = compiler
            .compile_with_captures("$DF.limit($N).collect()", "python")
            .unwrap();
        assert_eq!(captures, vec!["DF", "N"]);
    }
}
