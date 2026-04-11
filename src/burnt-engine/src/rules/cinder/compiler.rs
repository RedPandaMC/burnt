//! Cinder Pattern Language (CPL) Compiler
//!
//! Transforms human-readable CPL patterns into tree-sitter S-expressions.
//!
//! # CPL Syntax
//!
//! ## Captures
//! `$variable_name` - captures a tree-sitter node and binds it to `variable_name`
//!
//! ## Predicates
//! `$variable == "value"` - emits `(#eq? @variable "value")`
//! `$variable != "value"` - emits `(#not-eq? @variable "value")`
//! `$variable =~ "regex"` - emits `(#match? @variable "regex")`
//! `$n > 0` - emits `(#gt? @n "0")`
//! `$n < 10` - emits `(#lt? @n "10")`
//! `$n >= 0` - emits `(#gte? @n "0")`
//! `$n <= 100` - emits `(#lte? @n "100")`
//!
//! ## Examples
//! ```
//! $df.collect()                          // Python method chain
//! SELECT * FROM $tbl                     // SQL select
//! $df.collect() $method == "collect"    // With predicate (method auto-captured)
//! $cmd =~ "^(run|sql)$"                  // Regex predicate
//! $n > 0                                  // Numeric comparison
//! ```

use regex::Regex;
use std::collections::HashSet;
use thiserror::Error;
use tree_sitter::{Language as TreeSitterLanguage, Parser};
use tree_sitter_python::LANGUAGE as PYTHON_LANGUAGE;
use tree_sitter_sequel::LANGUAGE as SQL_LANGUAGE;

const CPL_CAPTURE_PREFIX: &str = "__cpl_";

#[derive(Error, Debug)]
pub enum CompileError {
    #[error("Failed to parse pattern as {0}: {1}")]
    ParseError(String, String),
    #[error("Unsupported language: {0}")]
    UnsupportedLanguage(String),
    #[error("Invalid pattern: {0}")]
    InvalidPattern(String),
    #[error("Predicate variable '{0}' is not captured in the pattern")]
    UndefinedPredicateVariable(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    Eq(String, String),
    NotEq(String, String),
    Match(String, String),
    Gt(String, String),
    Lt(String, String),
    Gte(String, String),
    Lte(String, String),
}

#[derive(Debug, Clone)]
pub struct CompiledCplPattern {
    pub sexp: String,
    pub captures: Vec<String>,
    pub predicates: Vec<Predicate>,
}

pub struct CinderCompiler {
    python_lang: TreeSitterLanguage,
    sql_lang: TreeSitterLanguage,
    string_predicate_regex: Regex,
    numeric_predicate_regex: Regex,
}

impl CinderCompiler {
    pub fn new() -> Self {
        Self {
            python_lang: PYTHON_LANGUAGE.into(),
            sql_lang: SQL_LANGUAGE.into(),
            string_predicate_regex: Regex::new(
                r#"\$([a-zA-Z_][a-zA-Z0-9_]*)\s*(==|!=|=~)\s*"([^"]*)""#,
            )
            .unwrap(),
            numeric_predicate_regex: Regex::new(
                r#"\$([a-zA-Z_][a-zA-Z0-9_]*)\s*(>|>=|<|<=)\s*(\d+)"#,
            )
            .unwrap(),
        }
    }

    fn extract_captures(pattern: &str) -> Vec<String> {
        let mut captures = Vec::new();
        let bytes = pattern.as_bytes();
        let mut i = 0;

        while i < bytes.len() {
            if bytes[i] == b'$'
                && i + 1 < bytes.len()
                && (bytes[i + 1].is_ascii_alphabetic() || bytes[i + 1] == b'_')
            {
                i += 1;
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                let var_name = pattern[start..i].to_string();
                if !captures.contains(&var_name) {
                    captures.push(var_name);
                }
            } else {
                i += 1;
            }
        }

        captures
    }

    fn extract_predicate_vars(&self, pattern: &str) -> HashSet<String> {
        let mut vars = HashSet::new();
        for cap in self.string_predicate_regex.captures_iter(pattern) {
            if let Some(m) = cap.get(1) {
                vars.insert(m.as_str().to_string());
            }
        }
        for cap in self.numeric_predicate_regex.captures_iter(pattern) {
            if let Some(m) = cap.get(1) {
                vars.insert(m.as_str().to_string());
            }
        }
        vars
    }

    fn extract_predicates(&self, pattern: &str) -> Vec<Predicate> {
        let mut predicates = Vec::new();

        for cap in self.string_predicate_regex.captures_iter(pattern) {
            let var_name = cap.get(1).unwrap().as_str().to_string();
            let op = cap.get(2).unwrap().as_str();
            let value = cap.get(3).unwrap().as_str().to_string();

            let predicate = match op {
                "==" => Predicate::Eq(var_name, value),
                "!=" => Predicate::NotEq(var_name, value),
                "=~" => Predicate::Match(var_name, value),
                _ => continue,
            };
            predicates.push(predicate);
        }

        for cap in self.numeric_predicate_regex.captures_iter(pattern) {
            let var_name = cap.get(1).unwrap().as_str().to_string();
            let op = cap.get(2).unwrap().as_str();
            let value = cap.get(3).unwrap().as_str().to_string();

            let predicate = match op {
                ">" => Predicate::Gt(var_name, value),
                ">=" => Predicate::Gte(var_name, value),
                "<" => Predicate::Lt(var_name, value),
                "<=" => Predicate::Lte(var_name, value),
                _ => continue,
            };
            predicates.push(predicate);
        }

        predicates
    }

    fn predicates_to_sexp(&self, predicates: &[Predicate]) -> String {
        let mut parts = Vec::new();

        for predicate in predicates {
            let sexp = match predicate {
                Predicate::Eq(var, value) => format!("(#eq? @{} \"{}\")", var, value),
                Predicate::NotEq(var, value) => format!("(#not-eq? @{} \"{}\")", var, value),
                Predicate::Match(var, pattern) => format!("(#match? @{} \"{}\")", var, pattern),
                Predicate::Gt(var, value) => format!("(#gt? @{} \"{}\")", var, value),
                Predicate::Lt(var, value) => format!("(#lt? @{} \"{}\")", var, value),
                Predicate::Gte(var, value) => format!("(#gte? @{} \"{}\")", var, value),
                Predicate::Lte(var, value) => format!("(#lte? @{} \"{}\")", var, value),
            };
            parts.push(format!(" {}", sexp));
        }

        parts.join("")
    }

    fn replace_cpl_captures(pattern: &str, captures: &[String]) -> String {
        let mut result = String::new();
        let bytes = pattern.as_bytes();
        let mut i = 0;

        while i < bytes.len() {
            if bytes[i] == b'$'
                && i + 1 < bytes.len()
                && (bytes[i + 1].is_ascii_alphabetic() || bytes[i + 1] == b'_')
            {
                i += 1;
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                let var_name = pattern[start..i].to_string();
                if let Some(idx) = captures.iter().position(|c| c == &var_name) {
                    result.push_str(&format!("{}{}", CPL_CAPTURE_PREFIX, idx));
                } else {
                    result.push_str(&pattern[start..i]);
                }
            } else if bytes[i] == b'_'
                && (i == 0 || !bytes[i - 1].is_ascii_alphanumeric())
                && (i + 1 >= bytes.len() || !bytes[i + 1].is_ascii_alphanumeric())
            {
                result.push('_');
                i += 1;
            } else {
                result.push(bytes[i] as char);
                i += 1;
            }
        }

        result
    }

    fn remove_predicates_from_pattern(pattern: &str, predicates: &[Predicate]) -> String {
        let mut result = pattern.to_string();

        for predicate in predicates {
            let search = match predicate {
                Predicate::Eq(v, val) => format!("${} == \"{}\"", v, val),
                Predicate::NotEq(v, val) => format!("${} != \"{}\"", v, val),
                Predicate::Match(v, val) => format!("${} =~ \"{}\"", v, val),
                Predicate::Gt(v, val) => format!("${} > {}", v, val),
                Predicate::Lt(v, val) => format!("${} < {}", v, val),
                Predicate::Gte(v, val) => format!("${} >= {}", v, val),
                Predicate::Lte(v, val) => format!("${} <= {}", v, val),
            };

            result = result.replace(&search, "");
        }

        result.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    pub fn compile(&self, pattern: &str, language: &str) -> Result<String, CompileError> {
        let compiled = self.compile_with_captures(pattern, language)?;
        Ok(compiled.sexp)
    }

    pub fn compile_with_captures(
        &self,
        pattern: &str,
        language: &str,
    ) -> Result<CompiledCplPattern, CompileError> {
        let mut captures = Self::extract_captures(pattern);
        let predicates = self.extract_predicates(pattern);

        let predicate_vars = self.extract_predicate_vars(pattern);
        for var in predicate_vars {
            if !captures.contains(&var) {
                captures.push(var);
            }
        }

        let clean_pattern = Self::remove_predicates_from_pattern(pattern, &predicates);

        let lang = match language.to_lowercase().as_str() {
            "python" | "pyspark" => &self.python_lang,
            "sql" => &self.sql_lang,
            _ => return Err(CompileError::UnsupportedLanguage(language.to_string())),
        };

        let processed = Self::replace_cpl_captures(&clean_pattern, &captures);

        let mut parser = Parser::new();
        parser
            .set_language(lang)
            .map_err(|e| CompileError::ParseError(language.to_string(), e.to_string()))?;

        let tree = parser
            .parse(&processed, None)
            .ok_or_else(|| CompileError::InvalidPattern("Failed to parse pattern".to_string()))?;

        let captures_set: HashSet<String> = captures.iter().cloned().collect();
        let root = tree.root_node();
        let mut sexp = self.ast_to_sexp_with_captures(root, &processed, &captures_set, 0, false)?;

        for (idx, _) in captures.iter().enumerate() {
            let from = format!("@{}{}", CPL_CAPTURE_PREFIX, idx);
            let to = format!("@{}", captures[idx]);
            sexp = sexp.replace(&from, &to);
        }

        let predicate_sexp = self.predicates_to_sexp(&predicates);
        if !predicate_sexp.is_empty() {
            sexp = format!("{}{})", &sexp[..sexp.len() - 1], predicate_sexp);
        }

        Ok(CompiledCplPattern {
            sexp,
            captures,
            predicates,
        })
    }

    fn ast_to_sexp(&self, node: tree_sitter::Node, source: &str) -> Result<String, CompileError> {
        self.ast_to_sexp_with_captures(node, source, &HashSet::new(), 0, false)
    }

    fn ast_to_sexp_with_captures(
        &self,
        node: tree_sitter::Node,
        source: &str,
        extra_captures: &HashSet<String>,
        depth: usize,
        is_function: bool,
    ) -> Result<String, CompileError> {
        let kind = node.kind();

        match kind {
            "identifier" => {
                let text = self.node_text(node, source);
                if text.starts_with(CPL_CAPTURE_PREFIX) {
                    let var_name = text.strip_prefix(CPL_CAPTURE_PREFIX).unwrap();
                    if let Some(idx) = var_name.parse::<usize>().ok() {
                        Ok(format!("(identifier) @__cpl_{}", idx))
                    } else {
                        Ok(format!("(identifier) @{}", text))
                    }
                } else {
                    Ok("(identifier)".to_string())
                }
            }
            "attribute" => self.attribute_to_sexp(node, source, extra_captures, depth, is_function),
            "string" | "integer" | "float" => Ok(self.literal_to_sexp(node, source)),
            _ => self.node_to_sexp_with_captures(node, source, extra_captures, depth),
        }
    }

    fn attribute_to_sexp(
        &self,
        node: tree_sitter::Node,
        source: &str,
        extra_captures: &HashSet<String>,
        depth: usize,
        is_function: bool,
    ) -> Result<String, CompileError> {
        let mut parts = vec!["(attribute".to_string()];
        let mut identifiers: Vec<(tree_sitter::Node, String)> = Vec::new();
        let mut has_call_child = false;

        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                let child_kind = child.kind();
                if child_kind == "." {
                    continue;
                }
                if child_kind == "\n" || child_kind == "\"" || child_kind == "'" {
                    continue;
                }

                if child_kind == "identifier" {
                    let text = self.node_text(child, source);
                    identifiers.push((child, text));
                } else {
                    if child_kind == "call" {
                        has_call_child = true;
                    }
                    let text = self.node_text(child, source);
                    if text.starts_with(CPL_CAPTURE_PREFIX) {
                        parts.push(format!(
                            " {}",
                            self.ast_to_sexp_with_captures(
                                child,
                                source,
                                extra_captures,
                                depth,
                                false
                            )?
                        ));
                    } else if extra_captures.contains(&text) {
                        let child_sexp = self.ast_to_sexp_with_captures(
                            child,
                            source,
                            extra_captures,
                            depth,
                            false,
                        )?;
                        parts.push(format!(" {}", child_sexp));
                    } else if let Ok(child_sexp) =
                        self.ast_to_sexp_with_captures(child, source, extra_captures, depth, false)
                    {
                        parts.push(format!(" {}", child_sexp));
                    }
                }
            }
        }

        for (i, (child, text)) in identifiers.iter().enumerate() {
            let is_last = i == identifiers.len() - 1;
            if text.starts_with(CPL_CAPTURE_PREFIX) {
                let var_name = text.strip_prefix(CPL_CAPTURE_PREFIX).unwrap();
                parts.push(format!(" (identifier) @__cpl_{}", var_name));
            } else if extra_captures.contains(text) {
                parts.push(format!(" (identifier) @{}", text));
            } else if is_last && is_function {
                if !has_call_child {
                    parts.push(" (identifier) @method".to_string());
                } else if extra_captures.contains("parent_method") {
                    parts.push(" (identifier) @parent_method".to_string());
                } else {
                    parts.push(" (identifier)".to_string());
                }
            } else {
                parts.push(" (identifier)".to_string());
            }
        }

        parts.push(")".to_string());
        Ok(parts.join(""))
    }

    fn node_to_sexp_with_captures(
        &self,
        node: tree_sitter::Node,
        source: &str,
        extra_captures: &HashSet<String>,
        depth: usize,
    ) -> Result<String, CompileError> {
        let kind = node.kind();
        let mut parts = vec![format!("({}", kind)];

        if kind == "call" {
            if let Ok(func_sexp) = self.capture_function_part(node, source, extra_captures, depth) {
                if !func_sexp.is_empty() {
                    parts.push(format!(" {}", func_sexp));
                }
            }
        }

        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                let child_kind = child.kind();
                if child_kind == ","
                    || child_kind == "\n"
                    || child_kind == "\""
                    || child_kind == "'"
                    || child_kind == ")"
                    || child_kind == "("
                {
                    continue;
                }
                if child_kind == "argument_list" && child.child_count() == 0 {
                    continue;
                }
                if kind == "call" && (child_kind == "attribute" || child_kind == "identifier") {
                    continue;
                }
                let child_depth = if kind == "attribute" && child_kind == "call" {
                    depth + 1
                } else {
                    depth
                };
                if let Ok(child_sexp) = self.ast_to_sexp_with_captures(
                    child,
                    source,
                    extra_captures,
                    child_depth,
                    false,
                ) {
                    parts.push(format!(" {}", child_sexp));
                }
            }
        }

        parts.push(")".to_string());
        Ok(parts.join(""))
    }

    fn capture_function_part(
        &self,
        call_node: tree_sitter::Node,
        source: &str,
        extra_captures: &HashSet<String>,
        depth: usize,
    ) -> Result<String, CompileError> {
        for child in call_node.children(&mut call_node.walk()) {
            if child.kind() == "attribute" || child.kind() == "identifier" {
                return self.ast_to_sexp_with_captures(
                    child,
                    source,
                    extra_captures,
                    depth + 1,
                    true,
                );
            }
        }
        Ok(String::new())
    }

    fn node_text(&self, node: tree_sitter::Node, source: &str) -> String {
        let range = node.byte_range();
        source[range].to_string()
    }

    fn literal_to_sexp(&self, node: tree_sitter::Node, source: &str) -> String {
        let text = self.node_text(node, source);
        if text.starts_with(CPL_CAPTURE_PREFIX) {
            format!("(@{})", text)
        } else {
            "(string)".to_string()
        }
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
    fn test_compile_python_attribute_chain() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("$df.collect()", "python");
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Compiled: {}", compiled);
        assert!(compiled.contains("@df"));
        assert!(compiled.contains("(attribute"));
    }

    #[test]
    fn test_compile_with_wildcard() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("df.limit($n)", "python");
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Compiled: {}", compiled);
        assert!(compiled.contains("@n"));
        assert!(compiled.contains("(attribute"));
    }

    #[test]
    fn test_compile_sql_select() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("SELECT * FROM $tbl", "sql");
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Compiled: {}", compiled);
        assert!(compiled.contains("@tbl"));
    }

    #[test]
    fn test_captures_extraction() {
        let compiler = CinderCompiler::new();
        let result = compiler
            .compile_with_captures("$df.limit($n)", "python")
            .unwrap();
        assert_eq!(result.captures, vec!["df", "n"]);
    }

    #[test]
    fn test_predicate_eq() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("$df.collect() $method == \"collect\"", "python");
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Compiled with predicate: {}", compiled);
        assert!(compiled.contains("@df"));
        assert!(compiled.contains("(attribute"));
        assert!(compiled.contains("@method"));
        assert!(compiled.contains("(#eq? @method \"collect\")"));
    }

    #[test]
    fn test_predicate_match() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("$cmd =~ \"^(run|sql)$\"", "python");
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Compiled with regex: {}", compiled);
        assert!(compiled.contains("@cmd"));
        assert!(compiled.contains("(#match? @cmd \"^(run|sql)$\")"));
    }

    #[test]
    fn test_predicate_not_eq() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("$method != \"forbidden\"", "python");
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Compiled with not-eq: {}", compiled);
        assert!(compiled.contains("@method"));
        assert!(compiled.contains("(#not-eq? @method \"forbidden\")"));
    }

    #[test]
    fn test_multiple_predicates() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("$df.collect() $method == \"collect\" $n == \"1\"", "python");
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Compiled with multiple predicates: {}", compiled);
        assert!(compiled.contains("(#eq? @method \"collect\")"));
        assert!(compiled.contains("(#eq? @n \"1\")"));
    }

    #[test]
    fn test_predicate_with_regex_in_pattern() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile(
            "$df.filter($col) $filter_method =~ \"^(filter|where)$\"",
            "python",
        );
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Compiled filter with regex: {}", compiled);
        assert!(compiled.contains("@df"));
        assert!(compiled.contains("@col"));
        assert!(compiled.contains("@filter_method"));
        assert!(compiled.contains("(#match? @filter_method \"^(filter|where)$\")"));
    }

    #[test]
    fn test_nested_call_pattern() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("$df.limit().collect()", "python");
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Nested call pattern: {}", compiled);
        assert!(compiled.contains("@df"));
        assert!(compiled.contains("(call"));
        assert!(compiled.contains("(attribute"));
    }

    #[test]
    fn test_nested_call_with_predicate() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile(
            "$df.limit().collect() $method == \"collect\" $limit_method == \"limit\"",
            "python",
        );
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Nested call with predicate: {}", compiled);
        assert!(compiled.contains("@df"));
        assert!(compiled.contains("(call"));
        assert!(compiled.contains("@method"));
        assert!(compiled.contains("@limit_method"));
        assert!(compiled.contains("(#eq? @method \"collect\")"));
        assert!(compiled.contains("(#eq? @limit_method \"limit\")"));
    }

    #[test]
    fn test_simple_collect() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("$df.collect()", "python");
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Simple collect pattern: {}", compiled);
    }

    #[test]
    fn test_method_chain_collect() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("$chain.collect()", "python");
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Method chain collect: {}", compiled);
        assert!(compiled.contains("@chain"), "Should have @chain capture");
    }

    #[test]
    fn test_multi_level_attribute_chain() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("$spark.read.csv($path)", "python");
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Multi-level attribute chain: {}", compiled);
        assert!(compiled.contains("@spark"));
        assert!(compiled.contains("@path"));
    }

    #[test]
    fn test_complex_nested_chain() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("$spark.read.csv($path).collect()", "python");
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Complex nested chain: {}", compiled);
        assert!(compiled.contains("@spark"));
        assert!(compiled.contains("@path"));
        assert!(compiled.contains("@method"));
    }

    #[test]
    fn test_import_statement() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("import $module", "python");
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Import statement: {}", compiled);
        assert!(compiled.contains("@module"));
    }

    #[test]
    fn test_import_from_statement() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("from $module import $name", "python");
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Import from statement: {}", compiled);
        assert!(compiled.contains("@module"));
        assert!(compiled.contains("@name"));
    }

    #[test]
    fn test_bare_function_call() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("udf()", "python");
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Bare function call: {}", compiled);
        assert!(compiled.contains("(call"));
    }

    #[test]
    fn test_predicate_gt() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("$df.repartition($n) $n > 1", "python");
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Compiled with gt predicate: {}", compiled);
        assert!(compiled.contains("@df"));
        assert!(compiled.contains("@n"));
        assert!(compiled.contains("(#gt? @n \"1\")"));
    }

    #[test]
    fn test_predicate_lt() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("$df.limit($n) $n < 100", "python");
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Compiled with lt predicate: {}", compiled);
        assert!(compiled.contains("@n"));
        assert!(compiled.contains("(#lt? @n \"100\")"));
    }

    #[test]
    fn test_predicate_gte() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("$df.take($n) $n >= 10", "python");
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Compiled with gte predicate: {}", compiled);
        assert!(compiled.contains("(#gte? @n \"10\")"));
    }

    #[test]
    fn test_predicate_lte() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("$df.take($n) $n <= 50", "python");
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Compiled with lte predicate: {}", compiled);
        assert!(compiled.contains("(#lte? @n \"50\")"));
    }

    #[test]
    fn test_multiple_numeric_predicates() {
        let compiler = CinderCompiler::new();
        let result = compiler.compile("$df.limit($n) $n > 0 $n < 1000", "python");
        assert!(result.is_ok());
        let compiled = result.unwrap();
        println!("Compiled with multiple numeric predicates: {}", compiled);
        assert!(compiled.contains("(#gt? @n \"0\")"));
        assert!(compiled.contains("(#lt? @n \"1000\")"));
    }
}
