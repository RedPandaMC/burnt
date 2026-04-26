use thiserror::Error;
use tree_sitter::{Language as TreeSitterLanguage, Parser, Query, QueryCursor};

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Failed to parse query: {0}")]
    ParseError(String),
    #[error("Language not supported: {0}")]
    LanguageError(String),
    #[error("Query execution error: {0}")]
    ExecutionError(String),
}

pub struct QueryEngine {
    python_language: TreeSitterLanguage,
    sql_language: TreeSitterLanguage,
}

impl QueryEngine {
    pub fn new() -> Self {
        Self {
            python_language: tree_sitter_python::LANGUAGE.into(),
            sql_language: tree_sitter_sequel::LANGUAGE.into(),
        }
    }

    pub fn get_language(&self, language: &str) -> Result<TreeSitterLanguage, QueryError> {
        match language.to_lowercase().as_str() {
            "python" | "pyspark" => Ok(self.python_language.clone()),
            "sql" => Ok(self.sql_language.clone()),
            _ => Err(QueryError::LanguageError(format!(
                "Language '{}' not supported",
                language
            ))),
        }
    }

    /// Creates a fresh parser per call — safe for parallel use under rayon.
    pub fn parse_source(
        &self,
        source: &str,
        language: &str,
    ) -> Result<tree_sitter::Tree, QueryError> {
        let lang = self.get_language(language)?;
        let mut parser = Parser::new();
        parser
            .set_language(&lang)
            .map_err(|e| QueryError::ExecutionError(format!("Failed to set language: {}", e)))?;

        parser
            .parse(source, None)
            .ok_or_else(|| QueryError::ExecutionError("Failed to parse source".to_string()))
    }

    pub fn create_query(&self, pattern: &str, language: &str) -> Result<Query, QueryError> {
        let lang = self.get_language(language)?;
        Query::new(&lang, pattern)
            .map_err(|e| QueryError::ParseError(format!("Invalid query pattern: {}", e)))
    }

    pub fn execute_query(
        &self,
        tree: &tree_sitter::Tree,
        query: &Query,
        source: &str,
    ) -> Vec<QueryMatch> {
        use streaming_iterator::StreamingIterator;

        let mut cursor = QueryCursor::new();
        let mut all_matches = Vec::new();
        let capture_names = query.capture_names();

        let mut query_matches = cursor.matches(query, tree.root_node(), source.as_bytes());

        while let Some(m) = query_matches.next() {
            let captures = m
                .captures
                .iter()
                .map(|c| QueryCapture {
                    capture_name: capture_names[c.index as usize].to_string(),
                    node_kind: c.node.kind().to_string(),
                    start_position: c.node.start_position(),
                    end_position: c.node.end_position(),
                    text: String::from_utf8_lossy(&source.as_bytes()[c.node.byte_range()])
                        .to_string(),
                })
                .collect();

            all_matches.push(QueryMatch {
                pattern_index: m.pattern_index as u32,
                captures,
            });
        }

        all_matches
    }

    pub fn test_pattern(
        &self,
        source: &str,
        language: &str,
        pattern: &str,
    ) -> Result<bool, QueryError> {
        let tree = self.parse_source(source, language)?;
        let query = self.create_query(pattern, language)?;

        let matches = self.execute_query(&tree, &query, source);
        Ok(!matches.is_empty())
    }
}

#[derive(Debug, Clone)]
pub struct QueryCapture {
    pub capture_name: String,
    pub node_kind: String,
    pub start_position: tree_sitter::Point,
    pub end_position: tree_sitter::Point,
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct QueryMatch {
    pub pattern_index: u32,
    pub captures: Vec<QueryCapture>,
}

impl Default for QueryEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use insta::assert_yaml_snapshot;

    #[test]
    fn test_not_in_pattern() {
        let engine = QueryEngine::new();
        let source = r#"SELECT * FROM a WHERE id NOT IN (SELECT id FROM b)"#;

        let pattern = r#"
        (binary_expression
          (not_in)
          (subquery))
        "#;

        let result = engine.test_pattern(source, "sql", pattern);
        println!("Not IN pattern test result: {:?}", result);
        assert!(result.is_ok());
        assert!(result.unwrap(), "Pattern should match NOT IN with subquery");
    }

    #[test]
    fn test_select_star_pattern_direct() {
        let engine = QueryEngine::new();

        // Test various SELECT * cases
        let cases = vec![
            ("SELECT * FROM users", true, "sql"), // Should match
            ("SELECT * FROM users WHERE id = 1", true, "sql"), // Should match
            ("SELECT * FROM users LIMIT 100", true, "sql"), // Should match (exclude handles it)
            ("SELECT id, name FROM users", false, "sql"), // Should NOT match
        ];

        let pattern = r#"
        (select
          (select_expression
            (term
              (all_fields))))
        "#;

        for (source, should_match, lang) in cases {
            let result = engine.test_pattern(source, lang, pattern);
            println!(
                "SELECT * pattern on '{}': {:?} (expected {})",
                source, result, should_match
            );
            if should_match {
                assert!(result.unwrap(), "Pattern should match for: {}", source);
            } else {
                assert!(!result.unwrap(), "Pattern should NOT match for: {}", source);
            }
        }
    }

    #[test]
    fn test_rule_pipeline_sq001() {
        let source = "SELECT * FROM users WHERE id = 1";
        let findings = crate::rules::run(source, "SQL").unwrap();
        let sq001_findings: Vec<_> = findings.iter().filter(|f| f.code == "SQ001").collect();
        println!("SQ001 findings for '{}': {:?}", source, sq001_findings);
        assert!(
            !sq001_findings.is_empty(),
            "SQ001 should fire for: {}",
            source
        );
    }

    #[test]
    fn test_rule_pipeline_bq001() {
        let source = "SELECT * FROM a WHERE id NOT IN (SELECT id FROM b)";
        let findings = crate::rules::run(source, "SQL").unwrap();
        let bq001_findings: Vec<_> = findings.iter().filter(|f| f.code == "BQ001").collect();
        println!("BQ001 findings for '{}': {:?}", source, bq001_findings);
        assert!(
            !bq001_findings.is_empty(),
            "BQ001 should fire for: {}",
            source
        );
    }

    #[test]
    fn test_python_collect_pattern() {
        let engine = QueryEngine::new();
        let source = r#"df.collect()"#;

        let pattern = r#"(call)"#;

        let _result = engine.test_pattern(source, "python", pattern);
        println!("Query test completed without panic");
    }

    #[test]
    fn test_sql_select_star_pattern() {
        let engine = QueryEngine::new();
        let source = r#"SELECT * FROM table"#;

        let pattern = r#"
        (select_statement
          select: (select_clause
            (star)
          )
        )
        "#;

        let _result = engine.test_pattern(source, "sql", pattern);
        println!("SQL query test completed without panic");
    }

    #[test]
    fn test_query_execution_details() {
        let engine = QueryEngine::new();
        let source = r#"df.collect()"#;

        let tree = engine.parse_source(source, "python").unwrap();
        let pattern = r#"
        (call
          function: (attribute
            object: (identifier) @df_var
            attribute: (identifier) @method_name
          )
          (#eq? @method_name "collect")
        )
        "#;

        let query = engine.create_query(pattern, "python").unwrap();

        assert!(tree.root_node().kind() == "module");
        assert!(query.pattern_count() > 0);
    }
}
