use crate::types::{Finding, Provenance, PythonParseResult, SdpSignal, SqlFragment};
use tree_sitter::Parser;

pub fn parse_python(source: &str) -> PythonParseResult {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_python::LANGUAGE.into())
        .expect("tree-sitter-python grammar failed to load");

    let tree = parser
        .parse(source, None)
        .expect("tree-sitter failed to parse");
    let root = tree.root_node();

    let mut sql_fragments = Vec::new();
    let mut sdp_signals = Vec::new();
    let mut findings = Vec::new();

    extract_sql_fragments(source, root, &mut sql_fragments, &mut findings);
    extract_sdp_signals(source, root, &mut sdp_signals);
    extract_syntax_errors(&tree, source, &mut findings);

    PythonParseResult {
        sql_fragments,
        sdp_signals,
        findings,
    }
}

fn extract_sql_fragments(
    source: &str,
    root: tree_sitter::Node,
    sql_fragments: &mut Vec<SqlFragment>,
    findings: &mut Vec<Finding>,
) {
    let mut visitor = FragmentVisitor {
        source,
        sql_fragments,
        findings,
    };
    visitor.visit(&root);
}

struct FragmentVisitor<'a> {
    source: &'a str,
    sql_fragments: &'a mut Vec<SqlFragment>,
    findings: &'a mut Vec<Finding>,
}

impl<'a> FragmentVisitor<'a> {
    fn visit(&mut self, node: &tree_sitter::Node) {
        if node.kind() == "call" {
            if let Some((sql_text, is_fstring)) = self.extract_spark_sql(node) {
                let start = node.start_position();
                let end = node.end_position();

                if is_fstring {
                    let finding = Finding {
                        rule_id: "BNT".to_string(),
                        code: "BN002".to_string(),
                        severity: crate::types::Severity::Warning,
                        message: "Dynamic SQL in f-string cannot be statically analyzed"
                            .to_string(),
                        suggestion: Some(
                            "Use spark.sql with a literal string or parameterize".to_string(),
                        ),
                        line_number: Some(start.row as u32 + 1),
                        column: Some(start.column as u32),
                        confidence: crate::types::Confidence::Medium,
                    };
                    self.findings.push(finding);
                }

                self.sql_fragments.push(SqlFragment {
                    text: sql_text,
                    provenance: Provenance {
                        source_path: None,
                        start_line: start.row as u32 + 1,
                        end_line: end.row as u32 + 1,
                    },
                });
            }
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.visit(&child);
        }
    }

    fn extract_spark_sql(&self, node: &tree_sitter::Node) -> Option<(String, bool)> {
        let (obj, method) = self.get_call_info(node)?;
        if obj != "spark" || method != "sql" {
            return None;
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            if child.kind() == "argument_list" {
                return self.extract_first_string_arg(&child);
            }
        }
        None
    }

    fn extract_first_string_arg(&self, arg_list: &tree_sitter::Node) -> Option<(String, bool)> {
        let mut cursor = arg_list.walk();
        for child in arg_list.children(&mut cursor) {
            if child.kind() == "string" {
                return self.extract_string_content(&child);
            }
        }
        None
    }

    fn extract_string_content(&self, string_node: &tree_sitter::Node) -> Option<(String, bool)> {
        let mut is_fstring = false;
        let mut cursor = string_node.walk();
        let mut content = String::new();

        for child in string_node.children(&mut cursor) {
            match child.kind() {
                "string_start" => {
                    let text = child.utf8_text(self.source.as_bytes()).ok()?;
                    if text.starts_with('f') || text.starts_with('F') {
                        is_fstring = true;
                    }
                }
                "string_content" => {
                    content.push_str(child.utf8_text(self.source.as_bytes()).ok()?);
                }
                "string_end" => {}
                _ => {
                    let text = child.utf8_text(self.source.as_bytes()).ok()?;
                    if text == "'" || text == "\"" {
                        continue;
                    }
                    content.push_str(text);
                }
            }
        }
        if content.is_empty() {
            let text = string_node.utf8_text(self.source.as_bytes()).ok()?;
            return Some((text.trim_matches('"').trim_matches('\'').to_string(), false));
        }
        Some((content, is_fstring))
    }

    fn get_call_info(&self, node: &tree_sitter::Node) -> Option<(String, String)> {
        let mut cursor = node.walk();
        let children: Vec<_> = node.children(&mut cursor).collect();
        if children.is_empty() {
            return None;
        }
        let first = &children[0];

        if first.kind() == "attribute" {
            let mut attr_cursor = first.walk();
            let attr_children: Vec<_> = first.children(&mut attr_cursor).collect();
            let mut parts = Vec::new();
            for child in attr_children {
                let text = child.utf8_text(self.source.as_bytes()).ok()?;
                parts.push(text.to_string());
            }
            if parts.len() >= 2 {
                return Some((parts[0].clone(), parts[2].clone()));
            }
        }

        None
    }
}

fn extract_sdp_signals(source: &str, root: tree_sitter::Node, sdp_signals: &mut Vec<SdpSignal>) {
    let mut visitor = SdpSignalVisitor {
        source,
        signals: sdp_signals,
    };
    visitor.visit(&root);
}

struct SdpSignalVisitor<'a> {
    source: &'a str,
    signals: &'a mut Vec<SdpSignal>,
}

impl<'a> SdpSignalVisitor<'a> {
    fn visit(&mut self, node: &tree_sitter::Node) {
        match node.kind() {
            "import_statement" => {
                let text = node.utf8_text(self.source.as_bytes()).unwrap_or("");
                if text.contains("import sdp") || text.contains("import dp") {
                    self.signals.push(SdpSignal::Import);
                }
            }
            "import_from_statement" => {
                let text = node.utf8_text(self.source.as_bytes()).unwrap_or("");
                if text.contains("from sdp import") || text.contains("from dp import") {
                    self.signals.push(SdpSignal::Import);
                }
            }
            "decorator" => {
                let text = node.utf8_text(self.source.as_bytes()).unwrap_or("");
                if text.contains("@sdp.table")
                    || text.contains("@dp.table")
                    || text.contains("@dp.materialized_view")
                {
                    if text.contains("materialized_view") {
                        self.signals
                            .push(SdpSignal::Decorator("materialized_view".to_string()));
                    } else if text.contains("table") {
                        self.signals.push(SdpSignal::Decorator("table".to_string()));
                    }
                }
            }
            _ => {}
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.visit(&child);
        }
    }
}

fn extract_syntax_errors(tree: &tree_sitter::Tree, source: &str, findings: &mut Vec<Finding>) {
    let root = tree.root_node();

    fn visit_node(node: tree_sitter::Node, source: &str, findings: &mut Vec<Finding>) {
        if node.kind() == "ERROR" {
            let start = node.start_position();
            let text = node.utf8_text(source.as_bytes()).unwrap_or("<unknown>");
            let message = if text.len() > 50 {
                format!("Syntax error near: {}...", &text[..50])
            } else {
                format!("Syntax error near: {}", text)
            };

            findings.push(Finding {
                rule_id: "BNT".to_string(),
                code: "BN001".to_string(),
                severity: crate::types::Severity::Error,
                message,
                suggestion: Some("Fix the syntax error before analysis".to_string()),
                line_number: Some(start.row as u32 + 1),
                column: Some(start.column as u32),
                confidence: crate::types::Confidence::High,
            });
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            visit_node(child, source, findings);
        }
    }

    visit_node(root, source, findings);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_python() {
        let result = parse_python("import pandas as pd\nprint('hello')");
        assert!(result.findings.is_empty());
    }

    #[test]
    fn test_dlt_import_detection() {
        let result = parse_python("import dlt\n@dlt.table\ndef my_table(): pass");
        assert!(!result.sdp_signals.is_empty());
    }

    #[test]
    fn test_spark_sql_extraction() {
        let result = parse_python("spark.sql('SELECT * FROM table')");
        assert!(!result.sql_fragments.is_empty());
        assert_eq!(result.sql_fragments[0].text, "SELECT * FROM table");
    }

    #[test]
    fn test_spark_sql_fstring_detection() {
        let result = parse_python("spark.sql(f'SELECT * FROM {table}')");
        assert!(!result.findings.is_empty());
        assert_eq!(result.findings[0].code, "BN002");
    }
}
