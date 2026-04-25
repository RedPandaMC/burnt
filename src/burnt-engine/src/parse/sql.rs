use crate::types::Finding;
use sqlparser::ast::Statement;
use sqlparser::dialect::DatabricksDialect;
use sqlparser::parser::Parser;
use tree_sitter::Parser as TsParser;

#[derive(Debug, Clone)]
pub struct SqlParseResult {
    pub fragments: Vec<SqlFragmentWithAst>,
    pub dlt_tables: Vec<DltTableDef>,
    pub findings: Vec<Finding>,
}

#[derive(Debug, Clone)]
pub struct SqlFragmentWithAst {
    pub text: String,
    pub statement: Option<Statement>,
}

#[derive(Debug, Clone)]
pub struct DltTableDef {
    pub name: String,
    pub kind: DltTableKind,
    pub query: String,
}

#[derive(Debug, Clone)]
pub enum DltTableKind {
    StreamingTable,
    MaterializedView,
}

pub fn parse_sql(source: &str) -> SqlParseResult {
    let statements = match Parser::parse_sql(&DatabricksDialect {}, source) {
        Ok(stmts) => stmts,
        Err(e) => {
            let finding = Finding {
                rule_id: "BNT".to_string(),
                code: "BN001".to_string(),
                severity: crate::types::Severity::Error,
                message: format!("SQL syntax error: {}", e),
                suggestion: Some("Fix the SQL syntax error".to_string()),
                line_number: None,
                column: None,
                confidence: crate::types::Confidence::High,
            };
            return SqlParseResult {
                fragments: vec![],
                dlt_tables: vec![],
                findings: vec![finding],
            };
        }
    };

    let mut dlt_tables = Vec::new();
    let fragments: Vec<SqlFragmentWithAst> = statements
        .iter()
        .map(|stmt| {
            if let Some(dlt_def) = detect_dlt_table(stmt) {
                dlt_tables.push(dlt_def);
            }
            SqlFragmentWithAst {
                text: source.to_string(),
                statement: Some(stmt.clone()),
            }
        })
        .collect();

    SqlParseResult {
        fragments,
        dlt_tables,
        findings: vec![],
    }
}

pub fn parse_sql_with_tree_sitter(source: &str) -> Option<tree_sitter::Tree> {
    let mut parser = TsParser::new();
    parser
        .set_language(&tree_sitter_sequel::LANGUAGE.into())
        .ok()?;
    parser.parse(source, None)
}

fn detect_dlt_table(stmt: &Statement) -> Option<DltTableDef> {
    match stmt {
        Statement::CreateView(create_view) => {
            let query = create_view.query.to_string();
            if query.contains("LIVE.") {
                return Some(DltTableDef {
                    name: create_view.name.to_string(),
                    kind: DltTableKind::MaterializedView,
                    query,
                });
            }
            None
        }
        Statement::CreateTable(create_table) => {
            let name = create_table.name.to_string();
            let query = create_table.query.as_ref()?.to_string();

            if name.to_lowercase().contains("streaming")
                || query.to_lowercase().contains("streaming")
            {
                return Some(DltTableDef {
                    name,
                    kind: DltTableKind::StreamingTable,
                    query,
                });
            }
            None
        }
        _ => None,
    }
}

pub fn extract_table_refs(source: &str) -> Vec<String> {
    let statements = match Parser::parse_sql(&DatabricksDialect {}, source) {
        Ok(stmts) => stmts,
        Err(_) => return vec![],
    };

    let mut refs = Vec::new();
    for stmt in statements {
        collect_from_statement(&stmt, &mut refs);
    }
    refs
}

fn collect_from_statement(stmt: &Statement, refs: &mut Vec<String>) {
    if let Statement::Query(query) = stmt {
        if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
            for table in &select.from {
                if let sqlparser::ast::TableFactor::Table { name, .. } = &table.relation {
                    refs.push(name.to_string());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_sql() {
        let result = parse_sql("SELECT * FROM my_table");
        assert!(!result.fragments.is_empty());
    }

    #[test]
    fn test_parse_sql_with_cte() {
        let sql = "WITH cte AS (SELECT 1) SELECT * FROM cte";
        let result = parse_sql(sql);
        assert!(!result.fragments.is_empty());
    }

    #[test]
    fn test_parse_join() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.id";
        let result = parse_sql(sql);
        assert!(!result.fragments.is_empty());
    }

    #[test]
    fn test_extract_table_refs() {
        let refs = extract_table_refs("SELECT * FROM a JOIN b ON a.id = b.id");
        assert!(!refs.is_empty());
    }

    #[test]
    fn test_parse_merge() {
        let sql = "MERGE INTO target AS t USING source AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.val = s.val";
        let result = parse_sql(sql);
        assert!(!result.fragments.is_empty());
    }

    #[test]
    fn test_tree_sitter_sequel_parsing() {
        let result = parse_sql_with_tree_sitter("SELECT * FROM users WHERE id = 1");
        assert!(result.is_some());
    }

    #[test]
    fn test_print_sequel_tree() {
        let source = "SELECT id FROM users WHERE id NOT IN (SELECT id FROM deleted_users)";
        let tree = parse_sql_with_tree_sitter(source).unwrap();

        fn print_tree(node: tree_sitter::Node, source: &str, indent: usize) {
            let prefix = "  ".repeat(indent);
            let kind = node.kind();
            let text = &source[node.byte_range()];
            let trimmed = if text.len() > 40 { &text[..40] } else { text };
            println!("{}[{}] '{}'", prefix, kind, trimmed);
            for i in 0..node.child_count() {
                if let Some(child) = node.child(i) {
                    print_tree(child, source, indent + 1);
                }
            }
        }

        print_tree(tree.root_node(), source, 0);
    }

    #[test]
    fn test_print_select_star_tree() {
        let source = "SELECT * FROM users";
        let tree = parse_sql_with_tree_sitter(source).unwrap();

        fn print_tree(node: tree_sitter::Node, source: &str, indent: usize) {
            let prefix = "  ".repeat(indent);
            let kind = node.kind();
            let text = &source[node.byte_range()];
            let trimmed = if text.len() > 40 { &text[..40] } else { text };
            println!("{}[{}] '{}'", prefix, kind, trimmed);
            for i in 0..node.child_count() {
                if let Some(child) = node.child(i) {
                    print_tree(child, source, indent + 1);
                }
            }
        }

        print_tree(tree.root_node(), source, 0);
    }

    #[test]
    fn test_print_bq001_pass_case() {
        let source = "SELECT * FROM users";
        let tree = parse_sql_with_tree_sitter(source).unwrap();

        fn collect_all_nodes(
            node: tree_sitter::Node,
            source: &str,
            results: &mut Vec<(String, String)>,
        ) {
            let kind = node.kind();
            let text = &source[node.byte_range()];
            results.push((kind.to_string(), text.to_string()));
            for i in 0..node.child_count() {
                if let Some(child) = node.child(i) {
                    collect_all_nodes(child, source, results);
                }
            }
        }

        let mut results = Vec::new();
        collect_all_nodes(tree.root_node(), source, &mut results);

        println!("All nodes:");
        for (kind, text) in results {
            let trimmed = if text.len() > 40 {
                format!("{}...", &text[..40])
            } else {
                text
            };
            println!("  [{}] '{}'", kind, trimmed);
        }
    }
}
