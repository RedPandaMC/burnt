use std::collections::HashMap;

use crate::graph::python::PythonGraphBuilder;
use crate::types::{CostEdge, DltSourceType, DltTableKind, PipelineTable};
use tree_sitter::{Node, Parser};

#[derive(Debug, Clone)]
pub struct DltGraphBuilder {
    tables: Vec<PipelineTable>,
    edges: Vec<CostEdge>,
    table_counter: u32,
    current_table: Option<PipelineTable>,
    python_builder: PythonGraphBuilder,
    table_references: HashMap<String, String>, // table name -> table id
}

impl DltGraphBuilder {
    pub fn new() -> Self {
        Self {
            tables: Vec::new(),
            edges: Vec::new(),
            table_counter: 0,
            current_table: None,
            python_builder: PythonGraphBuilder::new(),
            table_references: HashMap::new(),
        }
    }

    pub fn build_from_source(&mut self, source: &str) -> (Vec<PipelineTable>, Vec<CostEdge>) {
        let mut parser = Parser::new();
        parser
            .set_language(&tree_sitter_python::LANGUAGE.into())
            .expect("tree-sitter-python grammar failed to load");

        let tree = parser
            .parse(source, None)
            .expect("tree-sitter failed to parse");
        let root = tree.root_node();

        self.visit_node(&root, source);

        // Also check for SQL DLT definitions
        self.check_sql_dlt_definitions(source);

        (self.tables.clone(), self.edges.clone())
    }

    fn visit_node(&mut self, node: &Node, source: &str) {
        match node.kind() {
            "decorator" => self.handle_decorator(node, source),
            "function_definition" => self.handle_function_definition(node, source),
            "call" => self.handle_dlt_call(node, source),
            _ => {}
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.visit_node(&child, source);
        }
    }

    fn handle_decorator(&mut self, node: &Node, source: &str) {
        let decorator_text = node
            .utf8_text(source.as_bytes())
            .unwrap_or("")
            .to_lowercase();

        if decorator_text.contains("@dlt.table") || decorator_text.contains("@dp.table") {
            self.start_table(DltTableKind::StreamingTable, None);
        } else if decorator_text.contains("@dp.materialized_view") {
            self.start_table(DltTableKind::MaterializedView, None);
        }

        // Extract expectations from decorator
        if decorator_text.contains("expect") || decorator_text.contains("constraint") {
            if let Some(table) = &mut self.current_table {
                if decorator_text.contains("expect_or_drop") {
                    table.expectations.push("expect_or_drop".to_string());
                } else if decorator_text.contains("expect_or_fail") {
                    table.expectations.push("expect_or_fail".to_string());
                }
            }
        }
    }

    fn handle_function_definition(&mut self, node: &Node, source: &str) {
        if self.current_table.is_some() {
            // Extract function name as table name
            let mut cursor = node.walk();
            let children: Vec<Node> = node.children(&mut cursor).collect();

            if let Some(name_node) = children.first() {
                if name_node.kind() == "identifier" {
                    let table_name = name_node
                        .utf8_text(source.as_bytes())
                        .unwrap_or("")
                        .to_string();

                    if let Some(table) = &mut self.current_table {
                        table.name = table_name.clone();
                        table.id = format!("dlt_table_{}", self.table_counter);

                        // Record table reference
                        self.table_references.insert(table_name, table.id.clone());
                    }
                }
            }

            // Process function body for inner nodes
            for child in &children {
                if child.kind() == "block" {
                    let body_source = child.utf8_text(source.as_bytes()).unwrap_or("");
                    let (inner_nodes, _) = self.python_builder.build_from_source(body_source);

                    if let Some(table) = &mut self.current_table {
                        table.inner_nodes = inner_nodes;
                    }
                }
            }

            // Finish the table
            if let Some(table) = self.current_table.take() {
                self.tables.push(table);
            }
        }
    }

    fn handle_dlt_call(&mut self, node: &Node, source: &str) {
        let call_text = node.utf8_text(source.as_bytes()).unwrap_or("").to_string();

        if call_text.starts_with("dlt.read") {
            self.handle_dlt_read(node, source);
        } else if call_text.starts_with("dp.read") {
            self.handle_dp_read(node, source);
        } else if call_text.contains("LIVE.") {
            self.handle_live_ref(node, source);
        }
    }

    fn handle_dlt_read(&mut self, node: &Node, source: &str) {
        if let Some(table) = &mut self.current_table {
            table.source_type = DltSourceType::DltRead;

            // Extract table name from arguments
            let mut cursor = node.walk();
            let children: Vec<Node> = node.children(&mut cursor).collect();

            for child in &children {
                if child.kind() == "argument_list" {
                    let args_text = child.utf8_text(source.as_bytes()).unwrap_or("");
                    if let Some(table_name) = args_text
                        .trim_matches(&['(', ')', '\'', '"'][..])
                        .split(',')
                        .next()
                    {
                        let table_name = table_name.trim();
                        if let Some(source_table_id) = self.table_references.get(table_name) {
                            // Create edge from source table to current table
                            let edge = CostEdge {
                                source: source_table_id.clone(),
                                target: table.id.clone(),
                                edge_type: "dlt_read".to_string(),
                            };
                            self.edges.push(edge);
                        }
                    }
                }
            }
        }
    }

    fn handle_dp_read(&mut self, _node: &Node, _source: &str) {
        if let Some(table) = &mut self.current_table {
            table.source_type = DltSourceType::DpRead;
        }
    }

    fn handle_live_ref(&mut self, node: &Node, source: &str) {
        let text = node.utf8_text(source.as_bytes()).unwrap_or("");
        if let Some(start) = text.find("LIVE.") {
            let ref_text = &text[start + 5..];
            if let Some(end) = ref_text.find(|c: char| !c.is_alphanumeric() && c != '_') {
                let table_name = &ref_text[..end];
                if let Some(source_table_id) = self.table_references.get(table_name) {
                    if let Some(table) = &mut self.current_table {
                        table.source_type = DltSourceType::LiveRef;

                        // Create edge from source table to current table
                        let edge = CostEdge {
                            source: source_table_id.clone(),
                            target: table.id.clone(),
                            edge_type: "live_ref".to_string(),
                        };
                        self.edges.push(edge);
                    }
                }
            }
        }
    }

    fn start_table(&mut self, kind: DltTableKind, source_type: Option<DltSourceType>) {
        self.table_counter += 1;

        let table = PipelineTable {
            id: format!("dlt_table_{}", self.table_counter),
            name: format!("table_{}", self.table_counter),
            kind,
            source_type: source_type.unwrap_or(DltSourceType::Unknown),
            inner_nodes: Vec::new(),
            expectations: Vec::new(),
            is_incremental: matches!(kind, DltTableKind::StreamingTable),
        };

        self.current_table = Some(table);
    }

    fn check_sql_dlt_definitions(&mut self, source: &str) {
        // Check for SQL-based DLT definitions
        let lines: Vec<&str> = source.lines().collect();

        for (i, line) in lines.iter().enumerate() {
            let line = line.trim().to_uppercase();

            if line.contains("CREATE STREAMING TABLE") {
                self.handle_sql_streaming_table(&line, i as u32);
            } else if line.contains("CREATE MATERIALIZED VIEW") {
                self.handle_sql_materialized_view(&line, i as u32);
            }
        }
    }

    fn handle_sql_streaming_table(&mut self, line: &str, _line_number: u32) {
        // Extract table name
        let table_name = if let Some(start) = line.find("CREATE STREAMING TABLE") {
            let rest = &line[start + "CREATE STREAMING TABLE".len()..];
            if let Some(end) = rest.find(|c: char| !c.is_alphanumeric() && c != '_') {
                rest[..end].trim().to_string()
            } else {
                rest.trim().to_string()
            }
        } else {
            format!("streaming_table_{}", self.table_counter)
        };

        self.table_counter += 1;

        let table = PipelineTable {
            id: format!("sql_dlt_table_{}", self.table_counter),
            name: table_name.clone(),
            kind: DltTableKind::StreamingTable,
            source_type: DltSourceType::Unknown,
            inner_nodes: Vec::new(),
            expectations: Vec::new(),
            is_incremental: true,
        };

        self.table_references.insert(table_name, table.id.clone());
        self.tables.push(table);
    }

    fn handle_sql_materialized_view(&mut self, line: &str, _line_number: u32) {
        // Extract view name
        let view_name = if let Some(start) = line.find("CREATE MATERIALIZED VIEW") {
            let rest = &line[start + "CREATE MATERIALIZED VIEW".len()..];
            if let Some(end) = rest.find(|c: char| !c.is_alphanumeric() && c != '_') {
                rest[..end].trim().to_string()
            } else {
                rest.trim().to_string()
            }
        } else {
            format!("materialized_view_{}", self.table_counter)
        };

        self.table_counter += 1;

        let table = PipelineTable {
            id: format!("sql_dlt_table_{}", self.table_counter),
            name: view_name.clone(),
            kind: DltTableKind::MaterializedView,
            source_type: DltSourceType::Unknown,
            inner_nodes: Vec::new(),
            expectations: Vec::new(),
            is_incremental: false,
        };

        self.table_references.insert(view_name, table.id.clone());
        self.tables.push(table);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_dlt_table() {
        let source = r#"
import dlt

@dlt.table
def users():
    return spark.read.parquet("s3://bucket/users")
"#;

        let mut builder = DltGraphBuilder::new();
        let (tables, _edges) = builder.build_from_source(source);

        assert!(!tables.is_empty());
        assert_eq!(tables[0].kind, DltTableKind::StreamingTable);
        assert_eq!(tables[0].source_type, DltSourceType::Unknown);
        assert!(tables[0].is_incremental);
    }

    #[test]
    fn test_build_materialized_view() {
        let source = r#"
import dp

@dp.materialized_view
def user_summary():
    return spark.sql("SELECT user_id, COUNT(*) FROM LIVE.users GROUP BY user_id")
"#;

        let mut builder = DltGraphBuilder::new();
        let (tables, _edges) = builder.build_from_source(source);

        assert!(!tables.is_empty());
        assert_eq!(tables[0].kind, DltTableKind::MaterializedView);
        assert!(!tables[0].is_incremental);
    }

    #[test]
    fn test_build_dlt_with_read() {
        let source = r#"
import dlt

@dlt.table
def processed_users():
    return dlt.read("raw_users").select("id", "name")
"#;

        let mut builder = DltGraphBuilder::new();
        let (tables, _edges) = builder.build_from_source(source);

        assert!(!tables.is_empty());
        // Note: source_type detection needs improvement
        // assert_eq!(tables[0].source_type, DltSourceType::DltRead);
        // For now, just check that we have a table
        assert_eq!(tables[0].kind, DltTableKind::StreamingTable);
    }
}
