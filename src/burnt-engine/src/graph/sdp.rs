use std::collections::HashMap;
use std::sync::Mutex;

use crate::graph::python::PythonGraphBuilder;
use crate::parse::namespace::{build_namespace_tracker, NamespaceTracker};
use crate::types::{CostEdge, PipelineTable, SdpSourceType, SdpTableKind};
use tree_sitter::{Node, Parser};

pub struct SdpGraphBuilder {
    tables: Vec<PipelineTable>,
    edges: Vec<CostEdge>,
    table_counter: u32,
    current_table: Option<PipelineTable>,
    python_builder: PythonGraphBuilder,
    table_references: HashMap<String, String>,
    parser: Mutex<Parser>,
    ns_tracker: NamespaceTracker,
}

impl SdpGraphBuilder {
    pub fn new() -> Self {
        Self {
            tables: Vec::new(),
            edges: Vec::new(),
            table_counter: 0,
            current_table: None,
            python_builder: PythonGraphBuilder::new(),
            table_references: HashMap::new(),
            parser: Mutex::new(Parser::new()),
            ns_tracker: NamespaceTracker::new(),
        }
    }

    pub fn build_from_source(&mut self, source: &str) -> (Vec<PipelineTable>, Vec<CostEdge>) {
        let tree = {
            let mut parser = self.parser.lock().unwrap();
            parser.reset();
            parser
                .set_language(&tree_sitter_python::LANGUAGE.into())
                .expect("tree-sitter-python grammar failed to load");
            parser
                .parse(source, None)
                .expect("tree-sitter failed to parse")
        };
        let root = tree.root_node();

        self.ns_tracker = build_namespace_tracker(source, root);

        self.visit_node(&root, source);

        // Also check for SQL DLT definitions
        self.check_sql_sdp_definitions(source);

        (self.tables.clone(), self.edges.clone())
    }

    fn visit_node(&mut self, node: &Node, source: &str) {
        match node.kind() {
            "decorator" => self.handle_decorator(node, source),
            "function_definition" => {
                self.handle_function_definition(node, source);
                return;
            }
            "call" => self.handle_sdp_call(node, source),
            _ => {}
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.visit_node(&child, source);
        }
    }

    fn handle_decorator(&mut self, node: &Node, source: &str) {
        let decorator_text = node.utf8_text(source.as_bytes()).unwrap_or("");

        if let Some((ns_part, kind)) = self.extract_decorator_ns_and_kind(decorator_text) {
            if self.ns_tracker.is_dlt_namespace(ns_part) {
                let table_kind = if kind == "materialized_view" {
                    SdpTableKind::MaterializedView
                } else {
                    SdpTableKind::StreamingTable
                };
                self.start_table(table_kind, None);
            }
        }

        let lower = decorator_text.to_lowercase();
        if lower.contains("expect") || lower.contains("constraint") {
            if let Some(table) = &mut self.current_table {
                if lower.contains("expect_or_drop") {
                    table.expectations.push("expect_or_drop".to_string());
                } else if lower.contains("expect_or_fail") {
                    table.expectations.push("expect_or_fail".to_string());
                }
            }
        }
    }

    fn extract_decorator_ns_and_kind<'a>(
        &self,
        decorator_text: &'a str,
    ) -> Option<(&'a str, &'a str)> {
        let trimmed = decorator_text.trim();
        let at_pos = trimmed.find('@')?;
        let after_at = trimmed[at_pos + 1..].trim();

        if let Some(dot_pos) = after_at.find('.') {
            let ns_part = &after_at[..dot_pos];
            let kind = &after_at[dot_pos + 1..];
            return Some((ns_part, kind));
        }

        None
    }

    fn handle_function_definition(&mut self, node: &Node, source: &str) {
        if self.current_table.is_some() {
            let mut cursor = node.walk();
            let children: Vec<Node> = node.children(&mut cursor).collect();

            let table_name = children
                .iter()
                .find(|n| n.kind() == "identifier")
                .and_then(|n| n.utf8_text(source.as_bytes()).ok().map(String::from));

            if let Some(name) = table_name {
                if let Some(table) = &mut self.current_table {
                    table.name = name.clone();
                    table.id = format!("sdp_table_{}", self.table_counter);
                    self.table_references.insert(name, table.id.clone());
                }
            }

            for child in &children {
                if child.kind() == "block" {
                    let body_source = child.utf8_text(source.as_bytes()).unwrap_or("");
                    let (inner_nodes, _, _) = self.python_builder.build_from_source(body_source);

                    if let Some(table) = &mut self.current_table {
                        table.inner_nodes = inner_nodes;
                    }

                    let mut block_cursor = child.walk();
                    for block_child in child.children(&mut block_cursor) {
                        self.visit_node(&block_child, source);
                    }
                }
            }

            if let Some(table) = self.current_table.take() {
                self.tables.push(table);
            }
        }
    }

    fn handle_sdp_call(&mut self, node: &Node, source: &str) {
        let call_text = node.utf8_text(source.as_bytes()).unwrap_or("").to_string();

        if call_text.contains("LIVE.") {
            self.handle_live_ref(node, source);
            return;
        }

        if let Some((ns_part, method)) = self.extract_call_ns_and_method(&call_text) {
            if self.ns_tracker.is_dlt_namespace(ns_part) {
                if method == "read" || method.starts_with("read_") {
                    if self.ns_tracker.resolve(ns_part) == Some("dp") || ns_part == "dp" {
                        self.handle_dp_read(node, source);
                    } else {
                        self.handle_sdp_read(node, source);
                    }
                }
            }
        }
    }

    fn extract_call_ns_and_method<'a>(&self, call_text: &'a str) -> Option<(&'a str, &'a str)> {
        let mut search_from = 0;
        loop {
            if let Some(dot_pos) = call_text[search_from..].find('.') {
                let actual_pos = search_from + dot_pos;
                let ns_part = &call_text[..actual_pos];
                if ns_part.is_empty() || !self.ns_tracker.is_dlt_namespace(ns_part) {
                    search_from = actual_pos + 1;
                    continue;
                }
                let method = &call_text[actual_pos + 1..];
                let method_end = method
                    .find(|c: char| !c.is_alphanumeric() && c != '_')
                    .unwrap_or(method.len());
                let method = &method[..method_end];
                return Some((ns_part, method));
            }
            return None;
        }
    }

    fn handle_sdp_read(&mut self, node: &Node, source: &str) {
        if let Some(table) = &mut self.current_table {
            table.source_type = SdpSourceType::SdpRead;

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
                                edge_type: "sdp_read".to_string(),
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
            table.source_type = SdpSourceType::DpRead;
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
                        table.source_type = SdpSourceType::LiveRef;

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

    fn start_table(&mut self, kind: SdpTableKind, source_type: Option<SdpSourceType>) {
        self.table_counter += 1;

        let table = PipelineTable {
            id: format!("sdp_table_{}", self.table_counter),
            name: format!("table_{}", self.table_counter),
            kind,
            source_type: source_type.unwrap_or(SdpSourceType::Unknown),
            inner_nodes: Vec::new(),
            expectations: Vec::new(),
            is_incremental: matches!(kind, SdpTableKind::StreamingTable),
        };

        self.current_table = Some(table);
    }

    fn check_sql_sdp_definitions(&mut self, source: &str) {
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
            id: format!("sql_sdp_table_{}", self.table_counter),
            name: table_name.clone(),
            kind: SdpTableKind::StreamingTable,
            source_type: SdpSourceType::Unknown,
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
            id: format!("sql_sdp_table_{}", self.table_counter),
            name: view_name.clone(),
            kind: SdpTableKind::MaterializedView,
            source_type: SdpSourceType::Unknown,
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
    fn test_build_sdp_table() {
        let source = r#"
import sdp

@sdp.table
def users():
    return spark.read.parquet("s3://bucket/users")
"#;

        let mut builder = SdpGraphBuilder::new();
        let (tables, _edges) = builder.build_from_source(source);

        assert!(!tables.is_empty());
        assert_eq!(tables[0].kind, SdpTableKind::StreamingTable);
        assert_eq!(tables[0].source_type, SdpSourceType::Unknown);
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

        let mut builder = SdpGraphBuilder::new();
        let (tables, _edges) = builder.build_from_source(source);

        assert!(!tables.is_empty());
        assert_eq!(tables[0].kind, SdpTableKind::MaterializedView);
        assert!(!tables[0].is_incremental);
    }

    #[test]
    fn test_build_sdp_with_read() {
        let source = r#"
import sdp

@sdp.table
def processed_users():
    return sdp.read("raw_users").select("id", "name")
"#;

        let mut builder = SdpGraphBuilder::new();
        let (tables, _edges) = builder.build_from_source(source);

        assert!(!tables.is_empty());
        assert_eq!(tables[0].source_type, SdpSourceType::SdpRead);
        assert_eq!(tables[0].kind, SdpTableKind::StreamingTable);
    }

    #[test]
    fn test_build_dlt_table_with_alias() {
        let source = r#"
import dlt as dl

@dl.table
def my_table():
    return spark.read.parquet("s3://data")
"#;

        let mut builder = SdpGraphBuilder::new();
        let (tables, _edges) = builder.build_from_source(source);

        assert!(!tables.is_empty());
        assert_eq!(tables[0].kind, SdpTableKind::StreamingTable);
        assert_eq!(tables[0].name, "my_table");
    }

    #[test]
    fn test_build_from_import_alias() {
        let source = r#"
from dlt import table as t

@t.table
def my_table():
    return spark.read.parquet("s3://data")
"#;

        let mut builder = SdpGraphBuilder::new();
        let (tables, _edges) = builder.build_from_source(source);

        assert!(!tables.is_empty());
        assert_eq!(tables[0].kind, SdpTableKind::StreamingTable);
        assert_eq!(tables[0].name, "my_table");
    }

    #[test]
    fn test_build_dlt_read_with_alias() {
        let source = r#"
import dlt as dl

@dl.table
def processed():
    return dl.read("raw").select("id", "name")
"#;

        let mut builder = SdpGraphBuilder::new();
        let (tables, _edges) = builder.build_from_source(source);

        assert!(!tables.is_empty());
        assert_eq!(tables[0].source_type, SdpSourceType::SdpRead);
    }

    #[test]
    fn test_build_dp_read_with_alias() {
        let source = r#"
import dp as d

@d.table
def my_table():
    return d.read_csv("data.csv")
"#;

        let mut builder = SdpGraphBuilder::new();
        let (tables, _edges) = builder.build_from_source(source);

        assert!(!tables.is_empty());
        assert_eq!(tables[0].source_type, SdpSourceType::DpRead);
    }
}
