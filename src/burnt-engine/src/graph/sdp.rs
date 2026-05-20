use std::collections::HashMap;

use crate::graph::python::PythonGraphBuilder;
use crate::parse::import_map::ImportMap;
use crate::types::{Edge, PipelineTable, SdpSourceType, SdpTableKind, TableRef};
use tree_sitter::{Node as TsNode, Parser};

pub struct SdpGraphBuilder {
    tables: Vec<PipelineTable>,
    edges: Vec<Edge>,
    table_counter: u32,
    current_table: Option<PipelineTable>,
    table_references: HashMap<String, String>,
    parser: Parser,
    ns_tracker: ImportMap,
}

impl SdpGraphBuilder {
    pub fn new() -> Self {
        Self {
            tables: Vec::new(),
            edges: Vec::new(),
            table_counter: 0,
            current_table: None,
            table_references: HashMap::new(),
            parser: Parser::new(),
            ns_tracker: ImportMap::new(),
        }
    }

    pub fn build_from_source(mut self, source: &str) -> (Vec<PipelineTable>, Vec<Edge>) {
        self.parser.reset();
        self.parser
            .set_language(&tree_sitter_python::LANGUAGE.into())
            .expect("tree-sitter-python grammar failed to load");
        let tree = self
            .parser
            .parse(source, None)
            .expect("tree-sitter failed to parse");
        let root = tree.root_node();

        self.ns_tracker = ImportMap::build(source, root);

        self.visit_node(&root, source);

        // Also check for SQL DLT definitions
        self.check_sql_sdp_definitions(source);

        (self.tables, self.edges)
    }

    fn visit_node(&mut self, node: &TsNode, source: &str) {
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

    fn handle_decorator(&mut self, node: &TsNode, source: &str) {
        let decorator_text = node.utf8_text(source.as_bytes()).unwrap_or("");

        if let Some((ns_part, kind)) = self.extract_decorator_ns_and_kind(decorator_text) {
            if self.ns_tracker.is_pipeline_ns(ns_part) {
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

    fn handle_function_definition(&mut self, node: &TsNode, source: &str) {
        if self.current_table.is_some() {
            let mut cursor = node.walk();
            let children: Vec<TsNode> = node.children(&mut cursor).collect();

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
                    let (inner_nodes, _, _) = PythonGraphBuilder::new()
                        .build_from_source(body_source)
                        .unwrap_or_else(|_| (Vec::new(), Vec::new(), Vec::new()));

                    if let Some(table) = &mut self.current_table {
                        table.inner_nodes = inner_nodes;
                        // Tag every inner node with the DLT/SDP output ref so
                        // downstream consumers can answer "which pipeline table
                        // does this node materialise?" without back-walking
                        // through the parent struct.
                        let output_ref = TableRef::from_dotted(&table.name);
                        for n in &mut table.inner_nodes {
                            if !n.tables_referenced.contains(&output_ref) {
                                n.tables_referenced.push(output_ref.clone());
                            }
                        }
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

    fn handle_sdp_call(&mut self, node: &TsNode, source: &str) {
        let call_text = node.utf8_text(source.as_bytes()).unwrap_or("").to_string();

        if call_text.contains("LIVE.") {
            self.handle_live_ref(node, source);
            return;
        }

        if let Some((ns_part, method)) = self.extract_call_ns_and_method(&call_text) {
            if self.ns_tracker.is_pipeline_ns(ns_part)
                && (method == "read" || method.starts_with("read_"))
            {
                if self.ns_tracker.resolve(ns_part) == Some("dp") || ns_part == "dp" {
                    self.handle_dp_read(node, source);
                } else {
                    self.handle_sdp_read(node, source);
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
                if ns_part.is_empty() || !self.ns_tracker.is_pipeline_ns(ns_part) {
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

    fn handle_sdp_read(&mut self, node: &TsNode, source: &str) {
        if let Some(table) = &mut self.current_table {
            table.source_types.push(SdpSourceType::SdpRead);

            // Extract table name from arguments — use tree-sitter AST children
            // rather than raw string splitting (R-028 fix).
            let mut cursor = node.walk();
            let children: Vec<TsNode> = node.children(&mut cursor).collect();

            for child in &children {
                if child.kind() == "argument_list" {
                    let mut arg_cursor = child.walk();
                    let arg_children: Vec<TsNode> = child.children(&mut arg_cursor).collect();
                    // Skip opening/closing parens; process positional arguments only.
                    let positional: Vec<&TsNode> = arg_children
                        .iter()
                        .filter(|n| n.kind() != "," && n.kind() != "(" && n.kind() != ")")
                        .collect();
                    if let Some(first_arg) = positional.first() {
                        let table_name = first_arg
                            .utf8_text(source.as_bytes())
                            .unwrap_or("")
                            .trim_matches(&['\'', '"'][..]);
                        let table_name = table_name.trim();
                        if let Some(source_table_id) = self.table_references.get(table_name) {
                            // Create edge from source table to current table
                            let edge = Edge {
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

    fn handle_dp_read(&mut self, _node: &TsNode, _source: &str) {
        if let Some(table) = &mut self.current_table {
            table.source_types.push(SdpSourceType::DpRead);
        }
    }

    fn handle_live_ref(&mut self, node: &TsNode, source: &str) {
        let text = node.utf8_text(source.as_bytes()).unwrap_or("");
        if let Some(start) = text.find("LIVE.") {
            let ref_text = &text[start + 5..];
            if let Some(end) = ref_text.find(|c: char| !c.is_alphanumeric() && c != '_') {
                let table_name = &ref_text[..end];
                let live_ref = TableRef::temp_view(table_name);
                if let Some(source_table_id) = self.table_references.get(table_name) {
                    if let Some(table) = &mut self.current_table {
                        table.source_types.push(SdpSourceType::LiveRef);

                        // Create edge from source table to current table
                        let edge = Edge {
                            source: source_table_id.clone(),
                            target: table.id.clone(),
                            edge_type: "live_ref".to_string(),
                        };
                        self.edges.push(edge);
                        for n in &mut table.inner_nodes {
                            if !n.tables_referenced.contains(&live_ref) {
                                n.tables_referenced.push(live_ref.clone());
                            }
                        }
                    }
                } else if let Some(table) = &mut self.current_table {
                    // LIVE.<unknown> — still surface the temp_view ref so the
                    // pipeline table records the dependency even when the
                    // source DLT table isn't visible in this file.
                    for n in &mut table.inner_nodes {
                        if !n.tables_referenced.contains(&live_ref) {
                            n.tables_referenced.push(live_ref.clone());
                        }
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
            source_types: source_type.map(|s| vec![s]).unwrap_or_default(),
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
            source_types: vec![SdpSourceType::Unknown],
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
            source_types: vec![SdpSourceType::Unknown],
            inner_nodes: Vec::new(),
            expectations: Vec::new(),
            is_incremental: false,
        };

        self.table_references.insert(view_name, table.id.clone());
        self.tables.push(table);
    }
}

impl Default for SdpGraphBuilder {
    fn default() -> Self {
        Self::new()
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

        let (tables, _edges) = SdpGraphBuilder::new().build_from_source(source);

        assert!(!tables.is_empty());
        assert_eq!(tables[0].kind, SdpTableKind::StreamingTable);
        assert_eq!(tables[0].source_types[0], SdpSourceType::Unknown);
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

        let (tables, _edges) = SdpGraphBuilder::new().build_from_source(source);

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

        let (tables, _edges) = SdpGraphBuilder::new().build_from_source(source);

        assert!(!tables.is_empty());
        assert_eq!(tables[0].source_types[0], SdpSourceType::SdpRead);
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

        let (tables, _edges) = SdpGraphBuilder::new().build_from_source(source);

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

        let (tables, _edges) = SdpGraphBuilder::new().build_from_source(source);

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

        let (tables, _edges) = SdpGraphBuilder::new().build_from_source(source);

        assert!(!tables.is_empty());
        assert_eq!(tables[0].source_types[0], SdpSourceType::SdpRead);
    }

    #[test]
    fn test_build_dp_read_with_alias() {
        let source = r#"
import dp as d

@d.table
def my_table():
    return d.read_csv("data.csv")
"#;

        let (tables, _edges) = SdpGraphBuilder::new().build_from_source(source);

        assert!(!tables.is_empty());
        assert_eq!(tables[0].source_types[0], SdpSourceType::DpRead);
    }

    #[test]
    fn dlt_table_attaches_output_ref_to_each_inner_node() {
        let source = r#"
import dlt

@dlt.table
def gold_users():
    return spark.read.parquet("s3://b/raw/users")
"#;
        let (tables, _) = SdpGraphBuilder::new().build_from_source(source);
        assert_eq!(tables.len(), 1);
        let t = &tables[0];
        assert!(
            !t.inner_nodes.is_empty(),
            "expected at least one inner node"
        );
        for n in &t.inner_nodes {
            let fqns: Vec<String> = n.tables_referenced.iter().map(|r| r.fqn()).collect();
            assert!(
                fqns.contains(&"gold_users".to_string()),
                "inner node {} missing output ref: {:?}",
                n.id,
                fqns
            );
        }
    }

    #[test]
    fn live_ref_attaches_temp_view_ref_to_inner_nodes() {
        let source = r#"
import dlt

@dlt.table
def upstream():
    return spark.read.parquet("s3://b/raw/upstream")

@dlt.table
def downstream():
    return spark.read.parquet("hint").join(LIVE.upstream, "id")
"#;
        let (tables, _) = SdpGraphBuilder::new().build_from_source(source);
        let downstream = tables
            .iter()
            .find(|t| t.name == "downstream")
            .expect("downstream table");
        let any_live_ref = downstream.inner_nodes.iter().any(|n| {
            n.tables_referenced
                .iter()
                .any(|r| r.is_temp_view() && r.table() == "upstream")
        });
        assert!(
            any_live_ref,
            "no LIVE.upstream temp_view ref found on downstream inner nodes"
        );
    }
}
