use std::collections::HashMap;

use crate::graph::sql::extract_table_refs;
use crate::semantic::SemanticModel;
use crate::types::{Edge, Finding, Node, OperationKind, ScalingBehavior, TableRef};
use tree_sitter::{Node as TsNode, Parser};

pub struct PythonGraphBuilder {
    nodes: Vec<Node>,
    edges: Vec<Edge>,
    bindings: HashMap<String, String>,
    semantic_model: SemanticModel,
}

impl PythonGraphBuilder {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            bindings: HashMap::new(),
            semantic_model: SemanticModel::new(),
        }
    }

    /// Returns `(nodes, edges, semantic_findings)`. Semantic findings include
    /// shadow-variable warnings (BN003) accumulated during AST traversal.
    pub fn build_from_source(
        mut self,
        source: &str,
    ) -> (Vec<Node>, Vec<Edge>, Vec<Finding>) {
        let mut parser = Parser::new();
        parser
            .set_language(&tree_sitter_python::LANGUAGE.into())
            .expect("tree-sitter-python grammar failed to load");
        let tree = parser
            .parse(source, None)
            .expect("tree-sitter failed to parse");
        let root = tree.root_node();

        self.visit_node(&root, source);

        let findings = self.semantic_model.get_findings().to_vec();
        (self.nodes, self.edges, findings)
    }

    fn visit_node(&mut self, node: &TsNode, source: &str) {
        match node.kind() {
            "assignment" => {
                self.handle_assignment(node, source);
            }
            "call" => {
                self.handle_call(node, source);
            }
            _ => {}
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.visit_node(&child, source);
        }
    }

    fn handle_assignment(&mut self, node: &TsNode, source: &str) {
        let mut cursor = node.walk();
        let children: Vec<TsNode> = node.children(&mut cursor).collect();

        if let Some(left) = children.first() {
            if left.kind() == "identifier" {
                let var_name = left.utf8_text(source.as_bytes()).unwrap_or("").to_string();
                let line = left.start_position().row as u32 + 1;

                self.semantic_model.bind(
                    var_name.clone(),
                    crate::semantic::BindingKind::Assignment,
                    line,
                );

                if children.len() >= 3 {
                    let rhs = &children[2];
                    if rhs.kind() == "call" {
                        let node_id = self.handle_spark_call(rhs, source, line);
                        if let Some(node_id) = node_id {
                            self.bindings.insert(var_name, node_id);
                        }
                    }
                }
            }
        }
    }

    fn handle_call(&mut self, node: &TsNode, source: &str) -> Option<String> {
        let line = node.start_position().row as u32 + 1;
        self.handle_spark_call(node, source, line)
    }

    fn handle_spark_call(&mut self, node: &TsNode, source: &str, line: u32) -> Option<String> {
        let call_text = node.utf8_text(source.as_bytes()).unwrap_or("").to_string();

        let (kind, scaling, photon, shuffle, driver) =
            if call_text.contains("spark.read") || call_text.contains("spark.readStream") {
                (
                    OperationKind::Read,
                    ScalingBehavior::Linear,
                    false,
                    false,
                    false,
                )
            } else if call_text.contains(".write") || call_text.contains(".save") {
                (
                    OperationKind::Write,
                    ScalingBehavior::Linear,
                    false,
                    false,
                    false,
                )
            } else if call_text.contains(".collect")
                || call_text.contains(".take")
                || call_text.contains(".show")
            {
                (
                    OperationKind::Action,
                    ScalingBehavior::StepFailure,
                    false,
                    false,
                    true,
                )
            } else if call_text.contains(".groupBy") || call_text.contains(".join") {
                (
                    OperationKind::Shuffle,
                    ScalingBehavior::LinearWithCliff,
                    false,
                    true,
                    false,
                )
            } else if call_text.contains(".select")
                || call_text.contains(".filter")
                || call_text.contains(".withColumn")
            {
                (
                    OperationKind::Transform,
                    ScalingBehavior::Linear,
                    true,
                    false,
                    false,
                )
            } else {
                return None;
            };

        let refs = extract_refs_from_call(node, source);
        let node_id =
            self.create_node(kind, scaling, photon, shuffle, driver, line, Some(call_text));
        for tref in refs {
            self.push_table_ref(&node_id, tref);
        }
        Some(node_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn create_node(
        &mut self,
        kind: OperationKind,
        scaling_type: ScalingBehavior,
        photon_eligible: bool,
        shuffle_required: bool,
        driver_bound: bool,
        line: u32,
        source_code: Option<String>,
    ) -> String {
        // nodes.len() before push equals the 0-based index of the new node,
        // so +1 gives a stable 1-based ID without a separate counter field.
        let node_id = format!("node_{}", self.nodes.len() + 1);

        self.nodes.push(Node {
            id: node_id.clone(),
            kind,
            scaling_type,
            photon_eligible,
            shuffle_required,
            driver_bound,
            tables_referenced: Vec::new(),
            estimated_input_bytes: None,
            estimated_cost_usd: None,
            line_number: Some(line),
            source_code,
        });

        node_id
    }

    #[allow(dead_code)]
    fn create_edge(&mut self, source: &str, target: &str, edge_type: &str) {
        self.edges.push(Edge {
            source: source.to_string(),
            target: target.to_string(),
            edge_type: edge_type.to_string(),
        });
    }

    /// Attach a `TableRef` to a previously created node by id, deduping.
    fn push_table_ref(&mut self, node_id: &str, tref: TableRef) {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.id == node_id) {
            if !node.tables_referenced.contains(&tref) {
                node.tables_referenced.push(tref);
            }
        }
    }
}

/// Walk a `call` AST node and extract any `TableRef`s implied by its function
/// and argument shape. Mapping rules:
///
/// - `spark.sql("…")` / `<df>.sql("…")` — parse the string argument as SQL and
///   reuse [`crate::graph::sql::extract_table_refs`] so dynamic and static SQL
///   share the same parser path.
/// - `spark.table("a.b.c")` / `<x>.table("…")` — single dotted ref via
///   [`TableRef::from_dotted`].
/// - `*.saveAsTable("name")` — single dotted ref on the write side.
/// - `spark.read.parquet("path")`, `.load`, `.csv`, `.json`, `.orc`, `.text`
///   and the `.write`/`.save*` mirror — path read via [`TableRef::from_path`].
///
/// F-string and non-literal arguments are skipped (BN002 territory).
/// Returns an empty Vec when nothing matches.
fn extract_refs_from_call(call_node: &TsNode, source: &str) -> Vec<TableRef> {
    let Some(func) = call_node.child_by_field_name("function") else {
        return Vec::new();
    };
    let Some(args) = call_node.child_by_field_name("arguments") else {
        return Vec::new();
    };

    let func_text = func.utf8_text(source.as_bytes()).unwrap_or("");
    let leaf_method = func_text.rsplit('.').next().unwrap_or("");
    let Some(literal) = first_string_literal_arg(&args, source) else {
        return Vec::new();
    };

    match leaf_method {
        "sql" => extract_table_refs(&literal),
        "table" | "saveAsTable" => vec![TableRef::from_dotted(&literal)],
        "parquet" | "load" | "csv" | "json" | "orc" | "text" => {
            vec![TableRef::from_path(&literal)]
        }
        _ => Vec::new(),
    }
}

/// Returns the unescaped value of the first positional string argument, or
/// `None` if the first argument is missing, is an f-string (any
/// `interpolation` child), or is anything other than a plain string literal.
fn first_string_literal_arg(args_node: &TsNode, source: &str) -> Option<String> {
    let mut cursor = args_node.walk();
    for child in args_node.children(&mut cursor) {
        if child.kind() == "(" || child.kind() == "," || child.kind() == ")" {
            continue;
        }
        if child.kind() == "keyword_argument" {
            // skip keyword args entirely; we only look at the first positional
            continue;
        }
        if child.kind() != "string" {
            return None;
        }
        return string_literal_value(&child, source);
    }
    None
}

/// Extract the inner text of a `string` node, returning `None` if the node has
/// any `interpolation` children (i.e. it is an f-string).
fn string_literal_value(string_node: &TsNode, source: &str) -> Option<String> {
    let mut cursor = string_node.walk();
    let mut content = String::new();
    for child in string_node.children(&mut cursor) {
        match child.kind() {
            "string_start" | "string_end" => continue,
            "interpolation" => return None,
            "string_content" | "escape_sequence" => {
                content.push_str(child.utf8_text(source.as_bytes()).unwrap_or(""));
            }
            _ => {}
        }
    }
    Some(content)
}

impl Default for PythonGraphBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_spark_read() {
        let source = r#"df = spark.read.parquet("s3://bucket/data")"#;

        let (nodes, _edges, _findings) = PythonGraphBuilder::new().build_from_source(source);

        let read_nodes: Vec<&Node> = nodes
            .iter()
            .filter(|n| matches!(n.kind, OperationKind::Read))
            .collect();
        assert!(
            !read_nodes.is_empty(),
            "Expected at least one read node, got: {:?}",
            nodes
        );
    }

    #[test]
    fn test_build_spark_transform() {
        let source = r#"
df = spark.read.csv("data.csv")
df2 = df.select("col1", "col2").filter("col1 > 0")
df2.write.mode("overwrite").parquet("output.parquet")
"#;

        let (nodes, _edges, _findings) = PythonGraphBuilder::new().build_from_source(source);

        assert!(!nodes.is_empty());

        assert!(nodes.iter().any(|n| matches!(n.kind, OperationKind::Read)));
        assert!(nodes
            .iter()
            .any(|n| matches!(n.kind, OperationKind::Transform)));
        assert!(nodes.iter().any(|n| matches!(n.kind, OperationKind::Write)));
    }

    #[test]
    fn test_semantic_findings_surfaced() {
        let source = r#"
x = spark.read.parquet("path")
x = spark.read.csv("other")
"#;
        let (_nodes, _edges, findings) = PythonGraphBuilder::new().build_from_source(source);
        // BN003 should fire for the shadow of `x`
        assert!(
            findings.iter().any(|f| f.code == "BN003"),
            "Expected BN003 shadow finding, got: {:?}",
            findings
        );
    }

    fn read_node_table_fqns(nodes: &[Node]) -> Vec<String> {
        nodes
            .iter()
            .filter(|n| matches!(n.kind, OperationKind::Read))
            .flat_map(|n| n.tables_referenced.iter().map(|t| t.fqn()))
            .collect()
    }

    #[test]
    fn spark_read_parquet_extracts_path_ref() {
        let (nodes, _, _) =
            PythonGraphBuilder::new().build_from_source(r#"spark.read.parquet("s3://b/k")"#);
        let fqns = read_node_table_fqns(&nodes);
        assert_eq!(fqns, vec!["path:s3://b/k".to_string()]);
        let r = nodes
            .iter()
            .find(|n| matches!(n.kind, OperationKind::Read))
            .unwrap()
            .tables_referenced
            .first()
            .unwrap();
        assert!(r.is_path_read);
        assert_eq!(r.table, "k");
    }

    #[test]
    fn spark_read_table_extracts_dotted_ref() {
        let (nodes, _, _) =
            PythonGraphBuilder::new().build_from_source(r#"spark.read.table("cat.sch.t")"#);
        let fqns = read_node_table_fqns(&nodes);
        assert_eq!(fqns, vec!["cat.sch.t".to_string()]);
    }

    #[test]
    fn spark_sql_literal_extracts_inner_table_refs_via_helper() {
        // spark.sql doesn't match any of the kind heuristics — it returns no
        // graph node, so refs are dropped at the builder level. Once a future
        // commit adds a graph node for SQL execution call sites, this same
        // helper will populate them. For now assert the helper itself yields
        // the inner SQL's TableRefs.
        let src = r#"spark.sql("SELECT * FROM cat.sch.t JOIN d")"#;
        let mut parser = Parser::new();
        parser
            .set_language(&tree_sitter_python::LANGUAGE.into())
            .unwrap();
        let tree = parser.parse(src, None).unwrap();
        let module = tree.root_node();
        let expr = module.child(0).unwrap();
        let call = expr.child(0).unwrap();

        let refs = super::extract_refs_from_call(&call, src);
        let fqns: Vec<String> = refs.iter().map(|t| t.fqn()).collect();
        assert!(fqns.contains(&"cat.sch.t".to_string()));
        assert!(fqns.contains(&"d".to_string()));
    }

    #[test]
    fn fstring_argument_is_not_extracted() {
        let (nodes, _, _) = PythonGraphBuilder::new()
            .build_from_source(r#"spark.read.parquet(f"s3://{bucket}/k")"#);
        // No literal — no ref attached.
        let read = nodes
            .iter()
            .find(|n| matches!(n.kind, OperationKind::Read))
            .expect("read node still created");
        assert!(read.tables_referenced.is_empty());
    }

    #[test]
    fn write_save_as_table_attaches_to_write_node() {
        let source = r#"df.write.saveAsTable("out_db.events")"#;
        let (nodes, _, _) = PythonGraphBuilder::new().build_from_source(source);
        let write = nodes
            .iter()
            .find(|n| matches!(n.kind, OperationKind::Write))
            .expect("write node");
        let fqns: Vec<String> = write.tables_referenced.iter().map(|t| t.fqn()).collect();
        assert_eq!(fqns, vec!["out_db.events".to_string()]);
    }

    #[test]
    fn write_parquet_path_attaches_to_write_node() {
        let source = r#"df.write.parquet("s3://b/out/")"#;
        let (nodes, _, _) = PythonGraphBuilder::new().build_from_source(source);
        let write = nodes
            .iter()
            .find(|n| matches!(n.kind, OperationKind::Write))
            .expect("write node");
        let fqns: Vec<String> = write.tables_referenced.iter().map(|t| t.fqn()).collect();
        assert_eq!(fqns, vec!["path:s3://b/out/".to_string()]);
    }
}
