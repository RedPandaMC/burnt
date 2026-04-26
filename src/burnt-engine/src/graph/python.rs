use std::collections::HashMap;

use crate::semantic::SemanticModel;
use crate::types::{CostEdge, CostNode, Finding, OperationKind, ScalingBehavior};
use tree_sitter::{Node, Parser};

pub struct PythonGraphBuilder {
    nodes: Vec<CostNode>,
    edges: Vec<CostEdge>,
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
        &mut self,
        source: &str,
    ) -> (Vec<CostNode>, Vec<CostEdge>, Vec<Finding>) {
        let mut parser = Parser::new();
        parser
            .set_language(&tree_sitter_python::LANGUAGE.into())
            .expect("tree-sitter-python grammar failed to load");
        let tree = parser.parse(source, None).expect("tree-sitter failed to parse");
        let root = tree.root_node();

        self.visit_node(&root, source);

        let findings = self.semantic_model.get_findings().to_vec();
        (self.nodes.clone(), self.edges.clone(), findings)
    }

    fn visit_node(&mut self, node: &Node, source: &str) {
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

    fn handle_assignment(&mut self, node: &Node, source: &str) {
        let mut cursor = node.walk();
        let children: Vec<Node> = node.children(&mut cursor).collect();

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

    fn handle_call(&mut self, node: &Node, source: &str) -> Option<String> {
        let line = node.start_position().row as u32 + 1;
        self.handle_spark_call(node, source, line)
    }

    fn handle_spark_call(&mut self, node: &Node, source: &str, line: u32) -> Option<String> {
        let call_text = node.utf8_text(source.as_bytes()).unwrap_or("").to_string();

        if call_text.contains("spark.read") || call_text.contains("spark.readStream") {
            Some(self.create_node(
                OperationKind::Read,
                ScalingBehavior::Linear,
                false,
                false,
                false,
                line,
                Some(call_text),
            ))
        } else if call_text.contains(".write") || call_text.contains(".save") {
            Some(self.create_node(
                OperationKind::Write,
                ScalingBehavior::Linear,
                false,
                false,
                false,
                line,
                Some(call_text),
            ))
        } else if call_text.contains(".collect")
            || call_text.contains(".take")
            || call_text.contains(".show")
        {
            Some(self.create_node(
                OperationKind::Action,
                ScalingBehavior::StepFailure,
                false,
                false,
                true,
                line,
                Some(call_text),
            ))
        } else if call_text.contains(".groupBy") || call_text.contains(".join") {
            Some(self.create_node(
                OperationKind::Shuffle,
                ScalingBehavior::LinearWithCliff,
                false,
                true,
                false,
                line,
                Some(call_text),
            ))
        } else if call_text.contains(".select")
            || call_text.contains(".filter")
            || call_text.contains(".withColumn")
        {
            Some(self.create_node(
                OperationKind::Transform,
                ScalingBehavior::Linear,
                true,
                false,
                false,
                line,
                Some(call_text),
            ))
        } else {
            None
        }
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

        self.nodes.push(CostNode {
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
        self.edges.push(CostEdge {
            source: source.to_string(),
            target: target.to_string(),
            edge_type: edge_type.to_string(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_spark_read() {
        let source = r#"df = spark.read.parquet("s3://bucket/data")"#;

        let mut builder = PythonGraphBuilder::new();
        let (nodes, _edges, _findings) = builder.build_from_source(source);

        let read_nodes: Vec<&CostNode> = nodes
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

        let mut builder = PythonGraphBuilder::new();
        let (nodes, _edges, _findings) = builder.build_from_source(source);

        assert!(!nodes.is_empty());

        assert!(nodes.iter().any(|n| matches!(n.kind, OperationKind::Read)));
        assert!(nodes
            .iter()
            .any(|n| matches!(n.kind, OperationKind::Transform)));
        assert!(nodes
            .iter()
            .any(|n| matches!(n.kind, OperationKind::Write)));
    }

    #[test]
    fn test_semantic_findings_surfaced() {
        let source = r#"
x = spark.read.parquet("path")
x = spark.read.csv("other")
"#;
        let mut builder = PythonGraphBuilder::new();
        let (_nodes, _edges, findings) = builder.build_from_source(source);
        // BN003 should fire for the shadow of `x`
        assert!(
            findings.iter().any(|f| f.code == "BN003"),
            "Expected BN003 shadow finding, got: {:?}",
            findings
        );
    }
}
