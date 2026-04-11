use std::collections::HashMap;
use std::sync::Mutex;

use crate::semantic::SemanticModel;
use crate::types::{CostEdge, CostNode, OperationKind, ScalingBehavior};
use tree_sitter::{Node, Parser};

pub struct PythonGraphBuilder {
    nodes: Vec<CostNode>,
    edges: Vec<CostEdge>,
    node_counter: u32,
    bindings: HashMap<String, String>,
    semantic_model: SemanticModel,
    parser: Mutex<Parser>,
}

impl PythonGraphBuilder {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            node_counter: 0,
            bindings: HashMap::new(),
            semantic_model: SemanticModel::new(),
            parser: Mutex::new(Parser::new()),
        }
    }

    pub fn build_from_source(&mut self, source: &str) -> (Vec<CostNode>, Vec<CostEdge>) {
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

        self.visit_node(&root, source);

        (self.nodes.clone(), self.edges.clone())
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
        // Find variable name
        let mut cursor = node.walk();
        let children: Vec<Node> = node.children(&mut cursor).collect();

        if let Some(left) = children.first() {
            if left.kind() == "identifier" {
                let var_name = left.utf8_text(source.as_bytes()).unwrap_or("").to_string();
                let line = left.start_position().row as u32 + 1;

                // Record the binding
                self.semantic_model.bind(
                    var_name.clone(),
                    crate::semantic::BindingKind::Assignment,
                    line,
                );

                // Look for RHS
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
        // Extract the full call text
        let call_text = node.utf8_text(source.as_bytes()).unwrap_or("").to_string();

        // Check if it's a Spark operation
        if call_text.contains("spark.read") || call_text.contains("spark.readStream") {
            let node_id = self.create_node(
                OperationKind::Read,
                ScalingBehavior::Linear,
                false, // photon_eligible
                false, // shuffle_required
                false, // driver_bound
                line,
                Some(call_text),
            );
            Some(node_id)
        } else if call_text.contains(".write") || call_text.contains(".save") {
            let node_id = self.create_node(
                OperationKind::Write,
                ScalingBehavior::Linear,
                false,
                false,
                false,
                line,
                Some(call_text),
            );
            Some(node_id)
        } else if call_text.contains(".collect")
            || call_text.contains(".take")
            || call_text.contains(".show")
        {
            let node_id = self.create_node(
                OperationKind::Action,
                ScalingBehavior::StepFailure,
                false,
                false,
                true, // driver_bound
                line,
                Some(call_text),
            );
            Some(node_id)
        } else if call_text.contains(".groupBy") || call_text.contains(".join") {
            let node_id = self.create_node(
                OperationKind::Shuffle,
                ScalingBehavior::LinearWithCliff,
                false,
                true, // shuffle_required
                false,
                line,
                Some(call_text),
            );
            Some(node_id)
        } else if call_text.contains(".select")
            || call_text.contains(".filter")
            || call_text.contains(".withColumn")
        {
            let node_id = self.create_node(
                OperationKind::Transform,
                ScalingBehavior::Linear,
                true, // photon_eligible
                false,
                false,
                line,
                Some(call_text),
            );
            Some(node_id)
        } else {
            None
        }
    }

    #[allow(dead_code)]
    fn get_call_info(&self, node: &Node, source: &str) -> Option<(String, String)> {
        let mut cursor = node.walk();
        let children: Vec<Node> = node.children(&mut cursor).collect();

        if let Some(first) = children.first() {
            if first.kind() == "attribute" {
                // Handle attribute chains like spark.read.parquet
                let attr_text = first.utf8_text(source.as_bytes()).ok()?;
                let parts: Vec<&str> = attr_text.split('.').collect();

                if parts.len() >= 2 {
                    let obj = parts[0];
                    let method = parts[parts.len() - 1]; // Last part is the method
                    return Some((obj.to_string(), method.to_string()));
                }
            }
        }
        None
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
        self.node_counter += 1;
        let node_id = format!("node_{}", self.node_counter);

        let node = CostNode {
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
        };

        self.nodes.push(node);
        node_id
    }

    #[allow(dead_code)]
    fn create_edge(&mut self, source: &str, target: &str, edge_type: &str) {
        let edge = CostEdge {
            source: source.to_string(),
            target: target.to_string(),
            edge_type: edge_type.to_string(),
        };
        self.edges.push(edge);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_spark_read() {
        let source = r#"df = spark.read.parquet("s3://bucket/data")"#;

        let mut builder = PythonGraphBuilder::new();
        let (nodes, _edges) = builder.build_from_source(source);

        // Should have at least a read node
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
        let (nodes, _edges) = builder.build_from_source(source);

        assert!(!nodes.is_empty());

        // Should have read, transform, and write nodes
        let read_nodes: Vec<&CostNode> = nodes
            .iter()
            .filter(|n| matches!(n.kind, OperationKind::Read))
            .collect();
        let transform_nodes: Vec<&CostNode> = nodes
            .iter()
            .filter(|n| matches!(n.kind, OperationKind::Transform))
            .collect();
        let write_nodes: Vec<&CostNode> = nodes
            .iter()
            .filter(|n| matches!(n.kind, OperationKind::Write))
            .collect();

        assert!(!read_nodes.is_empty());
        assert!(!transform_nodes.is_empty());
        assert!(!write_nodes.is_empty());
    }
}
