use std::collections::HashMap;

use crate::graph::sql::extract_table_refs;
use crate::resolved::ast_shape::{
    AstArg, AstNode, AstShape, CallNode, ComprehensionKind, FStringPart, LitKind,
};
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
        self.visit_node_with_loop(node, source, false);
    }

    fn visit_node_with_loop(&mut self, node: &TsNode, source: &str, in_loop: bool) {
        let next_in_loop = in_loop
            || matches!(node.kind(), "for_statement" | "while_statement");

        match node.kind() {
            "assignment" => {
                self.handle_assignment(node, source, next_in_loop);
            }
            "call" => {
                self.handle_call(node, source, next_in_loop);
            }
            _ => {}
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.visit_node_with_loop(&child, source, next_in_loop);
        }
    }

    fn handle_assignment(&mut self, node: &TsNode, source: &str, in_loop: bool) {
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
                        let node_id = self.handle_spark_call(rhs, source, line, in_loop);
                        if let Some(node_id) = node_id {
                            self.bindings.insert(var_name, node_id);
                        }
                    }
                }
            }
        }
    }

    fn handle_call(&mut self, node: &TsNode, source: &str, in_loop: bool) -> Option<String> {
        let line = node.start_position().row as u32 + 1;
        self.handle_spark_call(node, source, line, in_loop)
    }

    fn handle_spark_call(&mut self, node: &TsNode, source: &str, line: u32, in_loop: bool) -> Option<String> {
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
            } else if call_text.contains("spark.sql(") || call_text.contains("spark.sql ") {
                // SQL execution call site — classified as Read for graph
                // purposes (its result is a DataFrame). Treated separately
                // from spark.read so BN002 can target it specifically via
                // the DSL `(ast/Call :method-chain ["spark" "sql"])`.
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
                || call_text.contains(".toPandas")
                || call_text.contains(".to_pandas")
                || call_text.contains(".toJSON")
                || call_text.contains(".count(")
                || call_text.contains(".count ")
                || call_text.contains(".first(")
                || call_text.contains(".head(")
                || call_text.contains(".explain")
                || call_text.contains(".foreach")
                || call_text.contains(".foreachPartition")
                || call_text.contains(".reduce(")
                || call_text.contains(".aggregate(")
                || call_text.contains(".conf.set")
                || call_text.contains("spark.conf.set")
                || call_text.contains(".createDataFrame")
                || call_text.contains("spark.table(")
                || call_text.contains(".saveAsTable")
                || call_text.contains(".start(")
                || call_text.contains("dbutils.")
            {
                (
                    OperationKind::Action,
                    ScalingBehavior::StepFailure,
                    false,
                    false,
                    true,
                )
            } else if call_text.contains(".groupBy")
                || call_text.contains(".join")
                || call_text.contains(".crossJoin")
                || call_text.contains(".union")
                || call_text.contains(".repartition")
                || call_text.contains(".coalesce")
                || call_text.contains(".orderBy")
                || call_text.contains(".sort")
                || call_text.contains(".window")
                || call_text.contains(".distinct")
                || call_text.contains(".dropDuplicates")
                || call_text.contains(".intersect")
                || call_text.contains(".except")
            {
                (
                    OperationKind::Shuffle,
                    ScalingBehavior::LinearWithCliff,
                    false,
                    true,
                    false,
                )
            } else if call_text.contains(".select")
                || call_text.contains(".filter")
                || call_text.contains(".where")
                || call_text.contains(".withColumn")
                || call_text.contains(".withColumns")
                || call_text.contains(".drop(")
                || call_text.contains(".alias")
                || call_text.contains(".cast(")
                || call_text.contains(".limit")
                || call_text.contains(".sample")
                || call_text.contains(".na.")
                || call_text.contains(".fillna")
                || call_text.contains(".dropna")
                || call_text.contains(".replace")
            {
                (
                    OperationKind::Transform,
                    ScalingBehavior::Linear,
                    true,
                    false,
                    false,
                )
            } else if call_text.contains(".cache")
                || call_text.contains(".persist")
                || call_text.contains(".unpersist")
                || call_text.contains(".checkpoint")
            {
                (
                    OperationKind::Maintenance,
                    ScalingBehavior::Linear,
                    false,
                    false,
                    false,
                )
            } else if call_text.contains(".rdd") {
                // .rdd access triggers a serialization fall-back; rules
                // like BNT-A04 target it.
                (
                    OperationKind::Action,
                    ScalingBehavior::StepFailure,
                    false,
                    false,
                    false,
                )
            } else if call_text.contains(".udf(")
                || call_text.contains(".pandas_udf")
                || call_text.contains("udf(")
            {
                (
                    OperationKind::UdfCall,
                    ScalingBehavior::Linear,
                    false,
                    false,
                    false,
                )
            } else {
                return None;
            };

        let refs = extract_refs_from_call(node, source);
        let ast = extract_call_ast(node, source).map(|c| AstShape::new(AstNode::Call(c)));
        let node_id =
            self.create_node(kind, scaling, photon, shuffle, driver, line, Some(call_text), in_loop);
        for tref in refs {
            self.push_table_ref(&node_id, tref);
        }
        if let Some(shape) = ast {
            self.set_ast(&node_id, shape);
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
        in_for_loop: bool,
    ) -> String {
        // nodes.len() before push equals the 0-based index of the new node,
        // so +1 gives a stable 1-based ID without a separate counter field.
        let node_id = format!("node_{}", self.nodes.len() + 1);

        let mut scope = crate::resolved::ScopeFacts::default();
        scope.in_for_loop = in_for_loop;

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
            ast: None,
            scope,
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

    /// Attach an `AstShape` to a previously created node by id.
    fn set_ast(&mut self, node_id: &str, shape: AstShape) {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.id == node_id) {
            node.ast = Some(shape);
        }
    }
}

// ---------------------------------------------------------------------------
// AST extraction — produces the AstShape::Call payload from a tree-sitter
// `call` node. Rules consume this through the DSL; the tree-sitter Tree
// itself is dropped after the builder finishes.
// ---------------------------------------------------------------------------

/// Build a `CallNode` from a tree-sitter `call` AST node.
///
/// Returns `None` when the call is too malformed to extract a useful shape
/// (no `function` child). All argument shapes the DSL currently inspects
/// are covered; anything unrecognised falls through to [`AstArg::Unknown`]
/// with the raw repr preserved.
pub(crate) fn extract_call_ast(call_node: &TsNode, source: &str) -> Option<CallNode> {
    let func = call_node.child_by_field_name("function")?;
    let method_chain = extract_method_chain(&func, source);

    let mut args: Vec<AstArg> = Vec::new();
    let mut kwargs: Vec<(String, AstArg)> = Vec::new();
    if let Some(args_node) = call_node.child_by_field_name("arguments") {
        let mut cursor = args_node.walk();
        for child in args_node.children(&mut cursor) {
            match child.kind() {
                "(" | "," | ")" => continue,
                "keyword_argument" => {
                    if let Some((k, v)) = extract_kwarg(&child, source) {
                        kwargs.push((k, v));
                    }
                }
                _ => args.push(extract_arg(&child, source)),
            }
        }
    }

    let pos = call_node.start_position();
    Some(CallNode {
        method_chain,
        args,
        kwargs,
        line: pos.row as u32 + 1,
        column: pos.column as u32,
    })
}

/// Walk an `attribute` or `identifier` chain and return the dotted parts
/// in source order (`["spark", "read", "parquet"]`).
fn extract_method_chain(func_node: &TsNode, source: &str) -> Vec<String> {
    let mut parts: Vec<String> = Vec::new();
    walk_attribute_chain(func_node, source, &mut parts);
    parts
}

fn walk_attribute_chain(node: &TsNode, source: &str, out: &mut Vec<String>) {
    match node.kind() {
        "identifier" => {
            if let Ok(text) = node.utf8_text(source.as_bytes()) {
                out.push(text.to_string());
            }
        }
        "attribute" => {
            if let Some(obj) = node.child_by_field_name("object") {
                walk_attribute_chain(&obj, source, out);
            }
            if let Some(attr) = node.child_by_field_name("attribute") {
                if let Ok(text) = attr.utf8_text(source.as_bytes()) {
                    out.push(text.to_string());
                }
            }
        }
        _ => {
            // Subscripts, calls-as-receivers, etc. — represent as a single
            // opaque token so downstream callers can at least see something
            // changed in the chain head.
            if let Ok(text) = node.utf8_text(source.as_bytes()) {
                out.push(text.to_string());
            }
        }
    }
}

fn extract_kwarg(node: &TsNode, source: &str) -> Option<(String, AstArg)> {
    let mut cursor = node.walk();
    let children: Vec<TsNode> = node.children(&mut cursor).collect();
    let name = children.iter().find(|c| c.kind() == "identifier")?;
    let key = name.utf8_text(source.as_bytes()).ok()?.to_string();
    let value_node = children
        .iter()
        .find(|c| c.kind() != "identifier" && c.kind() != "=")?;
    Some((key, extract_arg(value_node, source)))
}

/// Convert a single argument AST node into an `AstArg`.
fn extract_arg(node: &TsNode, source: &str) -> AstArg {
    match node.kind() {
        "string" => extract_string_arg(node, source),
        "integer" => {
            if let Ok(text) = node.utf8_text(source.as_bytes()) {
                if let Ok(v) = text.replace('_', "").parse::<i64>() {
                    return AstArg::Literal(LitKind::Int(v));
                }
            }
            AstArg::Unknown {
                repr: node.utf8_text(source.as_bytes()).unwrap_or("").to_string(),
            }
        }
        "float" => {
            if let Ok(text) = node.utf8_text(source.as_bytes()) {
                if let Ok(v) = text.replace('_', "").parse::<f64>() {
                    return AstArg::Literal(LitKind::Float(v));
                }
            }
            AstArg::Unknown {
                repr: node.utf8_text(source.as_bytes()).unwrap_or("").to_string(),
            }
        }
        "true" => AstArg::Literal(LitKind::Bool(true)),
        "false" => AstArg::Literal(LitKind::Bool(false)),
        "none" => AstArg::Literal(LitKind::None),
        "identifier" => AstArg::Identifier(
            node.utf8_text(source.as_bytes())
                .unwrap_or("")
                .to_string(),
        ),
        "attribute" => {
            let mut parts: Vec<String> = Vec::new();
            walk_attribute_chain(node, source, &mut parts);
            AstArg::Attribute(parts)
        }
        "call" => {
            // Two sub-shapes deserve special-casing:
            //   "...".format(x) — DotFormat
            //   any other call    — Call
            if is_dot_format_call(node, source) {
                return extract_dot_format(node, source);
            }
            extract_call_ast(node, source)
                .map(|c| AstArg::Call(Box::new(c)))
                .unwrap_or_else(|| AstArg::Unknown {
                    repr: node.utf8_text(source.as_bytes()).unwrap_or("").to_string(),
                })
        }
        "binary_operator" => extract_binary_op(node, source),
        "list_comprehension" | "set_comprehension" | "dictionary_comprehension"
        | "generator_expression" => extract_comprehension(node, source),
        _ => AstArg::Unknown {
            repr: node.utf8_text(source.as_bytes()).unwrap_or("").to_string(),
        },
    }
}

/// Inspect a `string` node and emit either a plain literal or an f-string.
fn extract_string_arg(node: &TsNode, source: &str) -> AstArg {
    let mut cursor = node.walk();
    let mut text = String::new();
    let mut parts: Vec<FStringPart> = Vec::new();
    let mut has_interp = false;
    let mut pending_text = String::new();

    for child in node.children(&mut cursor) {
        match child.kind() {
            "string_start" | "string_end" => {}
            "interpolation" => {
                has_interp = true;
                if !pending_text.is_empty() {
                    parts.push(FStringPart::Text(std::mem::take(&mut pending_text)));
                }
                // The interpolation contains an `expression` child; serialise
                // the full text minus surrounding `{` `}` for greppability.
                let raw = child.utf8_text(source.as_bytes()).unwrap_or("");
                let trimmed = raw.trim_start_matches('{').trim_end_matches('}').trim();
                parts.push(FStringPart::Interpolation {
                    expr: trimmed.to_string(),
                });
            }
            "string_content" | "escape_sequence" => {
                let piece = child.utf8_text(source.as_bytes()).unwrap_or("");
                pending_text.push_str(piece);
                if !has_interp {
                    text.push_str(piece);
                }
            }
            _ => {}
        }
    }
    if has_interp {
        if !pending_text.is_empty() {
            parts.push(FStringPart::Text(pending_text));
        }
        AstArg::FString { parts }
    } else {
        AstArg::Literal(LitKind::String(text))
    }
}

/// `"...%s..." % (x,)` is a `binary_operator` with op `%` and a string LHS.
/// Other binary operators map straight to `AstArg::BinaryOp`.
fn extract_binary_op(node: &TsNode, source: &str) -> AstArg {
    let mut cursor = node.walk();
    let children: Vec<TsNode> = node.children(&mut cursor).collect();

    // tree-sitter Python lays out children as [lhs, operator, rhs] with the
    // operator either as a typed node ("+", "-", …) or as an unnamed token.
    let (lhs_node, op_node, rhs_node) = match find_binary_parts(&children) {
        Some(triple) => triple,
        None => {
            return AstArg::Unknown {
                repr: node.utf8_text(source.as_bytes()).unwrap_or("").to_string(),
            };
        }
    };

    let op = op_node
        .utf8_text(source.as_bytes())
        .unwrap_or("")
        .to_string();
    let lhs = extract_arg(&lhs_node, source);
    let rhs = extract_arg(&rhs_node, source);

    // Python-style percent format: "...%s..." % args
    if op == "%" {
        if let AstArg::Literal(LitKind::String(template)) = &lhs {
            return AstArg::PercentFormat {
                template: template.clone(),
                args: vec![rhs],
            };
        }
    }

    AstArg::BinaryOp {
        op,
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
    }
}

fn find_binary_parts<'tree>(
    children: &[TsNode<'tree>],
) -> Option<(TsNode<'tree>, TsNode<'tree>, TsNode<'tree>)> {
    // Filter to non-trivial children; the operator is the middle one.
    let real: Vec<TsNode> = children.iter().copied().collect();
    if real.len() < 3 {
        return None;
    }
    let lhs = *real.first()?;
    let op = *real.get(real.len() / 2)?;
    let rhs = *real.last()?;
    Some((lhs, op, rhs))
}

fn is_dot_format_call(node: &TsNode, source: &str) -> bool {
    let Some(func) = node.child_by_field_name("function") else {
        return false;
    };
    if func.kind() != "attribute" {
        return false;
    }
    let Some(attr) = func.child_by_field_name("attribute") else {
        return false;
    };
    attr.utf8_text(source.as_bytes()).unwrap_or("") == "format"
}

fn extract_dot_format(node: &TsNode, source: &str) -> AstArg {
    let Some(func) = node.child_by_field_name("function") else {
        return AstArg::Unknown {
            repr: node.utf8_text(source.as_bytes()).unwrap_or("").to_string(),
        };
    };
    let Some(template_node) = func.child_by_field_name("object") else {
        return AstArg::Unknown {
            repr: node.utf8_text(source.as_bytes()).unwrap_or("").to_string(),
        };
    };
    let template_text = match extract_arg(&template_node, source) {
        AstArg::Literal(LitKind::String(s)) => s,
        _ => template_node
            .utf8_text(source.as_bytes())
            .unwrap_or("")
            .to_string(),
    };

    let mut args: Vec<AstArg> = Vec::new();
    let mut kwargs: Vec<(String, AstArg)> = Vec::new();
    if let Some(args_node) = node.child_by_field_name("arguments") {
        let mut cursor = args_node.walk();
        for child in args_node.children(&mut cursor) {
            match child.kind() {
                "(" | "," | ")" => continue,
                "keyword_argument" => {
                    if let Some((k, v)) = extract_kwarg(&child, source) {
                        kwargs.push((k, v));
                    }
                }
                _ => args.push(extract_arg(&child, source)),
            }
        }
    }

    AstArg::DotFormat {
        template: template_text,
        args,
        kwargs,
    }
}

fn extract_comprehension(node: &TsNode, source: &str) -> AstArg {
    let kind = match node.kind() {
        "list_comprehension" => ComprehensionKind::List,
        "set_comprehension" => ComprehensionKind::Set,
        "dictionary_comprehension" => ComprehensionKind::Dict,
        _ => ComprehensionKind::Generator,
    };
    // The tree-sitter Python grammar exposes a `for_in_clause` child carrying
    // `left` (target) and `right` (iter) fields. Look for one.
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "for_in_clause" {
            let target = child
                .child_by_field_name("left")
                .and_then(|n| n.utf8_text(source.as_bytes()).ok())
                .unwrap_or("")
                .to_string();
            let iter_node = child.child_by_field_name("right");
            let iter = match iter_node {
                Some(n) => extract_arg(&n, source),
                None => AstArg::Unknown {
                    repr: String::new(),
                },
            };
            return AstArg::Comprehension {
                kind,
                target,
                iter: Box::new(iter),
            };
        }
    }
    AstArg::Unknown {
        repr: node.utf8_text(source.as_bytes()).unwrap_or("").to_string(),
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

    // ----- AST shape coverage -----
    use crate::resolved::ast_shape::{AstArg, AstNode, FStringPart, LitKind};

    fn first_call_ast(source: &str) -> CallNode {
        let (nodes, _, _) = PythonGraphBuilder::new().build_from_source(source);
        let node = nodes
            .into_iter()
            .find(|n| n.ast.is_some())
            .expect("at least one node should have an ast");
        match node.ast.unwrap().root {
            AstNode::Call(c) => c,
            other => panic!("expected Call, got {:?}", other),
        }
    }

    #[test]
    fn ast_shape_captures_method_chain() {
        let c = first_call_ast(r#"spark.read.parquet("s3://b/k")"#);
        assert_eq!(c.method_chain, vec!["spark", "read", "parquet"]);
        assert_eq!(c.method(), "parquet");
        assert!(c.starts_with("spark"));
    }

    #[test]
    fn ast_shape_captures_string_literal_arg() {
        let c = first_call_ast(r#"spark.read.parquet("s3://b/k")"#);
        match c.args.first().expect("one positional arg") {
            AstArg::Literal(LitKind::String(s)) => assert_eq!(s, "s3://b/k"),
            other => panic!("expected string literal, got {:?}", other),
        }
    }

    #[test]
    fn ast_shape_captures_fstring_with_interpolations() {
        let c = first_call_ast(r#"spark.sql(f"SELECT * FROM {t}")"#);
        let arg = c.args.first().expect("one arg");
        match arg {
            AstArg::FString { parts } => {
                let text_count = parts.iter().filter(|p| matches!(p, FStringPart::Text(_))).count();
                let interp_count = parts
                    .iter()
                    .filter(|p| matches!(p, FStringPart::Interpolation { .. }))
                    .count();
                assert!(text_count >= 1, "expected text part(s)");
                assert_eq!(interp_count, 1);
                let interp = parts
                    .iter()
                    .find_map(|p| match p {
                        FStringPart::Interpolation { expr } => Some(expr.as_str()),
                        _ => None,
                    })
                    .unwrap();
                assert_eq!(interp, "t");
            }
            other => panic!("expected FString, got {:?}", other),
        }
    }

    #[test]
    fn ast_shape_captures_dot_format_template_and_args() {
        // df.collect("...".format(x)) — outer call is Action; inner format
        // is the first positional arg.
        let c = first_call_ast(r#"df.collect("SELECT {}".format(x))"#);
        match c.args.first().expect("one arg") {
            AstArg::DotFormat { template, args, .. } => {
                assert_eq!(template, "SELECT {}");
                assert_eq!(args.len(), 1);
                assert!(matches!(args[0], AstArg::Identifier(ref s) if s == "x"));
            }
            other => panic!("expected DotFormat, got {:?}", other),
        }
    }

    #[test]
    fn ast_shape_captures_percent_format_template() {
        let c = first_call_ast(r#"spark.sql("SELECT %s" % x)"#);
        match c.args.first().expect("one arg") {
            AstArg::PercentFormat { template, args } => {
                assert_eq!(template, "SELECT %s");
                assert_eq!(args.len(), 1);
                assert!(matches!(args[0], AstArg::Identifier(ref s) if s == "x"));
            }
            other => panic!("expected PercentFormat, got {:?}", other),
        }
    }

    #[test]
    fn ast_shape_captures_binary_op_concatenation() {
        let c = first_call_ast(r#"spark.sql("SELECT " + table)"#);
        match c.args.first().expect("one arg") {
            AstArg::BinaryOp { op, lhs, rhs } => {
                assert_eq!(op, "+");
                assert!(matches!(**lhs, AstArg::Literal(LitKind::String(ref s)) if s == "SELECT "));
                assert!(matches!(**rhs, AstArg::Identifier(ref s) if s == "table"));
            }
            other => panic!("expected BinaryOp, got {:?}", other),
        }
    }

    #[test]
    fn ast_shape_captures_kwargs() {
        let c = first_call_ast(r#"spark.read.parquet("s3://b", mode="overwrite")"#);
        assert_eq!(c.kwargs.len(), 1);
        assert_eq!(c.kwargs[0].0, "mode");
        assert!(matches!(
            c.kwargs[0].1,
            AstArg::Literal(LitKind::String(ref s)) if s == "overwrite"
        ));
    }
}
