use std::collections::HashMap;

use tree_sitter::Node;

use crate::resolved::ast_shape::{AstNode, AstShape, SqlExpr, SqlStatementKind, SqlStatementNode};
use crate::types::{Edge, Node as GraphNode, OperationKind, ScalingBehavior, TableRef};

// ---- Low-level CST helpers ------------------------------------------------

fn node_text<'a>(node: Node, source: &'a [u8]) -> &'a str {
    std::str::from_utf8(&source[node.byte_range()]).unwrap_or("")
}

fn object_ref_to_table_ref(node: Node, source: &[u8]) -> Option<TableRef> {
    if node.kind() != "object_reference" {
        return None;
    }
    let name = node
        .child_by_field_name("name")
        .map(|n| node_text(n, source).to_string())?;
    let schema = node
        .child_by_field_name("schema")
        .map(|n| node_text(n, source).to_string());
    // tree-sitter-sequel calls the catalog part "database"
    let catalog = node
        .child_by_field_name("database")
        .map(|n| node_text(n, source).to_string());
    Some(TableRef::from_parts(catalog, schema, name))
}

/// Collect table refs for a SELECT/INSERT/CTAS context.
///
/// - `ctx_node`: the node owning the FROM clause (inner stmt, create_query, insert)
/// - `extra_nodes`: program-level ERROR siblings of the outer `statement` node —
///   tree-sitter puts `JOIN t` (no ON clause) there, not inside the statement
fn collect_stmt_table_refs<'a>(
    ctx_node: Node<'a>,
    extra_nodes: &[Node<'a>],
    source: &[u8],
) -> Vec<TableRef> {
    let mut refs = Vec::new();
    let mut cursor = ctx_node.walk();
    for child in ctx_node.children(&mut cursor) {
        if child.kind() == "from" || child.is_error() {
            collect_relations_recursive(child, source, &mut refs);
        }
    }
    // Also collect from any program-level ERROR siblings (e.g., JOIN without ON)
    for &extra in extra_nodes {
        collect_relations_recursive(extra, source, &mut refs);
    }
    refs
}

fn collect_relations_recursive(node: Node, source: &[u8], out: &mut Vec<TableRef>) {
    if node.kind() == "relation" {
        let mut cursor = node.walk();
        for c in node.children(&mut cursor) {
            if c.kind() == "object_reference" {
                if let Some(r) = object_ref_to_table_ref(c, source) {
                    out.push(r);
                }
                return;
            }
        }
        return;
    }
    // Don't recurse into these — they contain column refs, not table refs
    if matches!(
        node.kind(),
        "subquery" | "where" | "group_by" | "order_by" | "having" | "limit"
    ) {
        return;
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_relations_recursive(child, source, out);
    }
}

/// Returns true if a SELECT context node has a GROUP BY anywhere in its FROM clause.
fn ctx_has_group_by(ctx_node: Node) -> bool {
    let Some(from) = find_child(ctx_node, "from") else {
        return false;
    };
    let mut cursor = from.walk();
    let result = from.children(&mut cursor).any(|c| c.kind() == "group_by");
    result
}

/// Returns true if a SELECT context has any JOIN, including program-level
/// ERROR siblings (tree-sitter puts `JOIN t` with no ON clause there).
fn ctx_has_join(ctx_node: Node, extra_nodes: &[Node]) -> bool {
    fn has_join_keyword(node: Node) -> bool {
        if node.kind() == "keyword_join" {
            return true;
        }
        if node.kind() == "subquery" {
            return false;
        }
        let mut cursor = node.walk();
        let result = node.children(&mut cursor).any(|c| has_join_keyword(c));
        result
    }
    let mut cursor = ctx_node.walk();
    if ctx_node.children(&mut cursor).any(|c| has_join_keyword(c)) {
        return true;
    }
    extra_nodes.iter().any(|n| has_join_keyword(*n))
}

fn find_child<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    let result = node.children(&mut cursor).find(|c| c.kind() == kind);
    result
}

// ---- Predicate/expression conversion --------------------------------------

fn convert_expr(node: Node, source: &[u8]) -> SqlExpr {
    match node.kind() {
        "binary_expression" => convert_binary_expr(node, source),
        _ => SqlExpr::Other(node_text(node, source).to_string()),
    }
}

fn convert_binary_expr(node: Node, source: &[u8]) -> SqlExpr {
    let left = node.child_by_field_name("left");
    let right = node.child_by_field_name("right");
    let op_node = node.child_by_field_name("operator");
    // Note: op_node.kind() is &'static str for named nodes, but anonymous
    // comparison operators (>, <, =, etc.) also appear here as unnamed nodes.
    let op_kind = op_node.map(|n| n.kind());

    match op_kind {
        Some("keyword_and") => {
            let lhs = left
                .map(|n| convert_expr(n, source))
                .unwrap_or(SqlExpr::Other(String::new()));
            let rhs = right
                .map(|n| convert_expr(n, source))
                .unwrap_or(SqlExpr::Other(String::new()));
            SqlExpr::Logical {
                op: "AND".into(),
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            }
        }
        Some("keyword_or") => {
            let lhs = left
                .map(|n| convert_expr(n, source))
                .unwrap_or(SqlExpr::Other(String::new()));
            let rhs = right
                .map(|n| convert_expr(n, source))
                .unwrap_or(SqlExpr::Other(String::new()));
            SqlExpr::Logical {
                op: "OR".into(),
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            }
        }
        Some("not_in") => {
            let lhs = left
                .map(|n| node_text(n, source).to_string())
                .unwrap_or_default();
            match right {
                Some(r) if r.kind() == "subquery" => SqlExpr::InSubquery {
                    lhs,
                    subquery: node_text(r, source).to_string(),
                    negated: true,
                },
                Some(r) => SqlExpr::InList {
                    lhs,
                    items: vec![node_text(r, source).to_string()],
                    negated: true,
                },
                None => SqlExpr::Other(node_text(node, source).to_string()),
            }
        }
        Some("keyword_in") => {
            let lhs = left
                .map(|n| node_text(n, source).to_string())
                .unwrap_or_default();
            match right {
                Some(r) if r.kind() == "subquery" => SqlExpr::InSubquery {
                    lhs,
                    subquery: node_text(r, source).to_string(),
                    negated: false,
                },
                Some(r) => SqlExpr::InList {
                    lhs,
                    items: vec![node_text(r, source).to_string()],
                    negated: false,
                },
                None => SqlExpr::Other(node_text(node, source).to_string()),
            }
        }
        // Anonymous comparison tokens: ">", "<", "=", ">=", "<=", "<>", "!="
        // as well as named keywords like IS, LIKE, etc. that don't match above.
        Some(_) => {
            let lhs_text = left
                .map(|n| node_text(n, source).to_string())
                .unwrap_or_default();
            let rhs_text = right
                .map(|n| node_text(n, source).to_string())
                .unwrap_or_default();
            let op_text = op_node
                .map(|n| node_text(n, source).to_string())
                .unwrap_or_default();
            SqlExpr::Comparison {
                lhs: lhs_text,
                op: op_text,
                rhs: rhs_text,
            }
        }
        None => SqlExpr::Other(node_text(node, source).to_string()),
    }
}

// ---- AstShape extraction --------------------------------------------------

/// `extra_nodes`: program-level ERROR siblings of the outer `statement` (e.g.,
/// `JOIN t` with no ON clause — tree-sitter places these at the program level).
fn extract_ast_shape<'a>(
    stmt_node: Node<'a>,
    extra_nodes: &[Node<'a>],
    source: &[u8],
    line: u32,
) -> Option<SqlStatementNode> {
    let mut cursor = stmt_node.walk();
    let children: Vec<Node> = stmt_node.children(&mut cursor).collect();

    // MERGE: keyword_merge is a direct child of the statement node
    if children.iter().any(|c| c.kind() == "keyword_merge") {
        let obj_refs: Vec<Node> = children
            .iter()
            .filter(|c| c.kind() == "object_reference")
            .copied()
            .collect();
        let target = obj_refs
            .first()
            .and_then(|n| object_ref_to_table_ref(*n, source))
            .map(|t| t.fqn());
        let from = obj_refs
            .get(1)
            .and_then(|n| object_ref_to_table_ref(*n, source))
            .map(|t| t.fqn())
            .into_iter()
            .collect();
        return Some(SqlStatementNode {
            kind: SqlStatementKind::Merge,
            from,
            target,
            predicates: Vec::new(),
            line,
        });
    }

    // Walk children for statement-type discriminant
    for child in &children {
        match child.kind() {
            "create_table" => {
                let target = find_child(*child, "object_reference")
                    .and_then(|n| object_ref_to_table_ref(n, source))
                    .map(|t| t.fqn());
                let (from_fqns, predicates) = child
                    .children(&mut child.walk())
                    .find_map(|c| {
                        if c.kind() == "create_query" {
                            Some(extract_from_fqns_and_predicates(c, &[], source))
                        } else {
                            None
                        }
                    })
                    .unwrap_or_default();
                return Some(SqlStatementNode {
                    kind: SqlStatementKind::CreateTable,
                    from: from_fqns,
                    target,
                    predicates,
                    line,
                });
            }
            "create_view" | "create_materialized_view" => {
                let target = find_child(*child, "object_reference")
                    .and_then(|n| object_ref_to_table_ref(n, source))
                    .map(|t| t.fqn());
                let (from_fqns, predicates) = child
                    .children(&mut child.walk())
                    .find_map(|c| {
                        if c.kind() == "create_query" {
                            Some(extract_from_fqns_and_predicates(c, &[], source))
                        } else {
                            None
                        }
                    })
                    .unwrap_or_default();
                return Some(SqlStatementNode {
                    kind: SqlStatementKind::CreateView,
                    from: from_fqns,
                    target,
                    predicates,
                    line,
                });
            }
            "insert" => {
                let target = find_child(*child, "object_reference")
                    .and_then(|n| object_ref_to_table_ref(n, source))
                    .map(|t| t.fqn());
                let (from_fqns, predicates) =
                    extract_from_fqns_and_predicates(*child, &[], source);
                return Some(SqlStatementNode {
                    kind: SqlStatementKind::Insert,
                    from: from_fqns,
                    target,
                    predicates,
                    line,
                });
            }
            "from" => {
                // Plain SELECT: pass stmt_node as ctx and extra_nodes to capture
                // program-level ERROR siblings (JOINs without ON clause).
                let (from_fqns, predicates) =
                    extract_from_fqns_and_predicates(stmt_node, extra_nodes, source);
                return Some(SqlStatementNode {
                    kind: SqlStatementKind::Select,
                    from: from_fqns,
                    target: None,
                    predicates,
                    line,
                });
            }
            "statement" => {
                // Recurse into double-wrapped statement; no ERROR siblings at this level.
                if let Some(shape) = extract_ast_shape(*child, &[], source, line) {
                    return Some(shape);
                }
            }
            _ => {}
        }
    }
    None
}

/// Extract table FQNs and WHERE predicates from a context node that owns a
/// FROM clause (inner statement, `create_query`, or `insert`).
/// `extra_nodes` carries program-level ERROR siblings for the SELECT path.
fn extract_from_fqns_and_predicates<'a>(
    ctx_node: Node<'a>,
    extra_nodes: &[Node<'a>],
    source: &[u8],
) -> (Vec<String>, Vec<SqlExpr>) {
    let fqns: Vec<String> = collect_stmt_table_refs(ctx_node, extra_nodes, source)
        .into_iter()
        .map(|t| t.fqn())
        .collect();

    // Predicates live inside `where`, which is a child of the `from` node.
    let mut predicates = Vec::new();
    if let Some(from_node) = find_child(ctx_node, "from") {
        let mut cursor = from_node.walk();
        for child in from_node.children(&mut cursor) {
            if child.kind() == "where" {
                if let Some(pred) = child.child_by_field_name("predicate") {
                    predicates.push(convert_expr(pred, source));
                }
            }
        }
    }

    (fqns, predicates)
}

// ---- Public helper: walk a SQL string and collect all table references ----

/// Walk a SQL string and collect every distinct table reference.
///
/// Public crate helper so the Python builder can reuse it for inline
/// `spark.sql("…")` literals. Returns an empty Vec if the input produces no
/// parseable statements (graceful degradation — no silent failure).
pub fn extract_table_refs(sql_text: &str) -> Vec<TableRef> {
    let mut parser = tree_sitter::Parser::new();
    if parser
        .set_language(&tree_sitter_sequel::LANGUAGE.into())
        .is_err()
    {
        return Vec::new();
    }
    let Some(tree) = parser.parse(sql_text, None) else {
        return Vec::new();
    };
    let source = sql_text.as_bytes();
    let mut out = Vec::new();
    collect_all_table_refs(tree.root_node(), source, &mut out);
    out
}

fn collect_all_table_refs(node: Node, source: &[u8], out: &mut Vec<TableRef>) {
    match node.kind() {
        "relation" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.kind() == "object_reference" {
                    if let Some(r) = object_ref_to_table_ref(child, source) {
                        out.push(r);
                    }
                    return;
                }
            }
        }
        "create_table" | "create_view" | "create_materialized_view" | "insert" => {
            // The first object_reference is the target table
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.kind() == "object_reference" {
                    if let Some(r) = object_ref_to_table_ref(child, source) {
                        out.push(r);
                    }
                    break;
                }
            }
            // Recurse into children to pick up source tables
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                collect_all_table_refs(child, source, out);
            }
        }
        // Don't recurse into these — they contain column refs, not table refs
        "where" | "binary_expression" | "field" | "assignment" | "when_clause" => {}
        _ => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                collect_all_table_refs(child, source, out);
            }
        }
    }
}

// ---- SqlGraphBuilder ------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SqlGraphBuilder {
    nodes: Vec<GraphNode>,
    edges: Vec<Edge>,
    table_definitions: HashMap<String, String>,
    table_references: HashMap<String, Vec<String>>,
}

impl SqlGraphBuilder {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            table_definitions: HashMap::new(),
            table_references: HashMap::new(),
        }
    }

    pub fn build_from_source(mut self, source: &str) -> (Vec<GraphNode>, Vec<Edge>) {
        let mut parser = tree_sitter::Parser::new();
        if parser
            .set_language(&tree_sitter_sequel::LANGUAGE.into())
            .is_err()
        {
            return (Vec::new(), Vec::new());
        }
        let Some(tree) = parser.parse(source, None) else {
            return (Vec::new(), Vec::new());
        };

        let src = source.as_bytes();
        let root = tree.root_node();
        // Collect all program-level children up front so we can look ahead for
        // ERROR siblings. `JOIN t` with no ON clause appears as an ERROR node
        // that is a sibling of the outer `statement` at the program level.
        let mut root_cursor = root.walk();
        let program_children: Vec<tree_sitter::Node> =
            root.children(&mut root_cursor).collect();
        let mut stmt_idx: u32 = 0;
        let mut i = 0;

        while i < program_children.len() {
            let outer_stmt = program_children[i];
            if outer_stmt.kind() != "statement" {
                i += 1;
                continue;
            }
            // Collect consecutive ERROR nodes that immediately follow — these are
            // program-level parse errors that belong to this statement.
            let mut j = i + 1;
            while j < program_children.len() && program_children[j].is_error() {
                j += 1;
            }
            let error_siblings = &program_children[i + 1..j];

            let inner = self.find_inner_statement(outer_stmt);
            let stmt_start = self.nodes.len();
            self.process_statement_node(inner, error_siblings, src, stmt_idx);

            if let Some(shape) = extract_ast_shape(inner, error_siblings, src, stmt_idx + 1) {
                let ast = AstShape::new(AstNode::SqlStatement(shape));
                for node in &mut self.nodes[stmt_start..] {
                    node.ast = Some(ast.clone());
                }
            }

            stmt_idx += 1;
            i = j;
        }

        self.create_table_edges();
        (self.nodes, self.edges)
    }

    fn find_inner_statement<'a>(&self, outer: Node<'a>) -> Node<'a> {
        let mut cursor = outer.walk();
        for child in outer.children(&mut cursor) {
            if child.kind() == "statement" {
                return child;
            }
        }
        outer
    }

    fn process_statement_node<'a>(
        &mut self,
        node: Node<'a>,
        extra_nodes: &[Node<'a>],
        source: &[u8],
        stmt_idx: u32,
    ) {
        let mut cursor = node.walk();
        let children: Vec<Node> = node.children(&mut cursor).collect();

        // MERGE has no sub-node — keyword_merge is a direct child
        if children.iter().any(|c| c.kind() == "keyword_merge") {
            self.process_merge(node, source, stmt_idx);
            return;
        }

        for child in &children {
            match child.kind() {
                "create_table" => {
                    self.process_create_table(*child, source, stmt_idx);
                    return;
                }
                "create_view" | "create_materialized_view" => {
                    self.process_create_view(*child, source, stmt_idx);
                    return;
                }
                "insert" => {
                    self.process_insert(*child, source, stmt_idx);
                    return;
                }
                "from" => {
                    // Pass inner stmt as ctx + program-level ERROR siblings so
                    // JOINs without ON (which land at the program level) are found.
                    self.process_from_clause(node, extra_nodes, source, stmt_idx, None);
                    return;
                }
                "statement" => {
                    // Double-wrapped — recurse once (no ERROR siblings at this level)
                    self.process_statement_node(*child, &[], source, stmt_idx);
                    return;
                }
                _ => {}
            }
        }
    }

    fn process_create_table(&mut self, node: Node, source: &[u8], stmt_idx: u32) {
        let target_ref = find_child(node, "object_reference")
            .and_then(|n| object_ref_to_table_ref(n, source));

        let table_name = target_ref
            .as_ref()
            .map(|t| t.fqn())
            .unwrap_or_else(|| "?".to_string());

        let write_id = self.create_node(
            OperationKind::Write,
            ScalingBehavior::Linear,
            false,
            false,
            false,
            stmt_idx + 1,
            Some(format!("CREATE TABLE {table_name}")),
        );

        if let Some(tref) = target_ref {
            self.table_definitions
                .insert(tref.canonical_key(), write_id.clone());
            self.push_table_ref(&write_id, tref);
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            if child.kind() == "create_query" {
                self.process_from_clause(child, &[], source, stmt_idx, Some(&write_id));
                return;
            }
        }
    }

    fn process_create_view(&mut self, node: Node, source: &[u8], stmt_idx: u32) {
        let target_ref = find_child(node, "object_reference")
            .and_then(|n| object_ref_to_table_ref(n, source));

        let view_name = target_ref
            .as_ref()
            .map(|t| t.fqn())
            .unwrap_or_else(|| "?".to_string());

        let write_id = self.create_node(
            OperationKind::Write,
            ScalingBehavior::Linear,
            false,
            false,
            false,
            stmt_idx + 1,
            Some(format!("CREATE VIEW {view_name}")),
        );

        if let Some(tref) = target_ref {
            self.table_definitions
                .insert(tref.canonical_key(), write_id.clone());
            self.push_table_ref(&write_id, tref);
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            if child.kind() == "create_query" {
                self.process_from_clause(child, &[], source, stmt_idx, Some(&write_id));
                return;
            }
        }
    }

    fn process_insert(&mut self, node: Node, source: &[u8], stmt_idx: u32) {
        let target_ref = find_child(node, "object_reference")
            .and_then(|n| object_ref_to_table_ref(n, source));

        let table_name = target_ref
            .as_ref()
            .map(|t| t.fqn())
            .unwrap_or_else(|| "?".to_string());

        let write_id = self.create_node(
            OperationKind::Write,
            ScalingBehavior::Linear,
            false,
            false,
            false,
            stmt_idx + 1,
            Some(format!("INSERT INTO {table_name}")),
        );

        if let Some(tref) = target_ref {
            self.table_definitions
                .insert(tref.canonical_key(), write_id.clone());
            self.push_table_ref(&write_id, tref);
        }

        self.process_from_clause(node, &[], source, stmt_idx, Some(&write_id));
    }

    fn process_merge(&mut self, stmt_node: Node, source: &[u8], stmt_idx: u32) {
        let mut cursor = stmt_node.walk();
        let obj_refs: Vec<Node> = stmt_node
            .children(&mut cursor)
            .filter(|c| c.kind() == "object_reference")
            .collect();

        // First object_reference = MERGE INTO target, second = USING source
        let target_ref = obj_refs
            .first()
            .and_then(|n| object_ref_to_table_ref(*n, source));
        let source_ref = obj_refs
            .get(1)
            .and_then(|n| object_ref_to_table_ref(*n, source));

        let merge_id = self.create_node(
            OperationKind::Write,
            ScalingBehavior::LinearWithCliff,
            false,
            true,
            false,
            stmt_idx + 1,
            Some("MERGE INTO".to_string()),
        );
        if let Some(ref t) = target_ref {
            self.push_table_ref(&merge_id, t.clone());
        }

        let src_read_id = self.create_node(
            OperationKind::Read,
            ScalingBehavior::Linear,
            false,
            false,
            false,
            stmt_idx + 1,
            Some("MERGE source read".to_string()),
        );
        if let Some(s) = source_ref {
            self.push_table_ref(&src_read_id, s);
        }

        let tgt_read_id = self.create_node(
            OperationKind::Read,
            ScalingBehavior::Linear,
            false,
            false,
            false,
            stmt_idx + 1,
            Some("MERGE target read".to_string()),
        );
        if let Some(t) = target_ref {
            self.push_table_ref(&tgt_read_id, t);
        }

        let shuffle_id = self.create_node(
            OperationKind::Shuffle,
            ScalingBehavior::LinearWithCliff,
            false,
            true,
            false,
            stmt_idx + 1,
            Some("MERGE shuffle".to_string()),
        );

        self.create_edge(&src_read_id, &shuffle_id, "data_flow");
        self.create_edge(&tgt_read_id, &shuffle_id, "data_flow");
        self.create_edge(&shuffle_id, &merge_id, "data_flow");
    }

    fn process_from_clause<'a>(
        &mut self,
        ctx_node: Node<'a>,       // inner statement, create_query, or insert
        extra_nodes: &[Node<'a>], // program-level ERROR siblings (JOINs without ON)
        source: &[u8],
        stmt_idx: u32,
        write_node_id: Option<&String>,
    ) {
        let needs_shuffle = ctx_has_group_by(ctx_node) || ctx_has_join(ctx_node, extra_nodes);
        let mut read_nodes: Vec<String> = Vec::new();

        for tref in collect_stmt_table_refs(ctx_node, extra_nodes, source) {
            let label = format!("Read {}", tref.fqn());
            let node_id = self.create_node(
                OperationKind::Read,
                ScalingBehavior::Linear,
                false,
                false,
                false,
                stmt_idx + 1,
                Some(label),
            );
            self.table_references
                .entry(tref.canonical_key())
                .or_default()
                .push(node_id.clone());
            self.push_table_ref(&node_id, tref);
            read_nodes.push(node_id);
        }

        if needs_shuffle {
            let shuffle_id = self.create_node(
                OperationKind::Shuffle,
                ScalingBehavior::LinearWithCliff,
                false,
                true,
                false,
                stmt_idx + 1,
                Some(if ctx_has_group_by(ctx_node) {
                    "GROUP BY shuffle".to_string()
                } else {
                    "Join shuffle".to_string()
                }),
            );
            for read_id in &read_nodes {
                self.create_edge(read_id, &shuffle_id, "data_flow");
            }
            match write_node_id {
                Some(w) => self.create_edge(&shuffle_id, w, "data_flow"),
                None => {
                    let action_id = self.create_node(
                        OperationKind::Action,
                        ScalingBehavior::StepFailure,
                        false,
                        false,
                        true,
                        stmt_idx + 1,
                        Some("SELECT result".to_string()),
                    );
                    self.create_edge(&shuffle_id, &action_id, "data_flow");
                }
            }
        } else if !read_nodes.is_empty() {
            match write_node_id {
                Some(w) => {
                    for read_id in &read_nodes {
                        self.create_edge(read_id, w, "data_flow");
                    }
                }
                None => {
                    let action_id = self.create_node(
                        OperationKind::Action,
                        ScalingBehavior::StepFailure,
                        false,
                        false,
                        true,
                        stmt_idx + 1,
                        Some("SELECT result".to_string()),
                    );
                    if let Some(first) = read_nodes.first() {
                        self.create_edge(first, &action_id, "data_flow");
                    }
                }
            }
        }
    }

    fn create_table_edges(&mut self) {
        let mut edges_to_create = Vec::new();
        for (table_key, ref_node_ids) in &self.table_references {
            if let Some(def_node_id) = self.table_definitions.get(table_key) {
                for ref_node_id in ref_node_ids {
                    edges_to_create.push((
                        def_node_id.clone(),
                        ref_node_id.clone(),
                        "table_dependency".to_string(),
                    ));
                }
            }
        }
        for (source, target, edge_type) in edges_to_create {
            self.create_edge(&source, &target, &edge_type);
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
        let node_id = format!("sql_node_{}", self.nodes.len() + 1);
        self.nodes.push(GraphNode {
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
            scope: crate::resolved::ScopeFacts::default(),
        });
        node_id
    }

    fn create_edge(&mut self, source: &str, target: &str, edge_type: &str) {
        self.edges.push(Edge {
            source: source.to_string(),
            target: target.to_string(),
            edge_type: edge_type.to_string(),
        });
    }

    fn push_table_ref(&mut self, node_id: &str, tref: TableRef) {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.id == node_id) {
            if !node.tables_referenced.contains(&tref) {
                node.tables_referenced.push(tref);
            }
        }
    }
}

impl Default for SqlGraphBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ---- Tests ----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_select_with_group_by() {
        let source = "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id";
        let (nodes, _edges) = SqlGraphBuilder::new().build_from_source(source);
        assert!(!nodes.is_empty());
        let shuffle_nodes: Vec<&GraphNode> = nodes
            .iter()
            .filter(|n| matches!(n.kind, OperationKind::Shuffle))
            .collect();
        assert!(!shuffle_nodes.is_empty());
    }

    #[test]
    fn test_build_create_table_as_select() {
        let source =
            "CREATE TABLE results AS SELECT * FROM users JOIN orders ON users.id = orders.user_id";
        let (nodes, _edges) = SqlGraphBuilder::new().build_from_source(source);
        assert!(!nodes.is_empty());
        assert!(nodes.iter().any(|n| matches!(n.kind, OperationKind::Read)));
        assert!(nodes.iter().any(|n| matches!(n.kind, OperationKind::Write)));
        assert!(nodes
            .iter()
            .any(|n| matches!(n.kind, OperationKind::Shuffle)));
    }

    #[test]
    fn three_part_select_populates_tables_referenced_on_each_read() {
        let source = "SELECT * FROM cat.sch.t JOIN sch.u";
        let (nodes, _) = SqlGraphBuilder::new().build_from_source(source);
        let read_fqns: Vec<String> = nodes
            .iter()
            .filter(|n| matches!(n.kind, OperationKind::Read))
            .flat_map(|n| n.tables_referenced.iter().map(|t| t.fqn()))
            .collect();
        assert_eq!(read_fqns.len(), 2);
        assert!(read_fqns.contains(&"cat.sch.t".to_string()));
        assert!(read_fqns.contains(&"sch.u".to_string()));
    }

    #[test]
    fn create_table_as_select_marks_target_on_write_node_and_source_on_reads() {
        let source = "CREATE TABLE out AS SELECT * FROM src";
        let (nodes, _) = SqlGraphBuilder::new().build_from_source(source);
        let write = nodes
            .iter()
            .find(|n| matches!(n.kind, OperationKind::Write))
            .expect("write node");
        assert_eq!(
            write
                .tables_referenced
                .iter()
                .map(|t| t.fqn())
                .collect::<Vec<_>>(),
            vec!["out".to_string()]
        );
        let read = nodes
            .iter()
            .find(|n| matches!(n.kind, OperationKind::Read))
            .expect("read node");
        assert_eq!(
            read.tables_referenced
                .iter()
                .map(|t| t.fqn())
                .collect::<Vec<_>>(),
            vec!["src".to_string()]
        );
    }

    #[test]
    fn merge_marks_target_on_write_and_target_read_source_on_source_read() {
        let source = "\
MERGE INTO target_tbl t \
USING src_tbl s ON s.id = t.id \
WHEN MATCHED THEN UPDATE SET t.x = s.x";
        let (nodes, _) = SqlGraphBuilder::new().build_from_source(source);
        let write = nodes
            .iter()
            .find(|n| matches!(n.kind, OperationKind::Write))
            .expect("merge write node");
        let write_fqns: Vec<String> = write.tables_referenced.iter().map(|t| t.fqn()).collect();
        assert_eq!(write_fqns, vec!["target_tbl".to_string()]);
        let reads: Vec<&GraphNode> = nodes
            .iter()
            .filter(|n| matches!(n.kind, OperationKind::Read))
            .collect();
        assert_eq!(reads.len(), 2);
        let all_read_fqns: Vec<String> = reads
            .iter()
            .flat_map(|n| n.tables_referenced.iter().map(|t| t.fqn()))
            .collect();
        assert!(all_read_fqns.contains(&"src_tbl".to_string()));
        assert!(all_read_fqns.contains(&"target_tbl".to_string()));
    }

    #[test]
    fn extract_table_refs_handles_select_and_join() {
        let refs = extract_table_refs("SELECT * FROM a.b.c JOIN d");
        let fqns: Vec<String> = refs.iter().map(|t| t.fqn()).collect();
        assert_eq!(fqns, vec!["a.b.c".to_string(), "d".to_string()]);
    }

    #[test]
    fn extract_table_refs_returns_empty_on_parse_error() {
        // tree-sitter is error-tolerant; invalid SQL produces no valid table refs
        let refs = extract_table_refs("not valid sql at all");
        assert!(refs.is_empty());
    }

    #[test]
    fn extract_table_refs_handles_ctas_and_insert() {
        let refs = extract_table_refs("CREATE TABLE out AS SELECT * FROM src");
        let fqns: Vec<String> = refs.iter().map(|t| t.fqn()).collect();
        assert!(fqns.contains(&"out".to_string()));
        assert!(fqns.contains(&"src".to_string()));

        let refs = extract_table_refs("INSERT INTO dst SELECT * FROM origin");
        let fqns: Vec<String> = refs.iter().map(|t| t.fqn()).collect();
        assert!(fqns.contains(&"dst".to_string()));
        assert!(fqns.contains(&"origin".to_string()));
    }

    // ---- SQL AstShape coverage ----

    use crate::resolved::ast_shape::{AstNode, SqlExpr, SqlStatementKind};

    fn first_ast(source: &str) -> AstNode {
        let (nodes, _) = SqlGraphBuilder::new().build_from_source(source);
        nodes
            .into_iter()
            .find(|n| n.ast.is_some())
            .expect("a node should have an ast")
            .ast
            .unwrap()
            .root
    }

    #[test]
    fn select_emits_sql_statement_with_from_fqns() {
        let ast = first_ast("SELECT * FROM cat.s.t JOIN sch.u");
        match ast {
            AstNode::SqlStatement(s) => {
                assert_eq!(s.kind, SqlStatementKind::Select);
                assert!(s.from.contains(&"cat.s.t".to_string()));
                assert!(s.from.contains(&"sch.u".to_string()));
                assert!(s.target.is_none());
            }
            other => panic!("expected SqlStatement, got {:?}", other),
        }
    }

    #[test]
    fn ctas_emits_target_table() {
        let ast = first_ast("CREATE TABLE out AS SELECT * FROM src");
        match ast {
            AstNode::SqlStatement(s) => {
                assert_eq!(s.kind, SqlStatementKind::CreateTable);
                assert_eq!(s.target.as_deref(), Some("out"));
                assert!(s.from.contains(&"src".to_string()));
            }
            other => panic!("expected SqlStatement, got {:?}", other),
        }
    }

    #[test]
    fn where_not_in_subquery_extracted_as_predicate() {
        let ast = first_ast("SELECT * FROM users WHERE id NOT IN (SELECT id FROM banned)");
        match ast {
            AstNode::SqlStatement(s) => {
                assert!(
                    s.predicates
                        .iter()
                        .any(|p| matches!(p, SqlExpr::InSubquery { negated: true, .. })),
                    "expected NOT IN subquery, got {:?}",
                    s.predicates
                );
            }
            other => panic!("expected SqlStatement, got {:?}", other),
        }
    }

    #[test]
    fn where_comparison_extracted_as_predicate() {
        let ast = first_ast("SELECT * FROM users WHERE age > 18");
        match ast {
            AstNode::SqlStatement(s) => {
                let cmp = s
                    .predicates
                    .iter()
                    .find_map(|p| match p {
                        SqlExpr::Comparison { lhs, op, rhs } => {
                            Some((lhs.clone(), op.clone(), rhs.clone()))
                        }
                        _ => None,
                    })
                    .expect("expected a Comparison predicate");
                assert_eq!(cmp.0, "age");
                assert_eq!(cmp.1, ">");
                assert_eq!(cmp.2, "18");
            }
            other => panic!("expected SqlStatement, got {:?}", other),
        }
    }

    #[test]
    fn where_and_combinator_becomes_logical() {
        let ast = first_ast("SELECT * FROM users WHERE age > 18 AND active = true");
        match ast {
            AstNode::SqlStatement(s) => {
                assert!(s
                    .predicates
                    .iter()
                    .any(|p| matches!(p, SqlExpr::Logical { op, .. } if op == "AND")));
            }
            other => panic!("expected SqlStatement, got {:?}", other),
        }
    }

    #[test]
    fn merge_emits_target_and_source_in_ast() {
        let source = "MERGE INTO target_tbl t USING src_tbl s ON s.id = t.id \
                      WHEN MATCHED THEN UPDATE SET t.x = s.x";
        let ast = first_ast(source);
        match ast {
            AstNode::SqlStatement(s) => {
                assert_eq!(s.kind, SqlStatementKind::Merge);
                assert_eq!(s.target.as_deref(), Some("target_tbl"));
                assert!(s.from.contains(&"src_tbl".to_string()));
            }
            other => panic!("expected SqlStatement, got {:?}", other),
        }
    }
}
