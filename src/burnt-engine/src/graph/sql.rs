use std::collections::HashMap;

use sqlparser::ast::{Join, Query, SetExpr, Statement, TableFactor, TableWithJoins};
use sqlparser::dialect::DatabricksDialect;
use sqlparser::parser::Parser;

use crate::types::{Edge, Node, OperationKind, ScalingBehavior, TableRef};

/// Walk a SQL string and collect every distinct table reference it touches.
///
/// Public crate helper so the Python builder can reuse it when it sees an
/// inline `spark.sql("…")` literal: the same parser path that backs the
/// SQL graph builder also yields its `TableRef`s. Returns an empty Vec on
/// parse failure (mirrors `SqlGraphBuilder::build_from_source` behaviour).
pub fn extract_table_refs(sql_text: &str) -> Vec<TableRef> {
    let Ok(statements) = Parser::parse_sql(&DatabricksDialect {}, sql_text) else {
        return Vec::new();
    };
    let mut refs: Vec<TableRef> = Vec::new();
    for stmt in &statements {
        collect_table_refs_from_statement(stmt, &mut refs);
    }
    refs
}

fn collect_table_refs_from_statement(stmt: &Statement, out: &mut Vec<TableRef>) {
    match stmt {
        Statement::Query(query) => collect_table_refs_from_query(query, out),
        Statement::CreateTable(create) => {
            out.push(TableRef::from_object_name(&create.name));
            if let Some(q) = &create.query {
                collect_table_refs_from_query(q, out);
            }
        }
        Statement::CreateView(create_view) => {
            out.push(TableRef::from_object_name(&create_view.name));
            collect_table_refs_from_query(&create_view.query, out);
        }
        Statement::Insert(insert) => {
            if let sqlparser::ast::TableObject::TableName(name) = &insert.table {
                out.push(TableRef::from_object_name(name));
            }
            if let Some(q) = &insert.source {
                collect_table_refs_from_query(q, out);
            }
        }
        Statement::Explain { statement, .. } => {
            collect_table_refs_from_statement(statement, out);
        }
        _ => {}
    }
}

fn collect_table_refs_from_query(query: &Query, out: &mut Vec<TableRef>) {
    if let SetExpr::Select(select) = &*query.body {
        for table in &select.from {
            collect_table_refs_from_table(table, out);
        }
    }
}

fn collect_table_refs_from_table(table: &TableWithJoins, out: &mut Vec<TableRef>) {
    if let TableFactor::Table { name, .. } = &table.relation {
        out.push(TableRef::from_object_name(name));
    }
    for join in &table.joins {
        if let TableFactor::Table { name, .. } = &join.relation {
            out.push(TableRef::from_object_name(name));
        }
    }
}

#[derive(Debug, Clone)]
pub struct SqlGraphBuilder {
    nodes: Vec<Node>,
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

    pub fn build_from_source(mut self, source: &str) -> (Vec<Node>, Vec<Edge>) {
        let statements = match Parser::parse_sql(&DatabricksDialect {}, source) {
            Ok(stmts) => stmts,
            Err(_) => return (Vec::new(), Vec::new()),
        };

        for (i, stmt) in statements.iter().enumerate() {
            self.process_statement(stmt, i as u32);
        }

        // Create edges between table definitions and references
        self.create_table_edges();

        (self.nodes, self.edges)
    }

    fn process_statement(&mut self, stmt: &Statement, statement_index: u32) {
        match stmt {
            Statement::Query(query) => {
                self.process_query(query, statement_index, None);
            }
            Statement::CreateTable(create_table) => {
                let table_ref = TableRef::from_object_name(&create_table.name);
                let table_name = create_table.name.to_string();
                if let Some(query) = &create_table.query {
                    let write_node_id = self.create_node(
                        OperationKind::Write,
                        ScalingBehavior::Linear,
                        false,
                        false,
                        false,
                        statement_index + 1,
                        Some(format!("CREATE TABLE {}", table_name)),
                    );

                    self.push_table_ref(&write_node_id, table_ref);
                    self.table_definitions
                        .insert(table_name.clone(), write_node_id.clone());

                    self.process_query(query, statement_index, Some(&write_node_id));
                }
            }
            Statement::CreateView(create_view) => {
                let table_ref = TableRef::from_object_name(&create_view.name);
                let view_name = create_view.name.to_string();
                let write_node_id = self.create_node(
                    OperationKind::Write,
                    ScalingBehavior::Linear,
                    false,
                    false,
                    false,
                    statement_index + 1,
                    Some(format!("CREATE VIEW {}", view_name)),
                );

                self.push_table_ref(&write_node_id, table_ref);
                self.table_definitions
                    .insert(view_name.clone(), write_node_id.clone());
                self.process_query(&create_view.query, statement_index, Some(&write_node_id));
            }
            Statement::Merge {
                table: target, source, ..
            } => {
                let target_ref = if let TableFactor::Table { name, .. } = target {
                    Some(TableRef::from_object_name(name))
                } else {
                    None
                };
                let source_ref = if let TableFactor::Table { name, .. } = source {
                    Some(TableRef::from_object_name(name))
                } else {
                    None
                };

                // MERGE INTO creates a write operation with shuffle
                let merge_node_id = self.create_node(
                    OperationKind::Write,
                    ScalingBehavior::LinearWithCliff,
                    false,
                    true, // shuffle_required for MERGE
                    false,
                    statement_index + 1,
                    Some("MERGE INTO".to_string()),
                );
                if let Some(ref t) = target_ref {
                    self.push_table_ref(&merge_node_id, t.clone());
                }

                // MERGE involves reading from source and target
                let read_node_id = self.create_node(
                    OperationKind::Read,
                    ScalingBehavior::Linear,
                    false,
                    false,
                    false,
                    statement_index + 1,
                    Some("MERGE source read".to_string()),
                );
                if let Some(s) = source_ref {
                    self.push_table_ref(&read_node_id, s);
                }

                let read_node_id2 = self.create_node(
                    OperationKind::Read,
                    ScalingBehavior::Linear,
                    false,
                    false,
                    false,
                    statement_index + 1,
                    Some("MERGE target read".to_string()),
                );
                if let Some(t) = target_ref {
                    self.push_table_ref(&read_node_id2, t);
                }

                let shuffle_node_id = self.create_node(
                    OperationKind::Shuffle,
                    ScalingBehavior::LinearWithCliff,
                    false,
                    true,
                    false,
                    statement_index + 1,
                    Some("MERGE shuffle".to_string()),
                );

                // Create edges for MERGE pipeline
                self.create_edge(&read_node_id, &shuffle_node_id, "data_flow");
                self.create_edge(&read_node_id2, &shuffle_node_id, "data_flow");
                self.create_edge(&shuffle_node_id, &merge_node_id, "data_flow");
            }
            Statement::Explain { statement, .. } => {
                if let Statement::Query(query) = &**statement {
                    self.process_query(query, statement_index, None);
                }
            }
            _ => {}
        }
    }

    fn process_query(
        &mut self,
        query: &Query,
        statement_index: u32,
        write_node_id: Option<&String>,
    ) {
        if let SetExpr::Select(select) = &*query.body {
            let mut read_nodes = Vec::new();

            // Process FROM clause
            for table in &select.from {
                self.process_table_with_joins(table, statement_index, &mut read_nodes);
            }

            // Check for GROUP BY to add shuffle
            let has_group_by = !matches!(&select.group_by, sqlparser::ast::GroupByExpr::Expressions(exprs, _) if exprs.is_empty());

            if has_group_by {
                let shuffle_node_id = self.create_node(
                    OperationKind::Shuffle,
                    ScalingBehavior::LinearWithCliff,
                    false,
                    true,
                    false,
                    statement_index + 1,
                    Some("GROUP BY shuffle".to_string()),
                );

                // Connect reads to shuffle
                for read_node_id in &read_nodes {
                    self.create_edge(read_node_id, &shuffle_node_id, "data_flow");
                }

                // If final SELECT (no write), create action node
                if write_node_id.is_none() {
                    let action_node_id = self.create_node(
                        OperationKind::Action,
                        ScalingBehavior::StepFailure,
                        false,
                        false,
                        true, // driver_bound for final result
                        statement_index + 1,
                        Some("SELECT result".to_string()),
                    );

                    self.create_edge(&shuffle_node_id, &action_node_id, "data_flow");
                } else if let Some(write_id) = write_node_id {
                    self.create_edge(&shuffle_node_id, write_id, "data_flow");
                }
            } else if !read_nodes.is_empty() {
                // Simple SELECT without GROUP BY
                if write_node_id.is_none() {
                    let action_node_id = self.create_node(
                        OperationKind::Action,
                        ScalingBehavior::StepFailure,
                        false,
                        false,
                        true,
                        statement_index + 1,
                        Some("SELECT result".to_string()),
                    );

                    // Connect first read to action
                    if let Some(first_read) = read_nodes.first() {
                        self.create_edge(first_read, &action_node_id, "data_flow");
                    }
                } else if let Some(write_id) = write_node_id {
                    // Connect reads to write
                    for read_node_id in &read_nodes {
                        self.create_edge(read_node_id, write_id, "data_flow");
                    }
                }
            }
        }
    }

    fn process_table_with_joins(
        &mut self,
        table: &TableWithJoins,
        statement_index: u32,
        read_nodes: &mut Vec<String>,
    ) {
        match &table.relation {
            TableFactor::Table { name, .. } => {
                let table_ref = TableRef::from_object_name(name);
                let table_name = name.to_string();
                let read_node_id = self.create_node(
                    OperationKind::Read,
                    ScalingBehavior::Linear,
                    false,
                    false,
                    false,
                    statement_index + 1,
                    Some(format!("Read {}", table_name)),
                );

                self.push_table_ref(&read_node_id, table_ref);
                read_nodes.push(read_node_id.clone());

                // Record table reference for edge creation
                self.table_references
                    .entry(table_name)
                    .or_default()
                    .push(read_node_id);
            }
            TableFactor::Derived { .. } => {
                // Subquery - handled by process_query
            }
            _ => {}
        }

        // Process joins
        for join in &table.joins {
            self.process_join(join, statement_index, read_nodes);
        }
    }

    fn process_join(&mut self, join: &Join, statement_index: u32, read_nodes: &mut Vec<String>) {
        if let TableFactor::Table { name, .. } = &join.relation {
            let table_ref = TableRef::from_object_name(name);
            let table_name = name.to_string();
            let read_node_id = self.create_node(
                OperationKind::Read,
                ScalingBehavior::Linear,
                false,
                false,
                false,
                statement_index + 1,
                Some(format!("Join read {}", table_name)),
            );

            self.push_table_ref(&read_node_id, table_ref);
            read_nodes.push(read_node_id.clone());

            // Record table reference
            self.table_references
                .entry(table_name)
                .or_default()
                .push(read_node_id);
        }

        // JOIN creates a shuffle operation
        let _shuffle_node_id = self.create_node(
            OperationKind::Shuffle,
            ScalingBehavior::LinearWithCliff,
            false,
            true,
            false,
            statement_index + 1,
            Some("Join shuffle".to_string()),
        );

        // The shuffle node will be connected later in process_query
    }

    fn create_table_edges(&mut self) {
        let mut edges_to_create = Vec::new();

        for (table_name, reference_node_ids) in &self.table_references {
            if let Some(definition_node_id) = self.table_definitions.get(table_name) {
                for reference_node_id in reference_node_ids {
                    edges_to_create.push((
                        definition_node_id.clone(),
                        reference_node_id.clone(),
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

        let node = Node {
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

    fn create_edge(&mut self, source: &str, target: &str, edge_type: &str) {
        let edge = Edge {
            source: source.to_string(),
            target: target.to_string(),
            edge_type: edge_type.to_string(),
        };
        self.edges.push(edge);
    }

    /// Attach a `TableRef` to a previously created node by id. Dedupes on
    /// `TableRef` equality so the same table appearing twice in a statement
    /// surfaces once per node.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_select_with_group_by() {
        let source = "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id";

        let (nodes, _edges) = SqlGraphBuilder::new().build_from_source(source);

        assert!(!nodes.is_empty());

        let shuffle_nodes: Vec<&Node> = nodes
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

        let read_nodes: Vec<&Node> = nodes
            .iter()
            .filter(|n| matches!(n.kind, OperationKind::Read))
            .collect();
        let write_nodes: Vec<&Node> = nodes
            .iter()
            .filter(|n| matches!(n.kind, OperationKind::Write))
            .collect();
        let shuffle_nodes: Vec<&Node> = nodes
            .iter()
            .filter(|n| matches!(n.kind, OperationKind::Shuffle))
            .collect();

        assert!(!read_nodes.is_empty());
        assert!(!write_nodes.is_empty());
        assert!(!shuffle_nodes.is_empty());
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

        let reads: Vec<&Node> = nodes
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
}
