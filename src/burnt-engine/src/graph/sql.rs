use std::collections::HashMap;

use sqlparser::ast::{Join, Query, SetExpr, Statement, TableFactor, TableWithJoins};
use sqlparser::dialect::DatabricksDialect;
use sqlparser::parser::Parser;

use crate::types::{CostEdge, CostNode, OperationKind, ScalingBehavior};

#[derive(Debug, Clone)]
pub struct SqlGraphBuilder {
    nodes: Vec<CostNode>,
    edges: Vec<CostEdge>,
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

    pub fn build_from_source(&mut self, source: &str) -> (Vec<CostNode>, Vec<CostEdge>) {
        let statements = match Parser::parse_sql(&DatabricksDialect {}, source) {
            Ok(stmts) => stmts,
            Err(_) => return (Vec::new(), Vec::new()),
        };

        for (i, stmt) in statements.iter().enumerate() {
            self.process_statement(stmt, i as u32);
        }

        // Create edges between table definitions and references
        self.create_table_edges();

        (self.nodes.clone(), self.edges.clone())
    }

    fn process_statement(&mut self, stmt: &Statement, statement_index: u32) {
        match stmt {
            Statement::Query(query) => {
                self.process_query(query, statement_index, None);
            }
            Statement::CreateTable(create_table) => {
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

                    self.table_definitions
                        .insert(table_name.clone(), write_node_id.clone());

                    self.process_query(query, statement_index, Some(&write_node_id));
                }
            }
            Statement::CreateView(create_view) => {
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

                self.table_definitions
                    .insert(view_name.clone(), write_node_id.clone());
                self.process_query(&create_view.query, statement_index, Some(&write_node_id));
            }
            Statement::Merge { .. } => {
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

                let read_node_id2 = self.create_node(
                    OperationKind::Read,
                    ScalingBehavior::Linear,
                    false,
                    false,
                    false,
                    statement_index + 1,
                    Some("MERGE target read".to_string()),
                );

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
    fn test_build_select_with_group_by() {
        let source = "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id";

        let mut builder = SqlGraphBuilder::new();
        let (nodes, _edges) = builder.build_from_source(source);

        assert!(!nodes.is_empty());

        let shuffle_nodes: Vec<&CostNode> = nodes
            .iter()
            .filter(|n| matches!(n.kind, OperationKind::Shuffle))
            .collect();
        assert!(!shuffle_nodes.is_empty());
    }

    #[test]
    fn test_build_create_table_as_select() {
        let source =
            "CREATE TABLE results AS SELECT * FROM users JOIN orders ON users.id = orders.user_id";

        let mut builder = SqlGraphBuilder::new();
        let (nodes, _edges) = builder.build_from_source(source);

        assert!(!nodes.is_empty());

        let read_nodes: Vec<&CostNode> = nodes
            .iter()
            .filter(|n| matches!(n.kind, OperationKind::Read))
            .collect();
        let write_nodes: Vec<&CostNode> = nodes
            .iter()
            .filter(|n| matches!(n.kind, OperationKind::Write))
            .collect();
        let shuffle_nodes: Vec<&CostNode> = nodes
            .iter()
            .filter(|n| matches!(n.kind, OperationKind::Shuffle))
            .collect();

        assert!(!read_nodes.is_empty());
        assert!(!write_nodes.is_empty());
        assert!(!shuffle_nodes.is_empty());
    }
}
