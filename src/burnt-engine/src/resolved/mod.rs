//! Resolved graph — the canonical static `Graph` fused with runtime overlays.
//!
//! This module is the substrate for the dynamic linter: it takes the static
//! AST graph, the Spark REST stage data, and the Catalyst physical plan
//! tree, and yields a single owned type that downstream consumers (the
//! `_check` orchestrator, the future graph-rule layer, the estimator)
//! query for everything they need to know about a workload.
//!
//! # Architectural firewall
//!
//! The PyO3 entry point is exported as `burnt._engine._resolve_graph` with
//! a leading underscore by intent: only the Python `_check._merge_runtime`
//! orchestrator may construct a [`ResolvedGraph`]. CLI display, future
//! rule code, and external integrations consume `CheckResult.resolved`
//! instead of constructing their own.
//!
//! # Identity model
//!
//! The static graph node is canonical. Each [`NodeOverlay`] is keyed by
//! the static node's ID; plan subtrees and stage observations attach to
//! that overlay rather than carrying their own first-class identity. Data
//! that can't be attached (a stage whose line number doesn't fall within
//! ±5 of any static node) lands in [`overlay::Unmatched`] so the merge is
//! lossless.
//!
//! # Module layout
//!
//! ```text
//! resolved/
//!   mod.rs       // ResolvedGraph + public re-exports
//!   ids.rs       // newtype IDs (StaticNodeId, StageId, SqlExecId, PlanNodeId)
//!   overlay.rs   // NodeOverlay, StageObservation, PlanSubtree, Provenance, Unmatched
//!   merge.rs     // ResolvedGraphBuilder + line-number heuristic
//!   error.rs     // ResolveError
//!   python.rs    // PyO3 adapters — the only file that knows about pyo3
//! ```
//!
//! The Rust core is PyO3-free; `cargo test` runs without `maturin develop`.

pub mod ast_shape;
pub mod error;
pub mod ids;
pub mod merge;
pub mod overlay;
pub mod python;
pub mod scope_facts;

use std::collections::HashMap;
use std::sync::Arc;

use crate::graph::Graph;
use crate::types::TableRef;

pub use ast_shape::{
    AssignmentNode, AstArg, AstNode, AstShape, CallNode, ComprehensionKind, DecoratorNode,
    FStringPart, FunctionDefNode, LitKind, SqlExpr, SqlStatementKind, SqlStatementNode,
};
pub use error::ResolveError;
pub use ids::{PlanNodeId, SqlExecId, StageId, StaticNodeId};
pub use merge::{PlanBundle, RawStage, ResolvedGraphBuilder};
pub use overlay::{NodeOverlay, PlanSubtree, Provenance, StageObservation, Unmatched};
pub use scope_facts::{populate_dag_facts, Namespace, ScopeFacts};

/// Lightweight Python-side `TableSpec` payload received via PyO3.
///
/// Owned by the Python enrichment layer (`DESCRIBE TABLE EXTENDED`); attached
/// to a resolved graph through [`ResolvedGraph::with_table_specs`] so all
/// three signals (static / plan / stage / table-spec) live in one type.
#[derive(Debug, Clone)]
pub struct TableSpec {
    pub fqn: String,
    pub size_bytes: Option<u64>,
    pub num_files: Option<u64>,
    pub num_partitions: Option<u64>,
    pub row_count: Option<u64>,
    pub file_format: Option<String>,
    pub location: Option<String>,
    pub is_managed: Option<bool>,
    pub partition_columns: Vec<String>,
}

/// Canonical static graph fused with runtime overlays.
///
/// Build via [`ResolvedGraphBuilder`]; consume via the read methods
/// ([`overlay`], [`overlays`], [`table_spec`], [`unmatched`]) — direct
/// field access is intentionally not exposed so the inner shape can grow
/// without breaking callers.
///
/// [`overlay`]: ResolvedGraph::overlay
/// [`overlays`]: ResolvedGraph::overlays
/// [`table_spec`]: ResolvedGraph::table_spec
/// [`unmatched`]: ResolvedGraph::unmatched
#[derive(Debug, Clone)]
pub struct ResolvedGraph {
    graph: Graph,
    overlays: HashMap<StaticNodeId, NodeOverlay>,
    table_specs: HashMap<String, TableSpec>,
    unmatched: Unmatched,
    /// Raw source text stored for `fact:source` DSL patterns.
    source_text: Option<Arc<str>>,
}

impl ResolvedGraph {
    /// Crate-internal constructor used by [`ResolvedGraphBuilder::build`].
    /// External callers must go through the builder.
    pub(crate) fn from_parts(
        graph: Graph,
        overlays: HashMap<StaticNodeId, NodeOverlay>,
        table_specs: HashMap<String, TableSpec>,
        unmatched: Unmatched,
    ) -> Self {
        Self {
            graph,
            overlays,
            table_specs,
            unmatched,
            source_text: None,
        }
    }

    /// Attach the raw source text for `fact:source` DSL patterns.
    #[must_use]
    pub(crate) fn with_source_text(mut self, source: &str) -> Self {
        self.source_text = Some(Arc::from(source));
        self
    }

    /// Return the raw source text if available.
    #[inline]
    #[must_use]
    pub fn source_text(&self) -> Option<&str> {
        self.source_text.as_deref()
    }

    /// Reference to the canonical static graph.
    #[inline]
    #[must_use]
    pub fn graph(&self) -> &Graph {
        &self.graph
    }

    /// Look up the overlay for a static node by id.
    pub fn overlay(&self, id: impl AsRef<str>) -> Option<&NodeOverlay> {
        self.overlays.get(&StaticNodeId::new(id.as_ref().to_string()))
    }

    /// Iterate `(id, overlay)` pairs in unspecified order.
    pub fn overlays(&self) -> impl Iterator<Item = (&StaticNodeId, &NodeOverlay)> {
        self.overlays.iter()
    }

    /// Look up a table spec by its `TableRef::fqn`.
    #[inline]
    pub fn table_spec(&self, fqn: &str) -> Option<&TableSpec> {
        self.table_specs.get(fqn)
    }

    /// Iterate `(fqn, spec)` pairs in unspecified order.
    pub fn table_specs(&self) -> impl Iterator<Item = (&String, &TableSpec)> {
        self.table_specs.iter()
    }

    /// All signals that the merge couldn't attach.
    #[inline]
    #[must_use]
    pub fn unmatched(&self) -> &Unmatched {
        &self.unmatched
    }

    /// Attach (or replace) the table-spec overlay. This is the one mutating
    /// operation on the resolved graph — used by the Python orchestrator
    /// after `DESCRIBE TABLE EXTENDED` returns.
    #[must_use]
    pub fn with_table_specs(mut self, specs: HashMap<String, TableSpec>) -> Self {
        self.table_specs = specs;
        self
    }

    /// In-place setter used by PyO3 layer where `with_table_specs` would
    /// require unwrapping through `Bound<PyResolvedGraph>`. Kept
    /// `pub(crate)` so only the python module reaches for it.
    pub(crate) fn set_table_specs(&mut self, specs: HashMap<String, TableSpec>) {
        self.table_specs = specs;
    }

    /// All `TableRef`s referenced across every node of the static graph.
    /// Convenience for the Python enrichment layer when it builds the
    /// `DESCRIBE` request list.
    pub fn distinct_table_refs(&self) -> Vec<TableRef> {
        let mut seen: HashMap<String, TableRef> = HashMap::new();
        for node in &self.graph.nodes {
            for tref in &node.tables_referenced {
                seen.entry(tref.fqn()).or_insert_with(|| tref.clone());
            }
        }
        seen.into_values().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Edge, Node, OperationKind, ScalingBehavior};

    fn mk_node(id: &str, line: u32) -> Node {
        Node {
            id: id.to_string(),
            kind: OperationKind::Read,
            scaling_type: ScalingBehavior::Linear,
            photon_eligible: false,
            shuffle_required: false,
            driver_bound: false,
            tables_referenced: Vec::new(),
            estimated_input_bytes: None,
            estimated_cost_usd: None,
            line_number: Some(line),
            source_code: None,
            ast: None,
            scope: crate::resolved::ScopeFacts::default(),
        }
    }

    fn mk_graph() -> Graph {
        Graph {
            nodes: vec![mk_node("n1", 10), mk_node("n2", 20)],
            edges: Vec::<Edge>::new(),
            mode: "python".to_string(),
            confidence: "low".to_string(),
            findings: Vec::new(),
        }
    }

    #[test]
    fn build_with_no_runtime_data_yields_static_only_overlays() {
        let resolved = ResolvedGraphBuilder::new(mk_graph()).build();
        assert_eq!(resolved.graph().nodes.len(), 2);

        for nid in ["n1", "n2"] {
            let ov = resolved.overlay(nid).expect("overlay exists");
            assert_eq!(ov.provenance, Provenance::STATIC);
            assert!(ov.stages.is_empty());
            assert!(ov.plan_subtree.is_none());
        }
        assert!(resolved.unmatched().stages.is_empty());
    }

    #[test]
    fn unattached_stages_land_in_unmatched_bucket() {
        let stage_json = serde_json::json!({
            "stageId": 9,
            "name": "collect at /foo.py:99",
            "inputBytes": 42
        });
        let raw = RawStage::try_from_json(&stage_json).unwrap();
        let resolved = ResolvedGraphBuilder::new(mk_graph())
            .with_stages(vec![raw])
            .build();
        // Today (commit 3) merge is a no-op; every stage routes to unmatched.
        assert_eq!(resolved.unmatched().stages.len(), 1);
        assert_eq!(resolved.unmatched().stages[0].input_bytes, Some(42));
    }

    #[test]
    fn distinct_table_refs_dedupes_by_fqn() {
        let mut g = mk_graph();
        g.nodes[0]
            .tables_referenced
            .push(TableRef::from_dotted("cat.sch.t"));
        g.nodes[1]
            .tables_referenced
            .push(TableRef::from_dotted("cat.sch.t"));
        g.nodes[1]
            .tables_referenced
            .push(TableRef::from_dotted("other"));
        let resolved = ResolvedGraphBuilder::new(g).build();
        let mut fqns: Vec<String> = resolved.distinct_table_refs().iter().map(|t| t.fqn()).collect();
        fqns.sort();
        assert_eq!(fqns, vec!["cat.sch.t".to_string(), "other".to_string()]);
    }
}
