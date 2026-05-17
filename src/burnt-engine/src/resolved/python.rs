//! PyO3 adapters for the `resolved` module.
//!
//! This is the only file in the `resolved` tree that imports `pyo3`. The
//! Rust core types stay testable without a Python interpreter.

use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::graph::PyGraph;
use crate::json_py::value_to_py;
use crate::plan_parser::PlanNode;
use crate::resolved::merge::{PlanBundle, RawStage, ResolvedGraphBuilder};
use crate::resolved::overlay::{NodeOverlay, PlanSubtree, StageObservation};
use crate::resolved::{ResolvedGraph, SqlExecId, TableSpec};
use crate::session::SessionStatePy;

/// Catalyst plan operator surfaced to Python from a [`PlanSubtree`].
///
/// `metrics` are held as their JSON values and materialised into a fresh
/// `PyDict` on every getter call. Materialisation cost is dwarfed by the
/// surrounding analysis work and avoids `Py<PyDict>`'s non-`Clone` shape.
#[pyclass(name = "PlanSubtreeNode")]
#[derive(Clone)]
pub struct PyPlanSubtreeNode {
    #[pyo3(get)]
    pub node_id: i64,
    #[pyo3(get)]
    pub node_name: String,
    #[pyo3(get)]
    pub parent_ids: Vec<i64>,
    metrics_json: std::collections::HashMap<String, serde_json::Value>,
}

#[pymethods]
impl PyPlanSubtreeNode {
    #[getter]
    fn metrics(&self, py: Python<'_>) -> Py<PyDict> {
        let dict = PyDict::new_bound(py);
        for (k, v) in &self.metrics_json {
            dict.set_item(k, value_to_py(py, v))
                .expect("PyDict::set_item failed under stable allocator");
        }
        dict.unbind()
    }

    fn __repr__(&self) -> String {
        format!(
            "PlanSubtreeNode(id={}, name={:?})",
            self.node_id, self.node_name
        )
    }
}

impl PyPlanSubtreeNode {
    fn from_node(n: &PlanNode) -> Self {
        Self {
            node_id: n.node_id,
            node_name: n.node_name.clone(),
            parent_ids: n.parent_ids.clone(),
            metrics_json: n.metrics.clone(),
        }
    }
}

#[pyclass(name = "PlanSubtree")]
#[derive(Clone)]
pub struct PyPlanSubtree {
    #[pyo3(get)]
    pub sql_exec_id: i64,
    #[pyo3(get)]
    pub root: i64,
    #[pyo3(get)]
    pub nodes: Vec<PyPlanSubtreeNode>,
}

impl PyPlanSubtree {
    fn from_subtree(s: &PlanSubtree) -> Self {
        Self {
            sql_exec_id: s.sql_exec_id.into_inner(),
            root: s.root.into_inner(),
            nodes: s.nodes.iter().map(PyPlanSubtreeNode::from_node).collect(),
        }
    }
}

#[pyclass(name = "StageObservation")]
#[derive(Clone)]
pub struct PyStageObservation {
    #[pyo3(get)]
    pub stage_id: i64,
    #[pyo3(get)]
    pub input_bytes: Option<u64>,
    #[pyo3(get)]
    pub shuffle_read_bytes: Option<u64>,
    #[pyo3(get)]
    pub shuffle_write_bytes: Option<u64>,
    #[pyo3(get)]
    pub duration_ms: Option<u64>,
    #[pyo3(get)]
    pub num_tasks: Option<u32>,
    #[pyo3(get)]
    pub source_line: Option<u32>,
}

impl From<&StageObservation> for PyStageObservation {
    fn from(s: &StageObservation) -> Self {
        Self {
            stage_id: s.stage_id.into_inner(),
            input_bytes: s.input_bytes,
            shuffle_read_bytes: s.shuffle_read_bytes,
            shuffle_write_bytes: s.shuffle_write_bytes,
            duration_ms: s.duration_ms,
            num_tasks: s.num_tasks,
            source_line: s.source_line,
        }
    }
}

#[pyclass(name = "NodeOverlay")]
#[derive(Clone)]
pub struct PyNodeOverlay {
    #[pyo3(get)]
    pub stages: Vec<PyStageObservation>,
    #[pyo3(get)]
    pub plan_subtree: Option<PyPlanSubtree>,
    /// Bitflag set: 0b001 STATIC, 0b010 PLAN, 0b100 STAGE.
    #[pyo3(get)]
    pub provenance: u8,
    /// Sum of `input_bytes` across attached stages, materialised at
    /// construction so callers don't need to iterate `.stages` themselves.
    #[pyo3(get)]
    pub observed_input_bytes: Option<u64>,
}

impl PyNodeOverlay {
    fn from_overlay(ov: &NodeOverlay) -> Self {
        Self {
            stages: ov.stages.iter().map(PyStageObservation::from).collect(),
            plan_subtree: ov.plan_subtree.as_ref().map(PyPlanSubtree::from_subtree),
            provenance: ov.provenance.bits(),
            observed_input_bytes: ov.observed_input_bytes(),
        }
    }
}

#[pyclass(name = "TableSpec")]
#[derive(Clone)]
pub struct PyTableSpec {
    #[pyo3(get)]
    pub fqn: String,
    #[pyo3(get)]
    pub size_bytes: Option<u64>,
    #[pyo3(get)]
    pub num_files: Option<u64>,
    #[pyo3(get)]
    pub num_partitions: Option<u64>,
    #[pyo3(get)]
    pub row_count: Option<u64>,
    #[pyo3(get)]
    pub file_format: Option<String>,
    #[pyo3(get)]
    pub location: Option<String>,
    #[pyo3(get)]
    pub is_managed: Option<bool>,
    #[pyo3(get)]
    pub partition_columns: Vec<String>,
}

#[pymethods]
impl PyTableSpec {
    #[new]
    #[pyo3(signature = (
        fqn,
        size_bytes=None,
        num_files=None,
        num_partitions=None,
        row_count=None,
        file_format=None,
        location=None,
        is_managed=None,
        partition_columns=Vec::new(),
    ))]
    #[allow(clippy::too_many_arguments)]
    fn py_new(
        fqn: String,
        size_bytes: Option<u64>,
        num_files: Option<u64>,
        num_partitions: Option<u64>,
        row_count: Option<u64>,
        file_format: Option<String>,
        location: Option<String>,
        is_managed: Option<bool>,
        partition_columns: Vec<String>,
    ) -> Self {
        Self {
            fqn,
            size_bytes,
            num_files,
            num_partitions,
            row_count,
            file_format,
            location,
            is_managed,
            partition_columns,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "TableSpec(fqn={:?}, size_bytes={:?})",
            self.fqn, self.size_bytes
        )
    }
}

impl From<TableSpec> for PyTableSpec {
    fn from(t: TableSpec) -> Self {
        Self {
            fqn: t.fqn,
            size_bytes: t.size_bytes,
            num_files: t.num_files,
            num_partitions: t.num_partitions,
            row_count: t.row_count,
            file_format: t.file_format,
            location: t.location,
            is_managed: t.is_managed,
            partition_columns: t.partition_columns,
        }
    }
}

impl From<&PyTableSpec> for TableSpec {
    fn from(p: &PyTableSpec) -> Self {
        Self {
            fqn: p.fqn.clone(),
            size_bytes: p.size_bytes,
            num_files: p.num_files,
            num_partitions: p.num_partitions,
            row_count: p.row_count,
            file_format: p.file_format.clone(),
            location: p.location.clone(),
            is_managed: p.is_managed,
            partition_columns: p.partition_columns.clone(),
        }
    }
}

#[pyclass(name = "ResolvedGraph")]
pub struct PyResolvedGraph {
    inner: ResolvedGraph,
}

#[pymethods]
impl PyResolvedGraph {
    /// The canonical static graph, cloned on access — the resolved graph
    /// owns the original.
    #[getter]
    fn graph(&self) -> PyGraph {
        self.inner.graph().clone().into()
    }

    /// Overlay for the given static node id, or `None` if no such node.
    fn overlay(&self, node_id: &str) -> Option<PyNodeOverlay> {
        self.inner.overlay(node_id).map(PyNodeOverlay::from_overlay)
    }

    /// All static node ids in arbitrary order. Convenience for Python
    /// iteration without exposing the internal HashMap.
    fn node_ids(&self) -> Vec<String> {
        self.inner
            .overlays()
            .map(|(id, _)| id.as_str().to_string())
            .collect()
    }

    /// Provenance bits for `node_id` (0b001 STATIC | 0b010 PLAN | 0b100 STAGE).
    fn provenance_bits(&self, node_id: &str) -> Option<u8> {
        self.inner.overlay(node_id).map(|ov| ov.provenance.bits())
    }

    /// All table specs keyed by FQN. Returns an empty dict when no specs
    /// have been attached.
    fn table_specs(&self) -> HashMap<String, PyTableSpec> {
        self.inner
            .table_specs()
            .map(|(k, v)| (k.clone(), PyTableSpec::from(v.clone())))
            .collect()
    }

    fn table_spec(&self, fqn: &str) -> Option<PyTableSpec> {
        self.inner.table_spec(fqn).cloned().map(PyTableSpec::from)
    }

    /// Attach (or replace) the table-spec overlay. The single mutating
    /// method on the resolved graph — only `_check._merge_runtime` should
    /// call this. Documented as such.
    fn set_table_specs(&mut self, specs: HashMap<String, PyTableSpec>) {
        let converted: HashMap<String, TableSpec> = specs
            .into_iter()
            .map(|(k, v)| (k, TableSpec::from(&v)))
            .collect();
        self.inner.set_table_specs(converted);
    }

    /// Number of input stages that the merge could not attach to any
    /// static node. Surfaces in diagnostics so users can spot
    /// mis-attribution.
    fn unmatched_stage_count(&self) -> usize {
        self.inner.unmatched().stages.len()
    }

    fn unmatched_plan_bundle_count(&self) -> usize {
        self.inner.unmatched().plan_bundles.len()
    }

    fn __repr__(&self) -> String {
        format!(
            "ResolvedGraph(static_nodes={}, table_specs={}, unmatched_stages={})",
            self.node_ids().len(),
            self.inner.table_specs().count(),
            self.unmatched_stage_count()
        )
    }
}

/// **Internal — only `_check._merge_runtime` may call this.**
///
/// Construct a `ResolvedGraph` from a `PyGraph` and (optionally) a session.
/// Exported with a leading underscore by intent: every other consumer
/// reads `CheckResult.resolved` instead.
///
/// Accepts either a real `SessionStatePy` (production path) or any
/// duck-typed object exposing `.stages` and `.plan_bundles` attributes
/// (test path — `_FakeSession` in `tests/unit/test_check_session_integration`
/// is the canonical example).
#[pyfunction]
#[pyo3(name = "_resolve_graph", signature = (graph, session=None))]
pub fn resolve_graph(
    py: Python<'_>,
    graph: &PyGraph,
    session: Option<&Bound<'_, PyAny>>,
) -> PyResolvedGraph {
    let domain = py_graph_to_domain(graph);
    let (stage_values, bundle_values) = match session {
        None => (Vec::new(), Vec::new()),
        Some(s) => extract_session_values(py, s),
    };

    let mut raw_stages = Vec::with_capacity(stage_values.len());
    for v in &stage_values {
        if let Ok(s) = RawStage::try_from_json(v) {
            raw_stages.push(s);
        }
    }

    let mut plan_bundles = Vec::with_capacity(bundle_values.len());
    for v in &bundle_values {
        if let Some(exec_id) = v.get("sqlExecId").and_then(|x| x.as_i64()) {
            let nodes = parse_plan_nodes(v.get("planNodes"));
            plan_bundles.push(PlanBundle {
                sql_exec_id: SqlExecId::new(exec_id),
                plan_nodes: nodes,
            });
        }
    }

    let resolved = ResolvedGraphBuilder::new(domain)
        .with_stages(raw_stages)
        .with_plan_bundles(plan_bundles)
        .build();
    PyResolvedGraph { inner: resolved }
}

/// Reconstruct an owned `Graph` from a `PyGraph` snapshot. The PyO3 layer
/// hands us a `Vec<PyNode>`/`Vec<PyEdge>`; we need a domain `Graph` for the
/// builder. Conversion is field-for-field with `TableRef` rehydrated from
/// the `PyTableRef` snapshot (catalog, schema, table, paths, temp-view flag).
fn py_graph_to_domain(py_graph: &PyGraph) -> crate::graph::Graph {
    use crate::types::{Edge, Node, TableRef};

    let nodes = py_graph
        .nodes
        .iter()
        .map(|n| Node {
            id: n.id.clone(),
            kind: parse_op_kind(&n.kind),
            scaling_type: parse_scaling(&n.scaling_type),
            photon_eligible: n.photon_eligible,
            shuffle_required: n.shuffle_required,
            driver_bound: n.driver_bound,
            tables_referenced: n
                .tables_referenced
                .iter()
                .map(|t| TableRef {
                    raw: t.raw.clone(),
                    catalog: t.catalog.clone(),
                    schema: t.schema.clone(),
                    table: t.table.clone(),
                    is_temp_view: t.is_temp_view,
                    is_path_read: t.is_path_read,
                    path: t.path.clone(),
                })
                .collect(),
            estimated_input_bytes: n.estimated_input_bytes,
            estimated_cost_usd: n.estimated_cost_usd,
            line_number: n.line_number,
            source_code: n.source_code.clone(),
            // The PyNode snapshot doesn't currently carry the AST overlay
            // back across the boundary. Builders populate `ast` on the
            // Rust side; reconstituting from a PyGraph (used only when
            // crossing PyO3 in the test harness) loses it. The matcher
            // tolerates `None` here — affected paths short-circuit.
            ast: None,
        })
        .collect();

    let edges = py_graph
        .edges
        .iter()
        .map(|e| Edge {
            source: e.source.clone(),
            target: e.target.clone(),
            edge_type: e.edge_type.clone(),
        })
        .collect();

    crate::graph::Graph {
        nodes,
        edges,
        findings: Vec::new(),
        mode: py_graph.mode.clone(),
        confidence: py_graph.confidence.clone(),
    }
}

fn parse_op_kind(s: &str) -> crate::types::OperationKind {
    use crate::types::OperationKind as K;
    match s {
        "read" => K::Read,
        "transform" => K::Transform,
        "shuffle" => K::Shuffle,
        "action" => K::Action,
        "write" => K::Write,
        "udf_call" => K::UdfCall,
        "maintenance" => K::Maintenance,
        _ => K::Unknown,
    }
}

fn parse_scaling(s: &str) -> crate::types::ScalingBehavior {
    use crate::types::ScalingBehavior as S;
    match s {
        "linear" => S::Linear,
        "linear_with_cliff" => S::LinearWithCliff,
        "quadratic" => S::Quadratic,
        "step_failure" => S::StepFailure,
        "maintenance" => S::Maintenance,
        _ => S::Linear,
    }
}

/// Pull stages and plan bundles out of a session-shaped Python object as
/// `serde_json::Value`s, ready for the typed adapter constructors.
///
/// Two paths:
/// 1. `SessionStatePy` downcast — uses `raw_collected()` directly. This is
///    the production path; no JSON round-trip.
/// 2. Duck-typed fallback — looks for `.stages` and `.plan_bundles`
///    attributes (lists of dicts), serialises via Python's `json.dumps`
///    and parses to `Value`. This supports `_FakeSession`-style tests
///    without forcing them to construct a real `SessionStatePy`.
fn extract_session_values(
    py: Python<'_>,
    session: &Bound<'_, PyAny>,
) -> (Vec<serde_json::Value>, Vec<serde_json::Value>) {
    if let Ok(state_cell) = session.downcast::<SessionStatePy>() {
        let state = state_cell.borrow();
        let mut stages = Vec::new();
        let mut bundles = Vec::new();
        for v in state.raw_collected() {
            if v.get("stageId").is_some() {
                stages.push(v.clone());
            } else if v.get("planNodes").is_some() {
                bundles.push(v.clone());
            }
        }
        return (stages, bundles);
    }

    let stages = pyattr_to_values(py, session, "stages");
    let bundles = pyattr_to_values(py, session, "plan_bundles");
    (stages, bundles)
}

/// Serialise `session.<attr>` to JSON via Python's `json.dumps`, then parse
/// back to a `Vec<serde_json::Value>`. Failure at any step yields an empty
/// Vec — duck-typed sessions that don't expose the attribute simply
/// contribute no runtime data.
fn pyattr_to_values(
    py: Python<'_>,
    session: &Bound<'_, PyAny>,
    attr: &str,
) -> Vec<serde_json::Value> {
    let Ok(attr_val) = session.getattr(attr) else {
        return Vec::new();
    };
    if attr_val.is_none() {
        return Vec::new();
    }
    let Ok(json_module) = py.import_bound("json") else {
        return Vec::new();
    };
    let Ok(dumped) = json_module.call_method1("dumps", (attr_val,)) else {
        return Vec::new();
    };
    let Ok(as_string) = dumped.extract::<String>() else {
        return Vec::new();
    };
    serde_json::from_str::<Vec<serde_json::Value>>(&as_string).unwrap_or_default()
}

fn parse_plan_nodes(value: Option<&serde_json::Value>) -> Vec<PlanNode> {
    let Some(arr) = value.and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(arr.len());
    for node_value in arr {
        let Some(node_id) = node_value.get("nodeId").and_then(|v| v.as_i64()) else {
            continue;
        };
        let node_name = node_value
            .get("nodeName")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let parent_ids = node_value
            .get("parentIds")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|x| x.as_i64()).collect())
            .unwrap_or_default();
        let metrics = node_value
            .get("metrics")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();
        out.push(PlanNode {
            node_id,
            node_name,
            parent_ids,
            metrics,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_op_kind_round_trip_for_known_kinds() {
        use crate::types::OperationKind as K;
        for (s, expected) in [
            ("read", K::Read),
            ("transform", K::Transform),
            ("shuffle", K::Shuffle),
            ("write", K::Write),
            ("action", K::Action),
            ("maintenance", K::Maintenance),
            ("udf_call", K::UdfCall),
        ] {
            assert_eq!(parse_op_kind(s), expected);
        }
    }

    #[test]
    fn parse_scaling_round_trip_for_known_kinds() {
        use crate::types::ScalingBehavior as S;
        assert_eq!(parse_scaling("linear"), S::Linear);
        assert_eq!(parse_scaling("linear_with_cliff"), S::LinearWithCliff);
        assert_eq!(parse_scaling("quadratic"), S::Quadratic);
        assert_eq!(parse_scaling("step_failure"), S::StepFailure);
        assert_eq!(parse_scaling("maintenance"), S::Maintenance);
        // unknown falls back to Linear rather than panicking
        assert_eq!(parse_scaling("nonsense"), S::Linear);
    }

    #[test]
    fn parse_plan_nodes_handles_missing_optional_fields() {
        let value = serde_json::json!([
            {"nodeId": 1, "nodeName": "Filter", "parentIds": [], "metrics": {}},
            {"nodeId": 2, "nodeName": "Scan"},  // no parents / metrics
            {"missing": "nodeId"},  // dropped silently
        ]);
        let nodes = parse_plan_nodes(Some(&value));
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].node_id, 1);
        assert_eq!(nodes[1].node_id, 2);
        assert!(nodes[1].parent_ids.is_empty());
    }
}
