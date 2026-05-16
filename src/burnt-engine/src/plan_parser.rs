//! Spark physical query plan parser.
//!
//! Parses the JSON body returned by `/applications/{app_id}/sql/{exec_id}`
//! into a flat list of `PlanNode`s. The Spark monitoring API itself returns
//! `nodes` plus a sibling `edges` list (`{fromId, toId}`); the issue spec
//! used a simplified `nodes[].children: [id]` shape. Both are accepted and
//! collapsed into the same internal `PlanNode { parent_ids, metrics }` form.
//!
//! Two metric layers are joined at parse time: each node carries a metric
//! *schema* (`name`, `accumulatorId`) and the top-level `metricValues` map
//! holds the actual numbers keyed by accumulator id. Consumers see a single
//! flat `name -> value` map per node.

use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum PlanParseError {
    #[error("malformed plan JSON: {0}")]
    Json(#[from] serde_json::Error),
}

/// A single Catalyst operator from a Spark physical plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlanNode {
    pub node_id: i64,
    pub node_name: String,
    /// Parents in the DAG. Multi-valued because `ReusedExchange` lets a
    /// single physical node feed two consumers.
    pub parent_ids: Vec<i64>,
    /// Resolved metric values: schema `name` mapped to the value pulled
    /// from `metricValues[accumulatorId]`. Strings, since Spark renders
    /// formatted sizes like `"128.0 MiB"` alongside raw numbers.
    pub metrics: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct RawPlan {
    #[serde(default)]
    nodes: Vec<RawNode>,
    #[serde(default)]
    edges: Vec<RawEdge>,
    #[serde(default, rename = "metricValues")]
    metric_values: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct RawNode {
    #[serde(rename = "nodeId")]
    node_id: i64,
    #[serde(rename = "nodeName")]
    node_name: String,
    #[serde(default)]
    metrics: Vec<RawMetric>,
    /// Simplified shape used in the issue spec — `children: [id, ...]`.
    /// When present we synthesise edges from it.
    #[serde(default)]
    children: Vec<i64>,
}

#[derive(Debug, Deserialize)]
struct RawMetric {
    name: String,
    #[serde(rename = "accumulatorId")]
    accumulator_id: i64,
}

#[derive(Debug, Deserialize)]
struct RawEdge {
    #[serde(rename = "fromId")]
    from_id: i64,
    #[serde(rename = "toId")]
    to_id: i64,
}

/// Parse a Spark physical-plan JSON body into a flat `Vec<PlanNode>`.
///
/// Unknown, empty, or unparseable input deliberately returns an empty
/// list rather than an error — the caller is the monitoring REST loop
/// and a single malformed execution must not poison the rest of the
/// collected state.
pub fn parse_physical_plan(json_str: &str) -> Vec<PlanNode> {
    try_parse(json_str).unwrap_or_default()
}

/// Python-facing adapter mirroring the `PyCostNode` style elsewhere in the
/// crate. Owns its data so it can cross the GIL boundary as `Clone`.
#[pyclass(name = "PlanNode")]
#[derive(Clone)]
pub struct PyPlanNode {
    #[pyo3(get)]
    pub node_id: i64,
    #[pyo3(get)]
    pub node_name: String,
    /// Full DAG view — multi-valued for ReusedExchange and similar.
    #[pyo3(get)]
    pub parent_ids: Vec<i64>,
    inner_metrics: HashMap<String, serde_json::Value>,
}

#[pymethods]
impl PyPlanNode {
    /// Convenience accessor matching the issue spec: returns the first
    /// parent or `None` for plan roots. Use `parent_ids` for the full
    /// multi-parent picture.
    #[getter]
    fn parent_id(&self) -> Option<i64> {
        self.parent_ids.first().copied()
    }

    /// Resolved metric `name -> value` map.
    #[getter]
    fn metrics<'py>(&self, py: Python<'py>) -> PyObject {
        let dict = PyDict::new_bound(py);
        for (k, v) in &self.inner_metrics {
            dict.set_item(k, json_value_to_py(py, v)).ok();
        }
        dict.into_py(py)
    }

    fn __repr__(&self) -> String {
        format!(
            "PlanNode(id={}, name={:?}, parents={:?})",
            self.node_id, self.node_name, self.parent_ids
        )
    }
}

impl From<PlanNode> for PyPlanNode {
    fn from(n: PlanNode) -> Self {
        PyPlanNode {
            node_id: n.node_id,
            node_name: n.node_name,
            parent_ids: n.parent_ids,
            inner_metrics: n.metrics,
        }
    }
}

fn json_value_to_py(py: Python<'_>, value: &serde_json::Value) -> PyObject {
    use serde_json::Value;
    match value {
        Value::Null => py.None(),
        Value::Bool(b) => b.into_py(py),
        Value::Number(n) => n
            .as_i64()
            .map(|i| i.into_py(py))
            .unwrap_or_else(|| n.as_f64().unwrap_or(0.0).into_py(py)),
        Value::String(s) => s.clone().into_py(py),
        Value::Array(arr) => {
            let list = pyo3::types::PyList::empty_bound(py);
            for item in arr {
                list.append(json_value_to_py(py, item)).ok();
            }
            list.into_py(py)
        }
        Value::Object(obj) => {
            let dict = PyDict::new_bound(py);
            for (k, v) in obj {
                dict.set_item(k, json_value_to_py(py, v)).ok();
            }
            dict.into_py(py)
        }
    }
}

/// PyO3-exposed parser entry-point. Mirrors `parse_physical_plan` but
/// returns the Python-friendly adapter.
#[pyfunction]
#[pyo3(name = "parse_physical_plan")]
pub fn parse_physical_plan_py(json_str: &str) -> Vec<PyPlanNode> {
    parse_physical_plan(json_str)
        .into_iter()
        .map(PyPlanNode::from)
        .collect()
}

fn try_parse(json_str: &str) -> Result<Vec<PlanNode>, PlanParseError> {
    if json_str.trim().is_empty() {
        return Ok(Vec::new());
    }
    let raw: RawPlan = serde_json::from_str(json_str)?;
    if raw.nodes.is_empty() {
        return Ok(Vec::new());
    }

    // Edge `{fromId: a, toId: b}` is data-flow direction: `a` produces, `b`
    // consumes — so in tree terms, `b` is the parent of `a`.
    let mut parents: HashMap<i64, Vec<i64>> = HashMap::new();
    for edge in &raw.edges {
        parents.entry(edge.from_id).or_default().push(edge.to_id);
    }
    for node in &raw.nodes {
        // `children: [c1, c2]` lists data sources of `node`, so `node` is the
        // parent of every entry.
        for child in &node.children {
            parents.entry(*child).or_default().push(node.node_id);
        }
    }

    let nodes = flatten(&raw.nodes, &parents, &raw.metric_values);
    Ok(nodes)
}

/// Cycle-safe walk over the plan node list. Spark DAGs can include
/// `ReusedExchange`, so the same physical node can be referenced from
/// two parents — a naive recursive walk would loop.
fn flatten(
    raw_nodes: &[RawNode],
    parents: &HashMap<i64, Vec<i64>>,
    metric_values: &HashMap<String, serde_json::Value>,
) -> Vec<PlanNode> {
    let mut seen: HashSet<i64> = HashSet::new();
    let mut out: Vec<PlanNode> = Vec::with_capacity(raw_nodes.len());
    for raw in raw_nodes {
        if !seen.insert(raw.node_id) {
            continue;
        }
        let mut metrics: HashMap<String, serde_json::Value> = HashMap::new();
        for m in &raw.metrics {
            if let Some(v) = metric_values.get(&m.accumulator_id.to_string()) {
                metrics.insert(m.name.clone(), v.clone());
            }
        }
        out.push(PlanNode {
            node_id: raw.node_id,
            node_name: raw.node_name.clone(),
            parent_ids: parents.get(&raw.node_id).cloned().unwrap_or_default(),
            metrics,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load_fixture(name: &str) -> String {
        let path = format!(
            "{}/../../tests/fixtures/plans/{}",
            env!("CARGO_MANIFEST_DIR"),
            name
        );
        std::fs::read_to_string(&path)
            .unwrap_or_else(|_| panic!("fixture not found at {path}"))
    }

    #[test]
    fn parses_canonical_sort_exchange_scan() {
        let json = load_fixture("sort_exchange.json");
        let nodes = parse_physical_plan(&json);

        assert_eq!(nodes.len(), 3);
        let sort = &nodes[0];
        assert_eq!(sort.node_name, "Sort");
        assert!(sort.parent_ids.is_empty(), "Sort is the plan root");
        assert_eq!(
            sort.metrics.get("number of output rows"),
            Some(&serde_json::json!("12345"))
        );

        let exchange = &nodes[1];
        assert_eq!(exchange.node_name, "Exchange");
        assert_eq!(exchange.parent_ids, vec![1]);
        assert_eq!(
            exchange.metrics.get("shuffle write size"),
            Some(&serde_json::json!("128.0 MiB"))
        );

        let scan = &nodes[2];
        assert!(scan.node_name.starts_with("Scan"));
        assert_eq!(scan.parent_ids, vec![2]);
    }

    #[test]
    fn parses_children_shape() {
        let json = load_fixture("children_shape.json");
        let nodes = parse_physical_plan(&json);
        assert_eq!(nodes.len(), 3);
        // Sort (root) has no parent; Exchange is consumed by Sort; Scan by Exchange.
        assert!(nodes[0].parent_ids.is_empty(), "Sort is the root");
        assert_eq!(nodes[1].parent_ids, vec![1]);
        assert_eq!(nodes[2].parent_ids, vec![2]);
    }

    #[test]
    fn handles_reused_exchange_dag_without_looping() {
        let json = load_fixture("reused_exchange.json");
        let nodes = parse_physical_plan(&json);
        assert_eq!(nodes.len(), 4);
        let reused = nodes.iter().find(|n| n.node_name == "ReusedExchange").unwrap();
        assert_eq!(reused.parent_ids, vec![1]);
        let exchange = nodes.iter().find(|n| n.node_name == "Exchange").unwrap();
        let mut p = exchange.parent_ids.clone();
        p.sort();
        assert_eq!(p, vec![1, 3]);
    }

    #[test]
    fn empty_input_returns_empty_list() {
        assert!(parse_physical_plan("").is_empty());
        assert!(parse_physical_plan("{}").is_empty());
        assert!(parse_physical_plan(r#"{"nodes": []}"#).is_empty());
    }

    #[test]
    fn malformed_input_returns_empty_list() {
        assert!(parse_physical_plan("not json").is_empty());
        assert!(parse_physical_plan(r#"{"nodes": "wrong type"}"#).is_empty());
    }

    #[test]
    fn unknown_metric_values_are_silently_dropped() {
        let json = r#"{
            "nodes": [{"nodeId": 1, "nodeName": "Filter",
                       "metrics": [{"name": "rows", "accumulatorId": 999}]}],
            "metricValues": {}
        }"#;
        let nodes = parse_physical_plan(json);
        assert_eq!(nodes.len(), 1);
        assert!(nodes[0].metrics.is_empty());
    }

    #[test]
    fn duplicate_node_id_is_kept_only_once() {
        let json = r#"{
            "nodes": [
                {"nodeId": 1, "nodeName": "A", "metrics": []},
                {"nodeId": 1, "nodeName": "DupA", "metrics": []}
            ]
        }"#;
        let nodes = parse_physical_plan(json);
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].node_name, "A");
    }
}
