//! Merge stage and plan signals onto the canonical static graph.
//!
//! This module is the **single source of truth** for the line-number
//! heuristic that maps Spark stages back to static graph nodes. The same
//! regex was previously duplicated in `src/burnt/graph/enrich.py` and
//! `src/burnt/graph/estimate.py`; both are slated to drop their copies and
//! consume the merged [`ResolvedGraph`] instead.
//!
//! The builder is **infallible**: orphaned stages and plan bundles land in
//! the [`Unmatched`] bucket so consumers can surface them in
//! diagnostics rather than silently lose data.

use std::collections::{BTreeMap, HashMap};

use regex::Regex;
use serde_json::Value;

use crate::graph::Graph;
use crate::resolved::error::ResolveError;
use crate::resolved::ids::{PlanNodeId, SqlExecId, StageId, StaticNodeId};
use crate::resolved::overlay::{NodeOverlay, PlanSubtree, Provenance, StageObservation, Unmatched};
use crate::resolved::ResolvedGraph;

/// Symmetric window (in source lines) within which a stage is considered to
/// belong to a static node. Matches the prior Python heuristic in
/// `src/burnt/graph/enrich.py` so this PR is a refactor, not a behaviour
/// change. A stage whose recovered line falls within `±LINE_WINDOW` of a
/// node's `line_number` is attached to the closest match.
const LINE_WINDOW: u32 = 5;

/// Raw Spark REST stage payload as collected by `SessionStatePy`.
#[derive(Debug, Clone)]
pub struct RawStage {
    pub stage_id: StageId,
    pub name: Option<String>,
    pub sql_exec_id: Option<SqlExecId>,
    pub input_bytes: Option<u64>,
    pub shuffle_read_bytes: Option<u64>,
    pub shuffle_write_bytes: Option<u64>,
    pub duration_ms: Option<u64>,
    pub num_tasks: Option<u32>,
}

impl RawStage {
    /// Build a `RawStage` from a Spark REST stage JSON object. Returns
    /// `ResolveError::MalformedStage` when the required `stageId` field is
    /// missing or non-numeric.
    pub fn try_from_json(value: &Value) -> Result<Self, ResolveError> {
        let stage_id = value
            .get("stageId")
            .and_then(Value::as_i64)
            .ok_or_else(|| ResolveError::MalformedStage {
                reason: "missing or non-numeric stageId".into(),
            })?;
        Ok(Self {
            stage_id: StageId::new(stage_id),
            name: value
                .get("name")
                .and_then(Value::as_str)
                .map(str::to_string),
            sql_exec_id: value
                .get("executionId")
                .and_then(Value::as_i64)
                .map(SqlExecId::new),
            input_bytes: value.get("inputBytes").and_then(Value::as_u64),
            shuffle_read_bytes: value.get("shuffleReadBytes").and_then(Value::as_u64),
            shuffle_write_bytes: value.get("shuffleWriteBytes").and_then(Value::as_u64),
            duration_ms: value.get("executorRunTime").and_then(Value::as_u64),
            num_tasks: value
                .get("numTasks")
                .and_then(Value::as_u64)
                .and_then(|n| u32::try_from(n).ok()),
        })
    }

    /// Convert into an observation, recovering `source_line` from the name.
    fn into_observation(self) -> StageObservation {
        let source_line = self.name.as_deref().and_then(extract_source_line);
        StageObservation {
            stage_id: self.stage_id,
            input_bytes: self.input_bytes,
            shuffle_read_bytes: self.shuffle_read_bytes,
            shuffle_write_bytes: self.shuffle_write_bytes,
            duration_ms: self.duration_ms,
            num_tasks: self.num_tasks,
            source_line,
        }
    }
}

/// Raw plan bundle as collected by `SessionStatePy::plan_bundles`.
#[derive(Debug, Clone)]
pub struct PlanBundle {
    pub sql_exec_id: SqlExecId,
    pub plan_nodes: Vec<crate::plan_parser::PlanNode>,
}

/// Builder for [`ResolvedGraph`]. Construct with the static graph, optionally
/// add stages and plan bundles, then call [`build`] for an infallible result.
///
/// [`build`]: ResolvedGraphBuilder::build
pub struct ResolvedGraphBuilder {
    graph: Graph,
    stages: Vec<RawStage>,
    plan_bundles: Vec<PlanBundle>,
}

impl ResolvedGraphBuilder {
    #[must_use]
    pub fn new(graph: Graph) -> Self {
        Self {
            graph,
            stages: Vec::new(),
            plan_bundles: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_stages(mut self, stages: Vec<RawStage>) -> Self {
        self.stages = stages;
        self
    }

    #[must_use]
    pub fn with_plan_bundles(mut self, bundles: Vec<PlanBundle>) -> Self {
        self.plan_bundles = bundles;
        self
    }

    /// Build the resolved graph. Infallible — unattached signals land in
    /// `unmatched` so callers can report on lossy correlation.
    ///
    /// Algorithm:
    /// 1. Index every static node by its `line_number` in a `BTreeMap`.
    /// 2. For each stage, recover its source line from the stage name and
    ///    pick the closest static node within `±LINE_WINDOW` lines. Ties
    ///    resolve to the lower line number for determinism. Unmatched
    ///    stages land in `unmatched.stages`.
    /// 3. For each plan bundle, look for a stage in the same SQL execution
    ///    that was attached in step 2; reuse that stage's static node as
    ///    the bundle's anchor. Bundles without an anchor land in
    ///    `unmatched.plan_bundles`.
    /// 4. Every node carries `Provenance::STATIC` unconditionally.
    #[must_use]
    pub fn build(self) -> ResolvedGraph {
        let mut overlays: HashMap<StaticNodeId, NodeOverlay> =
            HashMap::with_capacity(self.graph.nodes.len());
        for node in &self.graph.nodes {
            overlays.insert(StaticNodeId::new(node.id.clone()), NodeOverlay::new());
        }

        let line_index = build_line_index(&self.graph);
        let mut unmatched = Unmatched::default();
        // sqlExecId → first node that any of its stages attached to. Used by
        // step 3 to anchor plan bundles via shared execution id. First-
        // attached wins; ties don't matter for plan anchoring.
        let mut sql_exec_to_node: HashMap<SqlExecId, StaticNodeId> = HashMap::new();

        for stage in self.stages {
            let sql_exec_id = stage.sql_exec_id;
            let obs = stage.into_observation();
            match obs.source_line.and_then(|line| pick_node(&line_index, line)) {
                Some(node_id) => {
                    if let Some(overlay) = overlays.get_mut(&node_id) {
                        overlay.stages.push(obs);
                        overlay.provenance |= Provenance::STAGE;
                    }
                    if let Some(exec) = sql_exec_id {
                        sql_exec_to_node.entry(exec).or_insert(node_id);
                    }
                }
                None => {
                    unmatched.stages.push(obs);
                }
            }
        }

        for bundle in self.plan_bundles {
            let anchor = sql_exec_to_node.get(&bundle.sql_exec_id).cloned();
            match anchor {
                Some(node_id) => {
                    let root = bundle
                        .plan_nodes
                        .iter()
                        .find(|n| n.parent_ids.is_empty())
                        .map(|n| n.node_id)
                        .unwrap_or_else(|| {
                            bundle
                                .plan_nodes
                                .first()
                                .map(|n| n.node_id)
                                .unwrap_or_default()
                        });
                    let subtree = PlanSubtree::new(
                        bundle.sql_exec_id,
                        PlanNodeId::new(root),
                        bundle.plan_nodes,
                    );
                    if let Some(overlay) = overlays.get_mut(&node_id) {
                        overlay.plan_subtree = Some(subtree);
                        overlay.provenance |= Provenance::PLAN;
                    }
                }
                None => {
                    unmatched.plan_bundles.push(bundle.sql_exec_id);
                }
            }
        }

        ResolvedGraph::from_parts(self.graph, overlays, HashMap::new(), unmatched)
    }
}

/// Build a line-number index of static nodes.
///
/// A node may share a line with another (rare but possible — two operations
/// on the same source line), so the value is a `Vec<StaticNodeId>`. The
/// `BTreeMap` enables ±N range queries via `range()` in `pick_node`.
fn build_line_index(graph: &Graph) -> BTreeMap<u32, Vec<StaticNodeId>> {
    let mut index: BTreeMap<u32, Vec<StaticNodeId>> = BTreeMap::new();
    for node in &graph.nodes {
        if let Some(line) = node.line_number {
            index
                .entry(line)
                .or_default()
                .push(StaticNodeId::new(node.id.clone()));
        }
    }
    index
}

/// Pick the closest static node within ±`LINE_WINDOW` of `stage_line`.
///
/// Determinism rules: among nodes at equal absolute distance, prefer the
/// *lower* line number; among nodes on the same line, prefer the one that
/// hashes lowest by string id. The hash tiebreak is rare enough that
/// correctness doesn't ride on it — it just stops the result depending on
/// `HashMap` iteration order.
fn pick_node(
    index: &BTreeMap<u32, Vec<StaticNodeId>>,
    stage_line: u32,
) -> Option<StaticNodeId> {
    let lo = stage_line.saturating_sub(LINE_WINDOW);
    let hi = stage_line.saturating_add(LINE_WINDOW);
    let mut best: Option<(u32, &StaticNodeId)> = None;
    for (line, ids) in index.range(lo..=hi) {
        let dist = stage_line.abs_diff(*line);
        for id in ids {
            best = match best {
                None => Some((dist, id)),
                Some((bd, _)) if dist < bd => Some((dist, id)),
                Some((bd, bid)) if dist == bd && id.as_str() < bid.as_str() => {
                    Some((dist, id))
                }
                other => other,
            };
        }
    }
    best.map(|(_, id)| id.clone())
}

/// Extract a source line number from a Spark stage name.
///
/// Stage names typically embed the source location of the operation, e.g.
/// `"collect at /workspace/etl.py:42"` or `"save at <stdin>:7"`. Returns
/// `None` when no line marker is found.
pub(super) fn extract_source_line(stage_name: &str) -> Option<u32> {
    static PATTERN_INIT: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re = PATTERN_INIT.get_or_init(|| {
        // Matches "<file>.{py,sql,ipynb}:<digits>" and "<stdin>:<digits>".
        Regex::new(r"(?:\.py|\.sql|\.ipynb|<stdin>):(\d+)").expect("valid regex literal")
    });
    re.captures(stage_name)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<u32>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_source_line_handles_py_sql_ipynb_and_stdin() {
        assert_eq!(
            extract_source_line("collect at /workspace/etl.py:42"),
            Some(42)
        );
        assert_eq!(extract_source_line("save at q.sql:7"), Some(7));
        assert_eq!(
            extract_source_line("Spark <stdin>:5 broadcast"),
            Some(5)
        );
        assert_eq!(
            extract_source_line("nb.ipynb:128"),
            Some(128)
        );
        assert_eq!(extract_source_line("no marker here"), None);
        assert_eq!(extract_source_line(""), None);
    }

    #[test]
    fn raw_stage_try_from_json_rejects_missing_stage_id() {
        let v = json!({"name": "x", "inputBytes": 100});
        let err = RawStage::try_from_json(&v).unwrap_err();
        assert!(matches!(err, ResolveError::MalformedStage { .. }));
    }

    #[test]
    fn raw_stage_try_from_json_parses_full_payload() {
        let v = json!({
            "stageId": 7,
            "name": "save at /tmp/x.py:42",
            "executionId": 3,
            "inputBytes": 1024,
            "shuffleReadBytes": 256,
            "shuffleWriteBytes": 128,
            "executorRunTime": 5000,
            "numTasks": 4,
        });
        let s = RawStage::try_from_json(&v).expect("parse");
        assert_eq!(s.stage_id, StageId::new(7));
        assert_eq!(s.sql_exec_id, Some(SqlExecId::new(3)));
        assert_eq!(s.input_bytes, Some(1024));
        assert_eq!(s.shuffle_read_bytes, Some(256));
        assert_eq!(s.shuffle_write_bytes, Some(128));
        assert_eq!(s.duration_ms, Some(5000));
        assert_eq!(s.num_tasks, Some(4));
    }

    #[test]
    fn into_observation_recovers_source_line_from_name() {
        let v = json!({"stageId": 1, "name": "collect at file.py:10"});
        let s = RawStage::try_from_json(&v).unwrap();
        let obs = s.into_observation();
        assert_eq!(obs.source_line, Some(10));
    }

    use crate::plan_parser::PlanNode;
    use crate::resolved::Provenance;
    use crate::types::{Edge, Node, OperationKind, ScalingBehavior};

    fn mk_node(id: &str, line: u32) -> Node {
        Node {
            ast: None,
            scope: crate::resolved::ScopeFacts::default(),
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
        }
    }

    fn graph_with(nodes: Vec<Node>) -> Graph {
        Graph {
            nodes,
            edges: Vec::<Edge>::new(),
            findings: Vec::new(),
            mode: "python".into(),
            confidence: "low".into(),
        }
    }

    fn stage_at(stage_id: i64, line: u32, exec_id: Option<i64>) -> RawStage {
        let v = json!({
            "stageId": stage_id,
            "name": format!("collect at file.py:{line}"),
            "inputBytes": 1024,
        });
        let mut s = RawStage::try_from_json(&v).unwrap();
        s.sql_exec_id = exec_id.map(SqlExecId::new);
        s
    }

    #[test]
    fn stage_within_window_attaches_to_closest_node() {
        let graph = graph_with(vec![mk_node("a", 10), mk_node("b", 30)]);
        let resolved = ResolvedGraphBuilder::new(graph)
            .with_stages(vec![stage_at(1, 12, None)])
            .build();
        let a = resolved.overlay("a").unwrap();
        let b = resolved.overlay("b").unwrap();
        assert_eq!(a.stages.len(), 1, "stage at 12 should attach to a@10");
        assert!(b.stages.is_empty(), "b@30 should not see it");
        assert!(a.provenance.contains(Provenance::STAGE));
        assert!(!b.provenance.contains(Provenance::STAGE));
        assert!(resolved.unmatched().stages.is_empty());
    }

    #[test]
    fn stage_outside_window_lands_in_unmatched() {
        let graph = graph_with(vec![mk_node("a", 10)]);
        let resolved = ResolvedGraphBuilder::new(graph)
            .with_stages(vec![stage_at(1, 99, None)])
            .build();
        assert_eq!(resolved.unmatched().stages.len(), 1);
        assert!(resolved.overlay("a").unwrap().stages.is_empty());
    }

    #[test]
    fn equidistant_ties_resolve_to_lower_line_number() {
        // stage at line 15; nodes at 10 (dist 5) and 20 (dist 5). Both within
        // the ±5 window. Tiebreak: lower line wins → "a" at line 10.
        let graph = graph_with(vec![mk_node("a", 10), mk_node("b", 20)]);
        let resolved = ResolvedGraphBuilder::new(graph)
            .with_stages(vec![stage_at(1, 15, None)])
            .build();
        assert_eq!(resolved.overlay("a").unwrap().stages.len(), 1);
        assert!(resolved.overlay("b").unwrap().stages.is_empty());
    }

    #[test]
    fn plan_bundle_anchors_via_stage_sharing_sql_exec_id() {
        let graph = graph_with(vec![mk_node("a", 10), mk_node("b", 30)]);
        // Stage 7 is in exec 42 and lands on node "a" via line proximity.
        // Bundle for exec 42 should attach to the same node.
        let bundle = PlanBundle {
            sql_exec_id: SqlExecId::new(42),
            plan_nodes: vec![
                PlanNode {
                    node_id: 1,
                    node_name: "Sort".into(),
                    parent_ids: vec![],
                    metrics: Default::default(),
                },
                PlanNode {
                    node_id: 2,
                    node_name: "Exchange".into(),
                    parent_ids: vec![1],
                    metrics: Default::default(),
                },
            ],
        };
        let resolved = ResolvedGraphBuilder::new(graph)
            .with_stages(vec![stage_at(7, 11, Some(42))])
            .with_plan_bundles(vec![bundle])
            .build();
        let a = resolved.overlay("a").unwrap();
        assert!(a.plan_subtree.is_some());
        let pt = a.plan_subtree.as_ref().unwrap();
        assert_eq!(pt.sql_exec_id, SqlExecId::new(42));
        assert_eq!(pt.root, PlanNodeId::new(1));
        assert_eq!(pt.nodes.len(), 2);
        assert!(a
            .provenance
            .contains(Provenance::STATIC | Provenance::STAGE | Provenance::PLAN));
    }

    #[test]
    fn plan_bundle_with_no_anchor_stage_lands_in_unmatched() {
        let graph = graph_with(vec![mk_node("a", 10)]);
        let bundle = PlanBundle {
            sql_exec_id: SqlExecId::new(99),
            plan_nodes: vec![PlanNode {
                node_id: 1,
                node_name: "Sort".into(),
                parent_ids: vec![],
                metrics: Default::default(),
            }],
        };
        let resolved = ResolvedGraphBuilder::new(graph)
            .with_plan_bundles(vec![bundle])
            .build();
        assert_eq!(resolved.unmatched().plan_bundles.len(), 1);
        assert_eq!(
            resolved.unmatched().plan_bundles[0],
            SqlExecId::new(99)
        );
        assert!(resolved.overlay("a").unwrap().plan_subtree.is_none());
    }

    #[test]
    fn every_static_node_carries_static_provenance() {
        let graph = graph_with(vec![mk_node("a", 10), mk_node("b", 99)]);
        let resolved = ResolvedGraphBuilder::new(graph).build();
        for nid in ["a", "b"] {
            let ov = resolved.overlay(nid).unwrap();
            assert!(ov.provenance.contains(Provenance::STATIC));
            assert!(!ov.provenance.contains(Provenance::STAGE));
            assert!(!ov.provenance.contains(Provenance::PLAN));
        }
    }

}
