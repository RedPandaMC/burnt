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

use std::collections::HashMap;

use regex::Regex;
use serde_json::Value;

use crate::graph::Graph;
use crate::resolved::error::ResolveError;
use crate::resolved::ids::{SqlExecId, StageId, StaticNodeId};
use crate::resolved::overlay::{NodeOverlay, StageObservation, Unmatched};
use crate::resolved::ResolvedGraph;

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
    /// The merge logic is added in a subsequent commit; today this returns a
    /// graph with every static node carrying `Provenance::STATIC` and empty
    /// overlay vectors, plus all incoming stages / plan bundles routed to
    /// `unmatched`.
    #[must_use]
    pub fn build(self) -> ResolvedGraph {
        let mut overlays: HashMap<StaticNodeId, NodeOverlay> = HashMap::with_capacity(self.graph.nodes.len());
        for node in &self.graph.nodes {
            overlays.insert(StaticNodeId::new(node.id.clone()), NodeOverlay::new());
        }

        let mut unmatched = Unmatched::default();
        for stage in self.stages {
            unmatched.stages.push(stage.into_observation());
        }
        for bundle in self.plan_bundles {
            unmatched.plan_bundles.push(bundle.sql_exec_id);
        }

        ResolvedGraph::from_parts(self.graph, overlays, HashMap::new(), unmatched)
    }
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
}
