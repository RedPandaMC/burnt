//! End-to-end tests for the resolved-graph substrate.
//!
//! Runs against the public `resolved` API only — no PyO3, no Python — so
//! these tests run via plain `cargo test`.

use _engine::graph::Graph;
use _engine::plan_parser::PlanNode;
use _engine::resolved::{
    NodeOverlay, PlanBundle, Provenance, RawStage, ResolvedGraphBuilder, SqlExecId, StaticNodeId,
};
use _engine::types::{Edge, Node, OperationKind, ScalingBehavior};
use serde_json::json;

fn read_node(id: &str, line: u32) -> Node {
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
    }
}

fn graph(nodes: Vec<Node>) -> Graph {
    Graph {
        nodes,
        edges: Vec::<Edge>::new(),
        findings: Vec::new(),
        mode: "python".into(),
        confidence: "low".into(),
    }
}

fn raw_stage(stage_id: i64, line: u32, exec_id: Option<i64>, input_bytes: u64) -> RawStage {
    let v = json!({
        "stageId": stage_id,
        "name": format!("save at /etl/job.py:{line}"),
        "inputBytes": input_bytes,
    });
    let mut s = RawStage::try_from_json(&v).unwrap();
    s.sql_exec_id = exec_id.map(SqlExecId::new);
    s
}

#[test]
fn full_round_trip_with_no_runtime_data_preserves_static_graph() {
    let g = graph(vec![read_node("a", 10), read_node("b", 20)]);
    let resolved = ResolvedGraphBuilder::new(g).build();

    assert_eq!(resolved.graph().nodes.len(), 2);
    assert!(resolved.unmatched().stages.is_empty());
    for nid in ["a", "b"] {
        let ov: &NodeOverlay = resolved.overlay(nid).unwrap();
        assert_eq!(ov.provenance, Provenance::STATIC);
        assert!(ov.stages.is_empty());
        assert!(ov.plan_subtree.is_none());
    }
}

#[test]
fn round_trip_with_stages_and_plan_bundle_populates_overlays() {
    let g = graph(vec![read_node("read_a", 10), read_node("write_b", 50)]);
    let stages = vec![
        // attaches to read_a (line 10 ± 5)
        raw_stage(1, 11, Some(7), 1024),
        // attaches to write_b (line 50 ± 5)
        raw_stage(2, 49, Some(7), 2048),
        // unmatched — far from both
        raw_stage(3, 999, None, 0),
    ];
    let bundle = PlanBundle {
        sql_exec_id: SqlExecId::new(7),
        plan_nodes: vec![PlanNode {
            node_id: 100,
            node_name: "Sort".into(),
            parent_ids: vec![],
            metrics: Default::default(),
        }],
    };

    let resolved = ResolvedGraphBuilder::new(g)
        .with_stages(stages)
        .with_plan_bundles(vec![bundle])
        .build();

    let read_a = resolved.overlay("read_a").unwrap();
    let write_b = resolved.overlay("write_b").unwrap();

    assert_eq!(read_a.stages.len(), 1);
    assert_eq!(read_a.stages[0].input_bytes, Some(1024));
    assert!(read_a.provenance.contains(Provenance::STAGE | Provenance::PLAN));
    assert!(read_a.plan_subtree.is_some());

    assert_eq!(write_b.stages.len(), 1);
    assert_eq!(write_b.stages[0].input_bytes, Some(2048));
    assert!(write_b.provenance.contains(Provenance::STAGE));
    // The plan bundle attached to whichever node first registered exec 7;
    // since read_a is processed first by line attachment, write_b should
    // not also receive the plan.
    assert!(write_b.plan_subtree.is_none());

    assert_eq!(resolved.unmatched().stages.len(), 1);
    assert!(resolved.unmatched().plan_bundles.is_empty());
}

#[test]
fn distinct_id_types_are_visible_in_overlay() {
    let g = graph(vec![read_node("a", 10)]);
    let resolved = ResolvedGraphBuilder::new(g)
        .with_stages(vec![raw_stage(42, 10, Some(7), 1024)])
        .build();
    let ov = resolved.overlay("a").unwrap();
    let stage = &ov.stages[0];
    // The newtypes round-trip through into_inner unchanged.
    assert_eq!(stage.stage_id.clone().into_inner(), 42);

    // Sanity: the resolved graph's node id is a StaticNodeId.
    let ids: Vec<StaticNodeId> = resolved
        .overlays()
        .map(|(id, _)| id.clone())
        .collect();
    assert!(ids.iter().any(|id| id.as_str() == "a"));
}
