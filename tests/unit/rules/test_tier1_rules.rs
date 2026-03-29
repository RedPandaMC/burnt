//! Tests for Tier 1 rules (simple tree-sitter pattern matching)

use _engine::rules::{run, RulePipeline};
use insta::assert_yaml_snapshot;

#[test]
fn test_bp008_collect_without_limit() {
    let source = r#"
df = spark.read.table("orders")
result = df.collect()
print(result)
"#;

    let findings = run(source, "python").unwrap();

    // Create snapshot of findings
    let snapshot_data = serde_json::json!({
        "rule_count": findings.len(),
        "findings": findings.iter().map(|f| {
            serde_json::json!({
                "rule_id": f.rule_id,
                "code": f.code,
                "severity": format!("{:?}", f.severity),
                "message": f.message,
                "line": f.line_number,
            })
        }).collect::<Vec<_>>()
    });

    assert_yaml_snapshot!(snapshot_data);
}

#[test]
fn test_bp009_select_star_without_limit() {
    let source = r#"SELECT * FROM large_table WHERE date = '2025-01-01'"#;

    let findings = run(source, "sql").unwrap();

    let snapshot_data = serde_json::json!({
        "rule_count": findings.len(),
        "findings": findings.iter().map(|f| {
            serde_json::json!({
                "rule_id": f.rule_id,
                "code": f.code,
                "severity": format!("{:?}", f.severity),
            })
        }).collect::<Vec<_>>()
    });

    assert_yaml_snapshot!(snapshot_data);
}

#[test]
fn test_bp009_select_star_with_limit() {
    let source = r#"SELECT * FROM large_table WHERE date = '2025-01-01' LIMIT 100"#;

    let findings = run(source, "sql").unwrap();

    // Should not trigger the rule because there's a LIMIT clause
    assert_eq!(findings.len(), 0);
}

#[test]
fn test_bp014_cross_join_sql() {
    let source = r#"SELECT a.id, b.value FROM orders a CROSS JOIN products b"#;

    let findings = run(source, "sql").unwrap();

    let snapshot_data = serde_json::json!({
        "rule_count": findings.len(),
        "findings": findings.iter().map(|f| {
            serde_json::json!({
                "rule_id": f.rule_id,
                "code": f.code,
            })
        }).collect::<Vec<_>>()
    });

    assert_yaml_snapshot!(snapshot_data);
}

#[test]
fn test_bp014_cross_join_pyspark() {
    let source = r#"
df1 = spark.read.table("orders")
df2 = spark.read.table("products")
result = df1.crossJoin(df2)
"#;

    let findings = run(source, "python").unwrap();

    let snapshot_data = serde_json::json!({
        "rule_count": findings.len(),
        "findings": findings.iter().map(|f| {
            serde_json::json!({
                "rule_id": f.rule_id,
                "code": f.code,
            })
        }).collect::<Vec<_>>()
    });

    assert_yaml_snapshot!(snapshot_data);
}

#[test]
fn test_bq001_not_in_with_nulls() {
    let source = r#"SELECT id FROM users WHERE role NOT IN (SELECT role FROM deleted_users)"#;

    let findings = run(source, "sql").unwrap();

    let snapshot_data = serde_json::json!({
        "rule_count": findings.len(),
        "findings": findings.iter().map(|f| {
            serde_json::json!({
                "rule_id": f.rule_id,
                "code": f.code,
            })
        }).collect::<Vec<_>>()
    });

    assert_yaml_snapshot!(snapshot_data);
}

#[test]
fn test_rule_pipeline_phases() {
    let pipeline = RulePipeline::new();

    // Test that we have the right phases
    assert_eq!(pipeline.phases.len(), 7);

    // Test phase names
    let phase_names: Vec<String> = pipeline.phases.iter().map(|p| format!("{:?}", p)).collect();

    assert_yaml_snapshot!(phase_names);
}

#[test]
fn test_empty_source() {
    let findings = run("", "python").unwrap();
    assert_eq!(findings.len(), 0);

    let findings = run("", "sql").unwrap();
    assert_eq!(findings.len(), 0);
}
