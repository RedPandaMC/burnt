//! Tests for context rules (Tier 2 — context-aware analysis, no TOML pattern matching)

use _engine::rules::run;
use insta::assert_yaml_snapshot;

// ---------------------------------------------------------------------------
// Performance rules
// ---------------------------------------------------------------------------

#[test]
fn test_bp021_jdbc_without_partition() {
    let source = r#"
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://host/db") \
    .option("dbtable", "large_table") \
    .load()
"#;

    let findings = run(source, "python").unwrap();
    let relevant: Vec<_> = findings.iter().filter(|f| f.code == "BP021").collect();

    let snapshot_data = serde_json::json!({
        "rule": "BP021",
        "triggered": !relevant.is_empty(),
        "findings": relevant.iter().map(|f| serde_json::json!({
            "code": f.code,
            "message": f.message,
        })).collect::<Vec<_>>()
    });

    assert_yaml_snapshot!(snapshot_data);
}

#[test]
fn test_bp072_groupby_agg_then_filter() {
    let source = r#"
result = df.groupBy("dept") \
    .agg({"salary": "avg"}) \
    .filter(col("avg(salary)") > 50000)
"#;

    let findings = run(source, "python").unwrap();
    let relevant: Vec<_> = findings.iter().filter(|f| f.code == "BP072").collect();

    let snapshot_data = serde_json::json!({
        "rule": "BP072",
        "triggered": !relevant.is_empty(),
        "findings": relevant.iter().map(|f| serde_json::json!({
            "code": f.code,
            "message": f.message,
        })).collect::<Vec<_>>()
    });

    assert_yaml_snapshot!(snapshot_data);
}

#[test]
fn test_bp074_single_withcolumn_chain() {
    let source = r#"
df = df.withColumn("a", col("x") + 1)
df = df.withColumn("b", col("y") + 2)
df = df.withColumn("c", col("z") + 3)
df = df.withColumn("d", col("w") + 4)
df = df.withColumn("e", col("v") + 5)
"#;

    let findings = run(source, "python").unwrap();
    let relevant: Vec<_> = findings.iter().filter(|f| f.code == "BP074").collect();

    let snapshot_data = serde_json::json!({
        "rule": "BP074",
        "triggered": !relevant.is_empty(),
        "findings": relevant.iter().map(|f| serde_json::json!({
            "code": f.code,
            "message": f.message,
        })).collect::<Vec<_>>()
    });

    assert_yaml_snapshot!(snapshot_data);
}

// ---------------------------------------------------------------------------
// Delta rules
// ---------------------------------------------------------------------------

#[test]
fn test_bd010_overwrite_without_replace_where() {
    let source = r#"
df.write.mode("overwrite").saveAsTable("my_catalog.my_schema.events")
"#;

    let findings = run(source, "python").unwrap();
    let relevant: Vec<_> = findings.iter().filter(|f| f.code == "BD010").collect();

    let snapshot_data = serde_json::json!({
        "rule": "BD010",
        "triggered": !relevant.is_empty(),
        "findings": relevant.iter().map(|f| serde_json::json!({
            "code": f.code,
            "message": f.message,
        })).collect::<Vec<_>>()
    });

    assert_yaml_snapshot!(snapshot_data);
}

// ---------------------------------------------------------------------------
// Streaming rules
// ---------------------------------------------------------------------------

#[test]
fn test_bs001_writestream_no_checkpoint() {
    let source = r#"
query = df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .start("/tmp/output")
"#;

    let findings = run(source, "python").unwrap();
    let relevant: Vec<_> = findings.iter().filter(|f| f.code == "BS001").collect();

    let snapshot_data = serde_json::json!({
        "rule": "BS001",
        "triggered": !relevant.is_empty(),
        "findings": relevant.iter().map(|f| serde_json::json!({
            "code": f.code,
            "message": f.message,
        })).collect::<Vec<_>>()
    });

    assert_yaml_snapshot!(snapshot_data);
}

#[test]
fn test_bs001_writestream_with_checkpoint_not_flagged() {
    let source = r#"
query = df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .outputMode("append") \
    .start("/tmp/output")
"#;

    let findings = run(source, "python").unwrap();
    let bs001: Vec<_> = findings.iter().filter(|f| f.code == "BS001").collect();
    assert!(bs001.is_empty(), "BS001 should not fire when checkpointLocation is set");
}

// ---------------------------------------------------------------------------
// Governance rules
// ---------------------------------------------------------------------------

#[test]
fn test_bu001_two_part_table_name() {
    let source = r#"
df = spark.read.table("schema.table")
"#;

    let findings = run(source, "python").unwrap();
    let relevant: Vec<_> = findings.iter().filter(|f| f.code == "BU001").collect();

    let snapshot_data = serde_json::json!({
        "rule": "BU001",
        "triggered": !relevant.is_empty(),
        "findings": relevant.iter().map(|f| serde_json::json!({
            "code": f.code,
            "message": f.message,
        })).collect::<Vec<_>>()
    });

    assert_yaml_snapshot!(snapshot_data);
}

#[test]
fn test_bu001_three_part_name_not_flagged() {
    let source = r#"
df = spark.read.table("catalog.schema.table")
"#;

    let findings = run(source, "python").unwrap();
    let bu001: Vec<_> = findings.iter().filter(|f| f.code == "BU001").collect();
    assert!(bu001.is_empty(), "BU001 should not fire for three-part table names");
}
