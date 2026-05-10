//! Tests for dataflow rules (Tier 3 — semantic/dataflow analysis across statements)

use _engine::rules::run;
use insta::assert_yaml_snapshot;

// ---------------------------------------------------------------------------
// Cache lifecycle rules
// ---------------------------------------------------------------------------

#[test]
fn test_bp030_cache_without_unpersist() {
    let source = r#"
df = spark.read.table("events")
df = df.cache()
result = df.count()
"#;

    let findings = run(source, "python").unwrap();
    let relevant: Vec<_> = findings.iter().filter(|f| f.code == "BP030").collect();

    let snapshot_data = serde_json::json!({
        "rule": "BP030",
        "triggered": !relevant.is_empty(),
        "findings": relevant.iter().map(|f| serde_json::json!({
            "code": f.code,
            "message": f.message,
        })).collect::<Vec<_>>()
    });

    assert_yaml_snapshot!(snapshot_data);
}

#[test]
fn test_bp030_cache_with_unpersist_not_flagged() {
    let source = r#"
df = spark.read.table("events")
df = df.cache()
result = df.count()
df.unpersist()
"#;

    let findings = run(source, "python").unwrap();
    let bp030: Vec<_> = findings.iter().filter(|f| f.code == "BP030").collect();
    assert!(bp030.is_empty(), "BP030 should not fire when unpersist() is called");
}

#[test]
fn test_bp031_cache_with_no_action() {
    let source = r#"
df = spark.read.table("events")
df = df.cache()
other = df.select("id")
"#;

    let findings = run(source, "python").unwrap();
    let relevant: Vec<_> = findings.iter().filter(|f| f.code == "BP031").collect();

    let snapshot_data = serde_json::json!({
        "rule": "BP031",
        "triggered": !relevant.is_empty(),
        "findings": relevant.iter().map(|f| serde_json::json!({
            "code": f.code,
            "message": f.message,
        })).collect::<Vec<_>>()
    });

    assert_yaml_snapshot!(snapshot_data);
}

// ---------------------------------------------------------------------------
// Filter after cache
// ---------------------------------------------------------------------------

#[test]
fn test_bp060_filter_after_cache() {
    let source = r#"
df = spark.read.table("events")
df = df.cache()
filtered = df.filter(col("date") == "2025-01-01")
result = filtered.count()
"#;

    let findings = run(source, "python").unwrap();
    let relevant: Vec<_> = findings.iter().filter(|f| f.code == "BP060").collect();

    let snapshot_data = serde_json::json!({
        "rule": "BP060",
        "triggered": !relevant.is_empty(),
        "findings": relevant.iter().map(|f| serde_json::json!({
            "code": f.code,
            "message": f.message,
        })).collect::<Vec<_>>()
    });

    assert_yaml_snapshot!(snapshot_data);
}

#[test]
fn test_bp060_filter_before_cache_not_flagged() {
    let source = r#"
df = spark.read.table("events")
filtered = df.filter(col("date") == "2025-01-01")
cached = filtered.cache()
result = cached.count()
"#;

    let findings = run(source, "python").unwrap();
    let bp060: Vec<_> = findings.iter().filter(|f| f.code == "BP060").collect();
    assert!(bp060.is_empty(), "BP060 should not fire when filter precedes cache");
}

// ---------------------------------------------------------------------------
// Chained select alias
// ---------------------------------------------------------------------------

#[test]
fn test_bnt_a02_chained_select_alias() {
    let source = r#"
result = df.select(col("id").alias("user_id"), col("name").alias("user_name")).select(col("user_id").alias("uid"))
"#;

    let findings = run(source, "python").unwrap();
    let relevant: Vec<_> = findings.iter().filter(|f| f.code == "BNT-A02").collect();

    let snapshot_data = serde_json::json!({
        "rule": "BNT-A02",
        "triggered": !relevant.is_empty(),
        "findings": relevant.iter().map(|f| serde_json::json!({
            "code": f.code,
            "message": f.message,
        })).collect::<Vec<_>>()
    });

    assert_yaml_snapshot!(snapshot_data);
}
