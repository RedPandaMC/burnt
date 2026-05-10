use crate::rules::context_structs::RuleContext;
use crate::rules::finding::make_finding;
use crate::types::{Confidence, Finding, Severity};

pub(super) fn check_overwrite_without_replace_where(
    source: &str,
    _ctx: &RuleContext,
) -> Vec<Finding> {
    let has_overwrite =
        source.contains(".mode(\"overwrite\")") || source.contains(".mode('overwrite')");
    let has_delta = source.contains("\"delta\"") || source.contains("'delta'");
    let has_replace_where =
        source.contains("replaceWhere") || source.contains("partitionOverwriteMode");
    if has_overwrite && has_delta && !has_replace_where {
        vec![make_finding(
            "BD010",
            Severity::Warning,
            "mode('overwrite') on Delta table replaces the entire table — use replaceWhere for partition-level overwrites",
            "Add .option('replaceWhere', 'partition_col = value') to scope the overwrite",
            1,
            Confidence::Medium,
        )]
    } else {
        vec![]
    }
}

pub(super) fn check_csv_json_analytical_write(
    source: &str,
    _ctx: &RuleContext,
) -> Vec<Finding> {
    let has_csv_json_write = (source.contains(".format(\"csv\")")
        || source.contains(".format('csv')")
        || source.contains(".format(\"json\")")
        || source.contains(".format('json')"))
        && (source.contains(".saveAsTable(") || source.contains(".save("));
    let is_landing = source.contains("landing")
        || source.contains("archive")
        || source.contains("export")
        || source.contains("raw");
    if has_csv_json_write && !is_landing {
        vec![make_finding(
            "BD013",
            Severity::Warning,
            "Writing analytical table as CSV/JSON lacks ACID transactions, schema enforcement, and time travel",
            "Use .format('delta') for analytical tables; reserve CSV/JSON for landing zones or exports",
            1,
            Confidence::Low,
        )]
    } else {
        vec![]
    }
}

pub(super) fn check_parquet_write_databricks(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let has_parquet_write =
        source.contains(".format(\"parquet\")") || source.contains(".format('parquet')");
    let has_write = source.contains(".write") || source.contains(".saveAsTable(");
    if has_parquet_write && has_write {
        vec![make_finding(
            "BD014",
            Severity::Info,
            "Writing as Parquet on Databricks foregoes Delta Lake ACID transactions, schema enforcement, and time travel",
            "Use .format('delta') instead of .format('parquet') for analytical tables on Databricks",
            1,
            Confidence::Medium,
        )]
    } else {
        vec![]
    }
}

pub(super) fn check_optimize_without_where(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let upper = source.to_uppercase();
    if !upper.contains("OPTIMIZE ") {
        return vec![];
    }
    if !upper.contains(" WHERE ") {
        vec![make_finding(
            "BD020",
            Severity::Info,
            "OPTIMIZE without WHERE rewrites the entire table — add a partition predicate to scope the operation",
            "Add WHERE partition_col >= 'recent_value' to limit files rewritten",
            1,
            Confidence::High,
        )]
    } else {
        vec![]
    }
}

pub(super) fn check_merge_without_partition_predicate(
    source: &str,
    _ctx: &RuleContext,
) -> Vec<Finding> {
    let upper = source.to_uppercase();
    if !upper.contains("MERGE INTO") {
        return vec![];
    }
    let on_start = match upper.find(" ON ") {
        Some(i) => i,
        None => return vec![],
    };
    let on_clause = match upper.find(" WHEN ") {
        Some(w) => &upper[on_start..w],
        None => &upper[on_start..],
    };
    let lower_on = on_clause.to_lowercase();
    let has_partition_hint = lower_on.contains(".date")
        || lower_on.contains("_date")
        || lower_on.contains(".year")
        || lower_on.contains("_year")
        || lower_on.contains(".month")
        || lower_on.contains("_month")
        || lower_on.contains(".day")
        || lower_on.contains("_day")
        || lower_on.contains("partition");
    if !has_partition_hint {
        vec![make_finding(
            "BD021",
            Severity::Warning,
            "MERGE INTO ON clause may be missing a partition predicate — this causes a full table scan on every merge",
            "Add a partition column condition to the ON clause (e.g. target.date = source.date)",
            1,
            Confidence::Low,
        )]
    } else {
        vec![]
    }
}

pub(super) fn check_merge_update_star_no_filter(
    source: &str,
    _ctx: &RuleContext,
) -> Vec<Finding> {
    let upper = source.to_uppercase();
    if !upper.contains("MERGE INTO") {
        return vec![];
    }
    let has_update_star =
        upper.contains("THEN UPDATE SET *") || upper.contains("THEN UPDATE SET*");
    let has_and_condition = {
        let when_matched = "WHEN MATCHED";
        upper.contains(when_matched) && {
            let idx = upper.find(when_matched).unwrap_or(0);
            let snippet = &upper[idx..std::cmp::min(idx + 60, upper.len())];
            snippet.contains(" AND ")
        }
    };
    if has_update_star && !has_and_condition {
        vec![make_finding(
            "BD022",
            Severity::Info,
            "WHEN MATCHED THEN UPDATE SET * without AND condition updates all matched rows, causing unnecessary rewrites",
            "Add a change-detection condition: WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE SET *",
            1,
            Confidence::Medium,
        )]
    } else {
        vec![]
    }
}

pub(super) fn check_convert_to_delta_no_optimize(
    source: &str,
    _ctx: &RuleContext,
) -> Vec<Finding> {
    let upper = source.to_uppercase();
    if !upper.contains("CONVERT TO DELTA") || upper.contains("OPTIMIZE") {
        return vec![];
    }
    vec![make_finding(
        "BD026",
        Severity::Info,
        "CONVERT TO DELTA leaves small files from the source format — run OPTIMIZE afterward",
        "Follow CONVERT TO DELTA with: OPTIMIZE <table_name>",
        1,
        Confidence::High,
    )]
}

pub(super) fn check_too_many_cluster_keys(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let upper = source.to_uppercase();
    let cluster_kw = match upper.find("CLUSTER BY") {
        Some(i) => i,
        None => return vec![],
    };
    let after = &source[cluster_kw + 10..];
    let paren_content = if let Some(open) = after.find('(') {
        let rest = &after[open + 1..];
        rest.find(')').map(|close| &rest[..close]).unwrap_or(rest)
    } else {
        after.split_whitespace().next().unwrap_or("")
    };
    if paren_content.split(',').count() > 4 {
        vec![make_finding(
            "BD032",
            Severity::Warning,
            "More than 4 Liquid Clustering keys reduces clustering effectiveness and increases write overhead",
            "Limit CLUSTER BY to the 2-4 most-queried columns",
            1,
            Confidence::High,
        )]
    } else {
        vec![]
    }
}
