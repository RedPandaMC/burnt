use crate::parse::namespace::NamespaceTracker;
use crate::types::{Confidence, Finding, Severity};
use std::collections::HashMap;
use std::sync::OnceLock;

use super::context_structs::RuleContext;
use super::finding::make_finding;

type ContextFn = fn(&str, &RuleContext) -> Vec<Finding>;

static DISPATCH: OnceLock<HashMap<&'static str, ContextFn>> = OnceLock::new();

fn get_dispatch() -> &'static HashMap<&'static str, ContextFn> {
    DISPATCH.get_or_init(|| {
        let mut m: HashMap<&'static str, ContextFn> = HashMap::new();
        m.insert("BP001", check_cell_no_comment);
        m.insert("BP002", check_long_line);
        m.insert("BP021", check_jdbc_partition);
        m.insert("BP022", check_sdp_prohibited_ops);
        m.insert("BP023", check_window_without_partition);
        m.insert("BQ004", check_correlated_subquery);
        m.insert("SDP006", check_materialized_view_incremental);
        m.insert("BD010", check_overwrite_without_replace_where);
        m.insert("BD013", check_csv_json_analytical_write);
        m.insert("BD020", check_optimize_without_where);
        m.insert("BD021", check_merge_without_partition_predicate);
        m.insert("BD022", check_merge_update_star_no_filter);
        m.insert("BD026", check_convert_to_delta_no_optimize);
        m.insert("BD032", check_too_many_cluster_keys);
        m.insert("BP080", check_pandas_pyspark_mix);
        m.insert("BP081", check_pandas_roundtrip);
        m.insert("BS001", check_writestream_no_checkpoint);
        m.insert("BS003", check_event_time_no_watermark);
        m.insert("BS004", check_foreach_batch_no_idempotency);
        m.insert("BU001", check_two_part_table_name);
        m.insert("BD014", check_parquet_write_databricks);
        m.insert("BS002", check_readstream_no_trigger);
        m.insert("BS006", check_stream_static_join_non_delta);
        m.insert("BJ002", check_self_join_no_alias);
        m.insert("BP052", check_readstream_no_schema);
        m.insert("BP072", check_groupby_agg_filter);
        m.insert("BP073", check_orderby_before_shuffle);
        m.insert("BP074", check_single_withcolumn);
        m.insert("BP090", check_monotonically_increasing_id_join);
        m.insert("BP091", check_current_timestamp_in_cache);
        m.insert("BP094", check_input_file_name_as_key);
        m.insert("BP100", check_python_udf_photon);
        m.insert("BP102", check_photon_incompatible_expr);
        m.insert("BP110", check_broadcast_streaming);
        m.insert("BP112", check_tojson_collect);
        m
    })
}

pub fn analyze_context_for_rule(
    rule_code: &str,
    source: &str,
    tracker: Option<&NamespaceTracker>,
) -> Vec<Finding> {
    let default_tracker = NamespaceTracker::new();
    let tracker = tracker.unwrap_or(&default_tracker);
    let ctx = RuleContext::new(source, tracker);
    get_dispatch()
        .get(rule_code)
        .map(|f| f(source, &ctx))
        .unwrap_or_default()
}

fn check_jdbc_partition(source: &str, ctx: &RuleContext) -> Vec<Finding> {
    let mut findings = Vec::new();

    let has_jdbc = source.contains("jdbc");
    let has_partition_options = source.contains("partitionColumn")
        || source.contains("numPartitions")
        || source.contains("lowerBound")
        || source.contains("upperBound");

    let has_spark_read = source.split('\n').any(|line| {
        let trimmed = line.trim();
        if let Some((ns, method)) = ctx.tracker.extract_call_parts(trimmed) {
            if ctx.tracker.is_spark_namespace(ns) {
                let method_base = method.split('.').next().unwrap_or(method);
                if method_base == "read" || method_base.starts_with("read_") {
                    return true;
                }
            }
        }
        false
    });

    if has_jdbc
        && !has_partition_options
        && (source.contains(".read(") || source.contains(".jdbc(") || has_spark_read)
    {
        findings.push(make_finding(
            "BP021",
            Severity::Error,
            "JDBC read missing required partition options — reads entire table on single thread",
            "Add partitionColumn, numPartitions, lowerBound, and upperBound options",
            1,
            Confidence::High,
        ));
    }

    findings
}

fn check_sdp_prohibited_ops(source: &str, ctx: &RuleContext) -> Vec<Finding> {
    let mut findings = Vec::new();
    let prohibited = ["write", "collect", "show", "display"];

    let is_in_sdp_context = ctx.is_sdp_context();

    let lines: Vec<&str> = source.lines().collect();
    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();

        let contains_sdp_ref = trimmed.contains("@sdp.")
            || trimmed.contains("sdp.")
            || trimmed.contains("@dlt.")
            || trimmed.contains("dlt.")
            || trimmed.contains("@dp.")
            || trimmed.contains("dp.");

        if !is_in_sdp_context && !contains_sdp_ref {
            continue;
        }

        for op in &prohibited {
            if trimmed.contains(&format!(".{}(", op)) {
                findings.push(make_finding(
                    "BP022",
                    Severity::Error,
                    &format!(
                        "Prohibited operation (.{}()) inside Spark Declarative Pipeline function",
                        op
                    ),
                    "Remove this operation from SDP pipeline code",
                    (i + 1) as u32,
                    Confidence::High,
                ));
            }
        }
    }

    findings
}

fn check_window_without_partition(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let mut findings = Vec::new();

    let has_window_order = source.contains("Window.orderBy") || source.contains("Window.order_by");
    let has_partition_by = source.contains(".partitionBy(") || source.contains(".partition_by(");

    if has_window_order && !has_partition_by {
        findings.push(make_finding(
            "BP023",
            Severity::Warning,
            "Window.orderBy() without .partitionBy() causes global sort",
            "Add .partitionBy() before .orderBy() or use .orderBy().limit()",
            1,
            Confidence::High,
        ));
    }

    findings
}

fn check_materialized_view_incremental(source: &str, ctx: &RuleContext) -> Vec<Finding> {
    let mut findings = Vec::new();

    let has_dlt_table = source.split('\n').any(|line| {
        let trimmed = line.trim();
        if trimmed.starts_with('@') {
            let dec_part = trimmed.trim_start_matches('@');
            let parts: Vec<&str> = dec_part.split('.').collect();
            if parts.len() >= 1 {
                let ns = parts[0];
                if ctx.tracker.is_dlt_namespace(ns) {
                    return true;
                }
                if ns == "sdp" || ns == "dlt" || ns == "dp" {
                    return true;
                }
            }
        }
        false
    });

    let has_incremental = source.contains("incremental") || source.contains("stream");

    if has_dlt_table && !has_incremental {
        findings.push(make_finding(
            "SDP006",
            Severity::Warning,
            "Materialized view defined without incremental strategy",
            "Consider incremental materialized view for large datasets",
            1,
            Confidence::Medium,
        ));
    }

    findings
}

fn check_cell_no_comment(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let mut findings = Vec::new();
    let lines: Vec<&str> = source.lines().collect();

    let mut in_cell = false;
    let mut cell_start_line = 0;
    let mut has_comment = false;
    let cell_markers = ["# cell", "#%%", "# %%", "# In["];

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();

        let mut is_marker = false;
        for marker in &cell_markers {
            if trimmed.starts_with(marker) {
                if in_cell && !has_comment && cell_start_line < i {
                    findings.push(make_finding(
                        "BP001",
                        Severity::Info,
                        "Cell has no comments",
                        "Add comments for clarity",
                        (cell_start_line + 1) as u32,
                        Confidence::Low,
                    ));
                }
                in_cell = true;
                cell_start_line = i;
                has_comment = false;
                is_marker = true;
                break;
            }
        }

        if !is_marker && in_cell && (trimmed.starts_with('#') && !trimmed.starts_with("# MAGIC")) {
            has_comment = true;
        }
    }

    findings
}

fn check_long_line(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let mut findings = Vec::new();
    let lines: Vec<&str> = source.lines().collect();
    let max_line_length = 120;

    for (i, line) in lines.iter().enumerate() {
        if line.len() > max_line_length {
            findings.push(Finding {
                rule_id: "BP002".to_string(),
                code: "BP002".to_string(),
                severity: Severity::Info,
                message: format!("Line exceeds {} characters", max_line_length),
                suggestion: Some("Break line for readability".to_string()),
                line_number: Some((i + 1) as u32),
                column: Some(max_line_length as u32),
                confidence: Confidence::High,
            });
        }
    }

    findings
}

fn check_correlated_subquery(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let upper = source.to_uppercase();

    // Must have NOT IN with a SELECT subquery
    if !upper.contains("NOT IN") || !upper.contains("SELECT") {
        return vec![];
    }

    // Correlation signal: the inner SELECT's WHERE clause contains a dotted column
    // reference (e.g. outer_table.col = inner_table.col), typical of correlated subqueries.
    // We look for NOT IN followed (within ~200 chars) by SELECT ... WHERE ... word.word
    let not_in_positions: Vec<_> = upper.match_indices("NOT IN").collect();
    for (pos, _) in not_in_positions {
        let window = &source[pos..std::cmp::min(pos + 300, source.len())];
        let window_upper = window.to_uppercase();
        if window_upper.contains("SELECT") && window_upper.contains("WHERE") {
            // Look for dotted identifier pattern (word.word) in the subquery window
            let has_dot_ref = window.split_whitespace().any(|tok| {
                let t = tok.trim_matches(|c: char| !c.is_alphanumeric() && c != '.' && c != '_');
                let parts: Vec<&str> = t.split('.').collect();
                parts.len() == 2
                    && parts.iter().all(|p| {
                        !p.is_empty() && p.chars().all(|c| c.is_alphanumeric() || c == '_')
                    })
            });
            if has_dot_ref {
                return vec![make_finding(
                    "BQ004",
                    Severity::Error,
                    "Correlated subquery references outer columns — Spark may execute as a nested loop join",
                    "Rewrite as a join or use window functions",
                    1,
                    Confidence::Medium,
                )];
            }
        }
    }

    vec![]
}

fn check_overwrite_without_replace_where(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
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

fn check_csv_json_analytical_write(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
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

fn check_optimize_without_where(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let upper = source.to_uppercase();
    if !upper.contains("OPTIMIZE ") {
        return vec![];
    }
    let has_where = upper.contains(" WHERE ");
    if !has_where {
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

fn check_merge_without_partition_predicate(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
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
    // Heuristic: partition predicate has a column *named* date/year/month/day (via `.date`, `_date`
    // suffix patterns). Avoid substring matches inside table names (e.g. "UPDATES" contains "DATE").
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

fn check_merge_update_star_no_filter(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let upper = source.to_uppercase();
    if !upper.contains("MERGE INTO") {
        return vec![];
    }
    let has_update_star = upper.contains("THEN UPDATE SET *") || upper.contains("THEN UPDATE SET*");
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

fn check_convert_to_delta_no_optimize(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let upper = source.to_uppercase();
    if !upper.contains("CONVERT TO DELTA") {
        return vec![];
    }
    if upper.contains("OPTIMIZE") {
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

fn check_too_many_cluster_keys(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let upper = source.to_uppercase();
    let cluster_kw = match upper.find("CLUSTER BY") {
        Some(i) => i,
        None => return vec![],
    };
    let after = &source[cluster_kw + 10..];
    let paren_content = if let Some(open) = after.find('(') {
        let rest = &after[open + 1..];
        if let Some(close) = rest.find(')') {
            &rest[..close]
        } else {
            rest
        }
    } else {
        after.split_whitespace().next().unwrap_or("")
    };
    let key_count = paren_content.split(',').count();
    if key_count > 4 {
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

fn check_pandas_pyspark_mix(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let has_ps = source.contains("pyspark.pandas") || source.contains("import ps");
    let has_pd = source.contains("import pandas")
        || (source.contains("import pd") && !source.contains("pyspark"));
    if has_ps && has_pd {
        vec![make_finding(
            "BP080",
            Severity::Warning,
            "Mixing pyspark.pandas and pandas in the same file causes implicit serialization round-trips",
            "Use pyspark.pandas exclusively, or convert at a single boundary and stay in Spark",
            1,
            Confidence::Medium,
        )]
    } else {
        vec![]
    }
}

fn check_pandas_roundtrip(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let has_to_pandas = source.contains(".toPandas()") || source.contains(".to_pandas()");
    let has_to_spark = source.contains(".to_spark()");
    if has_to_pandas && has_to_spark {
        vec![make_finding(
            "BP081",
            Severity::Warning,
            ".to_pandas() followed by .to_spark() materialises the entire DataFrame to the driver and back",
            "Keep data in Spark; use Spark built-in functions or pandas UDFs instead",
            1,
            Confidence::Medium,
        )]
    } else {
        vec![]
    }
}

fn check_writestream_no_checkpoint(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    if !source.contains("writeStream") && !source.contains("write_stream") {
        return vec![];
    }
    if !source.contains(".start(") {
        return vec![];
    }
    if source.contains("checkpointLocation") || source.contains("checkpoint_location") {
        return vec![];
    }
    vec![make_finding(
        "BS001",
        Severity::Error,
        "writeStream without checkpointLocation loses all progress on restart",
        "Add .option('checkpointLocation', '/path/to/checkpoint') before .start()",
        1,
        Confidence::High,
    )]
}

fn check_event_time_no_watermark(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let has_window_group =
        source.contains("groupBy(window(") || source.contains("groupBy( window(");
    let has_watermark = source.contains("withWatermark(") || source.contains("with_watermark(");
    if has_window_group && !has_watermark {
        vec![make_finding(
            "BS003",
            Severity::Warning,
            "Event-time aggregation without withWatermark causes unbounded state accumulation",
            "Add .withWatermark('event_time_col', '10 minutes') before groupBy(window(...))",
            1,
            Confidence::High,
        )]
    } else {
        vec![]
    }
}

fn check_foreach_batch_no_idempotency(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    if !source.contains("foreachBatch(") && !source.contains("foreach_batch(") {
        return vec![];
    }
    let has_idempotency = source.contains("txnAppId") || source.contains("txnVersion");
    if !has_idempotency {
        vec![make_finding(
            "BS004",
            Severity::Info,
            "foreachBatch without txnAppId/txnVersion idempotency options may cause duplicate writes on retry",
            "Add .option('txnAppId', app_name).option('txnVersion', epoch_id) to Delta writes inside foreachBatch",
            1,
            Confidence::Low,
        )]
    } else {
        vec![]
    }
}

fn check_two_part_table_name(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let upper = source.to_uppercase();
    // Look for FROM/JOIN/INTO/TABLE <word>.<word> without a third part
    for keyword in &["FROM ", "JOIN ", "INTO ", "TABLE "] {
        let mut search_pos = 0;
        while let Some(kw_pos) = upper[search_pos..].find(keyword) {
            let abs_pos = search_pos + kw_pos;
            let after = upper[abs_pos + keyword.len()..].trim_start();
            // Count dots in first token
            let token: &str = after.split_whitespace().next().unwrap_or("");
            let dot_count = token.chars().filter(|&c| c == '.').count();
            if dot_count == 1 && !token.is_empty() {
                // Two-part name — check it's not a file path or special syntax
                let first_part = token.split('.').next().unwrap_or("");
                if !first_part.is_empty()
                    && first_part.chars().all(|c| c.is_alphanumeric() || c == '_')
                {
                    return vec![make_finding(
                        "BU001",
                        Severity::Warning,
                        "Two-part table name omits the Unity Catalog prefix — resolves to the default catalog",
                        "Use three-part naming: catalog.schema.table for explicit catalog resolution",
                        1,
                        Confidence::Medium,
                    )];
                }
            }
            search_pos = abs_pos + keyword.len();
        }
    }
    vec![]
}

fn check_parquet_write_databricks(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
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

fn check_readstream_no_trigger(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let has_readstream = source.contains("readStream") || source.contains("read_stream");
    let has_start = source.contains(".start(");
    let has_trigger = source.contains(".trigger(");
    if has_readstream && has_start && !has_trigger {
        vec![make_finding(
            "BS002",
            Severity::Warning,
            "readStream without .trigger() runs in continuous micro-batch mode — add a trigger interval to control cost",
            "Add .trigger(processingTime='1 minute') or .trigger(availableNow=True)",
            1,
            Confidence::Medium,
        )]
    } else {
        vec![]
    }
}

fn check_stream_static_join_non_delta(source: &str, ctx: &RuleContext) -> Vec<Finding> {
    let has_readstream = source.contains("readStream") || source.contains("read_stream");
    let has_join = source.contains(".join(");
    if !has_readstream || !has_join {
        return vec![];
    }

    let has_static_non_delta = source.split('\n').any(|line| {
        let trimmed = line.trim();
        if let Some((ns, method)) = ctx.tracker.extract_call_parts(trimmed) {
            if ctx.tracker.is_spark_namespace(ns) {
                let method_base = method.split('.').next().unwrap_or(method);
                if method_base == "read" || method_base.starts_with("read_") {
                    return trimmed.contains(".format(\"parquet\")")
                        || trimmed.contains(".format('parquet')")
                        || trimmed.contains(".format(\"csv\")")
                        || trimmed.contains(".format('csv')")
                        || trimmed.contains(".format(\"json\")")
                        || trimmed.contains(".format('json')")
                        || trimmed.contains(".format(\"orc\")")
                        || trimmed.contains(".format('orc')")
                        || trimmed.contains(".csv(")
                        || trimmed.contains(".json(");
                }
            }
        }
        let has_spark_read_call =
            trimmed.contains("spark.read.") || trimmed.contains("my_spark.read.");
        if has_spark_read_call {
            let has_non_delta_format = trimmed.contains(".format(\"parquet\")")
                || trimmed.contains(".format('parquet')")
                || trimmed.contains(".format(\"csv\")")
                || trimmed.contains(".format('csv')")
                || trimmed.contains(".format(\"json\")")
                || trimmed.contains(".format('json')")
                || trimmed.contains(".format(\"orc\")")
                || trimmed.contains(".format('orc')")
                || trimmed.contains(".csv(")
                || trimmed.contains(".json(");
            if has_non_delta_format {
                return true;
            }
        }
        false
    });

    if has_static_non_delta {
        vec![make_finding(
            "BS006",
            Severity::Warning,
            "Stream-static join with a non-Delta static side does not automatically reflect updates to the static table",
            "Use a Delta table for the static side so updates are visible without restarting the stream",
            1,
            Confidence::Low,
        )]
    } else {
        vec![]
    }
}

fn check_self_join_no_alias(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    if !source.contains(".join(") {
        return vec![];
    }
    // Detect `x.join(x,` pattern — same variable name on both sides
    let lines: Vec<&str> = source.lines().collect();
    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        // Simple heuristic: find `.join(var,` or `.join(var)` where var == the lhs object
        if let Some(join_pos) = trimmed.find(".join(") {
            let lhs = trimmed[..join_pos].trim();
            // Extract the first argument of join
            let after_open = &trimmed[join_pos + 6..];
            let first_arg = after_open
                .split(|c: char| c == ',' || c == ')')
                .next()
                .unwrap_or("")
                .trim();
            if !lhs.is_empty() && !first_arg.is_empty() && lhs == first_arg {
                return vec![make_finding(
                    "BJ002",
                    Severity::Warning,
                    "Self-join without aliasing produces ambiguous column references",
                    "Alias both sides: left = df.alias('left'); right = df.alias('right'); left.join(right, ...)",
                    (i + 1) as u32,
                    Confidence::High,
                )];
            }
        }
    }
    vec![]
}

fn check_readstream_no_schema(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let has_readstream = source.contains("readStream") || source.contains("read_stream");
    if !has_readstream {
        return vec![];
    }
    let needs_schema = source.contains(".format(\"json\")")
        || source.contains(".format('json')")
        || source.contains(".format(\"csv\")")
        || source.contains(".format('csv')")
        || source.contains(".format(\"avro\")")
        || source.contains(".format('avro')");
    let has_schema = source.contains(".schema(");
    if needs_schema && !has_schema {
        vec![make_finding(
            "BP052",
            Severity::Warning,
            "readStream on JSON/CSV/Avro without an explicit schema triggers expensive inference on every restart",
            "Provide .schema(my_schema) before .load() to avoid inference",
            1,
            Confidence::High,
        )]
    } else {
        vec![]
    }
}

fn check_groupby_agg_filter(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    // Detect .agg(...).filter( or .agg(...).where(
    let has_agg = source.contains(".agg(");
    let has_post_agg_filter = source.contains(".agg(")
        && (source.find(".filter(").unwrap_or(0) > source.find(".agg(").unwrap_or(usize::MAX)
            || source.find(".where(").unwrap_or(0) > source.find(".agg(").unwrap_or(usize::MAX));
    if has_agg && has_post_agg_filter {
        vec![make_finding(
            "BP072",
            Severity::Info,
            ".filter() after .agg() is a post-aggregation filter — ensure non-aggregated column filters are placed before groupBy for performance",
            "Move filters on non-aggregated columns before groupBy(); keep filters on agg results after agg()",
            1,
            Confidence::Low,
        )]
    } else {
        vec![]
    }
}

fn check_orderby_before_shuffle(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let orderby_pos = source
        .find(".orderBy(")
        .or_else(|| source.find(".order_by("));
    let shuffle_pos = source
        .find(".groupBy(")
        .or_else(|| source.find(".join("))
        .or_else(|| source.find(".repartition("));
    if let (Some(ob), Some(sh)) = (orderby_pos, shuffle_pos) {
        if ob < sh {
            return vec![make_finding(
                "BP073",
                Severity::Warning,
                ".orderBy() before a shuffle operation (groupBy/join/repartition) is discarded by the shuffle",
                "Remove the pre-shuffle .orderBy(); apply sorting after the shuffle if order is required",
                1,
                Confidence::Medium,
            )];
        }
    }
    vec![]
}

fn check_single_withcolumn(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    // Count occurrences of .withColumn( — fire if 3 or more in the same chain
    let count = source.matches(".withColumn(").count();
    if count >= 3 {
        vec![make_finding(
            "BP074",
            Severity::Info,
            "Multiple chained .withColumn() calls each add a Project node — use .withColumns({}) for efficiency",
            "Replace chained .withColumn() calls with a single .withColumns({'col': expr, ...})",
            1,
            Confidence::Low,
        )]
    } else {
        vec![]
    }
}

fn check_monotonically_increasing_id_join(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let has_mono_id = source.contains("monotonically_increasing_id");
    let has_join = source.contains(".join(");
    if has_mono_id && has_join {
        vec![make_finding(
            "BP090",
            Severity::Warning,
            "monotonically_increasing_id() used as a join key — IDs differ across recomputation and are not stable",
            "Use a deterministic business key or a UUID derived from row content instead",
            1,
            Confidence::Medium,
        )]
    } else {
        vec![]
    }
}

fn check_current_timestamp_in_cache(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let has_timestamp = source.contains("current_timestamp()")
        || source.contains("F.now()")
        || source.contains("functions.now()");
    let has_cache = source.contains(".cache()") || source.contains(".persist(");
    if has_timestamp && has_cache {
        vec![make_finding(
            "BP091",
            Severity::Warning,
            "current_timestamp() or now() inside a cached DataFrame returns the cache evaluation time, not query time",
            "Materialise the timestamp before caching, or avoid caching DataFrames that include current_timestamp()/now()",
            1,
            Confidence::Medium,
        )]
    } else {
        vec![]
    }
}

fn check_input_file_name_as_key(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let has_input_file = source.contains("input_file_name");
    let used_as_key = source.contains(".join(") || source.contains(".partitionBy(");
    if has_input_file && used_as_key {
        vec![make_finding(
            "BP094",
            Severity::Warning,
            "input_file_name() used as a partition or join key — file names vary by run, making results non-deterministic",
            "Extract stable identifiers from file content rather than using input_file_name() as a key",
            1,
            Confidence::Medium,
        )]
    } else {
        vec![]
    }
}

fn check_python_udf_photon(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    // Detect Python UDFs (not pandas_udf which is Arrow-based and more Photon compatible)
    let has_plain_udf = source.contains("@udf(")
        || source.contains("@udf\n")
        || (source.contains("= udf(") && !source.contains("pandas_udf"));
    let has_pandas_udf_only =
        source.contains("@pandas_udf") && !source.contains("@udf(") && !source.contains("= udf(");
    if has_plain_udf && !has_pandas_udf_only {
        vec![make_finding(
            "BP100",
            Severity::Warning,
            "Python UDFs disable Photon acceleration — each row is serialized to Python and back",
            "Rewrite using Spark built-in functions or use a Pandas UDF (Arrow-based) for better Photon compatibility",
            1,
            Confidence::High,
        )]
    } else {
        vec![]
    }
}

fn check_photon_incompatible_expr(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    if source.contains("from_xml(") || source.contains("to_xml(") {
        vec![make_finding(
            "BP102",
            Severity::Info,
            "from_xml() and to_xml() are not supported by Photon — the query falls back to the non-Photon engine",
            "Pre-process XML with a pandas UDF or switch to JSON/Avro format for Photon-accelerated parsing",
            1,
            Confidence::High,
        )]
    } else {
        vec![]
    }
}

fn check_broadcast_streaming(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let has_broadcast = source.contains("broadcast(");
    let has_readstream = source.contains("readStream") || source.contains("read_stream");
    if !has_broadcast || !has_readstream {
        return vec![];
    }
    // Heuristic: broadcast( appears on the same line or close to readStream
    let broadcast_pos = source.find("broadcast(").unwrap_or(usize::MAX);
    let readstream_pos = source
        .find("readStream")
        .or_else(|| source.find("read_stream"))
        .unwrap_or(usize::MAX);
    // Fire if broadcast wraps something near readStream (within 100 chars)
    let distance = if broadcast_pos < readstream_pos {
        readstream_pos - broadcast_pos
    } else {
        broadcast_pos - readstream_pos
    };
    if distance < 100 {
        vec![make_finding(
            "BP110",
            Severity::Error,
            "broadcast() applied to a streaming DataFrame causes a StreamingQueryException at runtime",
            "Remove broadcast() from streaming DataFrames; Spark handles stream-static joins automatically",
            1,
            Confidence::Medium,
        )]
    } else {
        vec![]
    }
}

fn check_tojson_collect(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    if source.contains(".toJSON().collect()") || source.contains(".toJSON().\ncollect()") {
        vec![make_finding(
            "BP112",
            Severity::Warning,
            ".toJSON().collect() pulls all rows as JSON strings to the driver — use .toPandas() or write to storage",
            "Replace with df.toPandas().to_json() for small data, or df.write.format('json').save(path) for large data",
            1,
            Confidence::High,
        )]
    } else {
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_long_line() {
        let source = "This is a very long line that exceeds the maximum line length of 120 characters and should trigger a finding for BP002 because it is longer than 120 characters";
        let tracker = NamespaceTracker::new();
        let ctx = RuleContext::new(source, &tracker);
        let findings = check_long_line(source, &ctx);
        assert!(!findings.is_empty());
        assert_eq!(findings[0].code, "BP002");
    }

    #[test]
    fn test_long_line_ok() {
        let source = "Short line";
        let tracker = NamespaceTracker::new();
        let ctx = RuleContext::new(source, &tracker);
        let findings = check_long_line(source, &ctx);
        assert!(findings.is_empty());
    }

    #[test]
    fn test_bp001_dispatched() {
        let findings = analyze_context_for_rule("BP001", "# cell\nsome_code()\n", None);
        let _ = findings;
    }

    #[test]
    fn test_bp002_dispatched() {
        let long_line = "x".repeat(130);
        let findings = analyze_context_for_rule("BP002", &long_line, None);
        assert!(!findings.is_empty());
        assert_eq!(findings[0].code, "BP002");
    }

    #[test]
    fn test_unknown_rule_returns_empty() {
        let findings = analyze_context_for_rule("UNKNOWN999", "some source", None);
        assert!(findings.is_empty());
    }
}
