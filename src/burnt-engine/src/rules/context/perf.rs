use crate::rules::context_structs::RuleContext;
use crate::rules::finding::make_finding;
use crate::types::{Confidence, Finding, Severity};

pub(super) fn check_jdbc_partition(source: &str, ctx: &RuleContext) -> Vec<Finding> {
    let has_jdbc = source.contains("jdbc");
    let has_partition_options = source.contains("partitionColumn")
        || source.contains("numPartitions")
        || source.contains("lowerBound")
        || source.contains("upperBound");

    let has_spark_read = source.split('\n').any(|line| {
        let trimmed = line.trim();
        if let Some((ns, method)) = ctx.tracker.extract_call_parts(trimmed) {
            if ctx.tracker.is_spark_ns(ns) {
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
        vec![make_finding(
            "BP021",
            Severity::Error,
            "JDBC read missing required partition options — reads entire table on single thread",
            "Add partitionColumn, numPartitions, lowerBound, and upperBound options",
            1,
            Confidence::High,
        )]
    } else {
        vec![]
    }
}

pub(super) fn check_window_without_partition(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let has_window_order =
        source.contains("Window.orderBy") || source.contains("Window.order_by");
    let has_partition_by =
        source.contains(".partitionBy(") || source.contains(".partition_by(");

    if has_window_order && !has_partition_by {
        vec![make_finding(
            "BP023",
            Severity::Warning,
            "Window.orderBy() without .partitionBy() causes global sort",
            "Add .partitionBy() before .orderBy() or use .orderBy().limit()",
            1,
            Confidence::High,
        )]
    } else {
        vec![]
    }
}

pub(super) fn check_pandas_pyspark_mix(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
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

pub(super) fn check_pandas_roundtrip(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
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

pub(super) fn check_self_join_no_alias(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    if !source.contains(".join(") {
        return vec![];
    }
    for (i, line) in source.lines().enumerate() {
        let trimmed = line.trim();
        if let Some(join_pos) = trimmed.find(".join(") {
            let lhs = trimmed[..join_pos].trim();
            let after_open = &trimmed[join_pos + 6..];
            let first_arg = after_open
                .split([',', ')'])
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

pub(super) fn check_readstream_no_schema(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
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

pub(super) fn check_groupby_agg_filter(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let has_agg = source.contains(".agg(");
    let has_post_agg_filter = has_agg
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

pub(super) fn check_orderby_before_shuffle(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
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

pub(super) fn check_single_withcolumn(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    if source.matches(".withColumn(").count() >= 3 {
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

pub(super) fn check_monotonically_increasing_id_join(
    source: &str,
    _ctx: &RuleContext,
) -> Vec<Finding> {
    if source.contains("monotonically_increasing_id") && source.contains(".join(") {
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

pub(super) fn check_current_timestamp_in_cache(
    source: &str,
    _ctx: &RuleContext,
) -> Vec<Finding> {
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

pub(super) fn check_input_file_name_as_key(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
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

pub(super) fn check_python_udf_photon(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
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

pub(super) fn check_photon_incompatible_expr(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
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

pub(super) fn check_broadcast_streaming(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    let has_broadcast = source.contains("broadcast(");
    let has_readstream = source.contains("readStream") || source.contains("read_stream");
    if !has_broadcast || !has_readstream {
        return vec![];
    }
    let broadcast_pos = source.find("broadcast(").unwrap_or(usize::MAX);
    let readstream_pos = source
        .find("readStream")
        .or_else(|| source.find("read_stream"))
        .unwrap_or(usize::MAX);
    let distance = broadcast_pos.abs_diff(readstream_pos);
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

pub(super) fn check_tojson_collect(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
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
