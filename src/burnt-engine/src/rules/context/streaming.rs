use crate::rules::context_structs::RuleContext;
use crate::rules::finding::make_finding;
use crate::types::{Confidence, Finding, Severity};

pub(super) fn check_writestream_no_checkpoint(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
    if (!source.contains("writeStream") && !source.contains("write_stream"))
        || !source.contains(".start(")
        || source.contains("checkpointLocation")
        || source.contains("checkpoint_location")
    {
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

pub(super) fn check_readstream_no_trigger(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
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

pub(super) fn check_event_time_no_watermark(source: &str, _ctx: &RuleContext) -> Vec<Finding> {
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

pub(super) fn check_foreach_batch_no_idempotency(
    source: &str,
    _ctx: &RuleContext,
) -> Vec<Finding> {
    if !source.contains("foreachBatch(") && !source.contains("foreach_batch(") {
        return vec![];
    }
    if source.contains("txnAppId") || source.contains("txnVersion") {
        return vec![];
    }
    vec![make_finding(
        "BS004",
        Severity::Info,
        "foreachBatch without txnAppId/txnVersion idempotency options may cause duplicate writes on retry",
        "Add .option('txnAppId', app_name).option('txnVersion', epoch_id) to Delta writes inside foreachBatch",
        1,
        Confidence::Low,
    )]
}

pub(super) fn check_stream_static_join_non_delta(
    source: &str,
    ctx: &RuleContext,
) -> Vec<Finding> {
    let has_readstream = source.contains("readStream") || source.contains("read_stream");
    let has_join = source.contains(".join(");
    if !has_readstream || !has_join {
        return vec![];
    }

    let non_delta_formats = [
        ".format(\"parquet\")",
        ".format('parquet')",
        ".format(\"csv\")",
        ".format('csv')",
        ".format(\"json\")",
        ".format('json')",
        ".format(\"orc\")",
        ".format('orc')",
        ".csv(",
        ".json(",
    ];

    let has_static_non_delta = source.split('\n').any(|line| {
        let trimmed = line.trim();
        let is_static_read = if let Some((ns, method)) = ctx.tracker.extract_call_parts(trimmed) {
            if ctx.tracker.is_spark_ns(ns) {
                let method_base = method.split('.').next().unwrap_or(method);
                method_base == "read" || method_base.starts_with("read_")
            } else {
                // Namespace not imported — fall back to literal check so that
                // inline calls like `df.join(spark.read.format(...), ...)` are caught.
                trimmed.contains("spark.read.") || trimmed.contains("my_spark.read.")
            }
        } else {
            trimmed.contains("spark.read.") || trimmed.contains("my_spark.read.")
        };
        is_static_read && non_delta_formats.iter().any(|fmt| trimmed.contains(fmt))
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
