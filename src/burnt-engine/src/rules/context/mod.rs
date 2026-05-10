use crate::parse::import_map::ImportMap;
use crate::types::Finding;
use std::collections::HashMap;
use std::sync::OnceLock;

use super::context_structs::RuleContext;

mod delta;
mod governance;
mod perf;
mod sdp;
mod sql;
mod streaming;
mod style;

type ContextFn = fn(&str, &RuleContext) -> Vec<Finding>;

static DISPATCH: OnceLock<HashMap<&'static str, ContextFn>> = OnceLock::new();

fn get_dispatch() -> &'static HashMap<&'static str, ContextFn> {
    DISPATCH.get_or_init(|| {
        let mut m: HashMap<&'static str, ContextFn> = HashMap::new();
        m.insert("BP001", style::check_cell_no_comment);
        m.insert("BP002", style::check_long_line);
        m.insert("BP021", perf::check_jdbc_partition);
        m.insert("BP022", sdp::check_sdp_prohibited_ops);
        m.insert("BP023", perf::check_window_without_partition);
        m.insert("BP052", perf::check_readstream_no_schema);
        m.insert("BP072", perf::check_groupby_agg_filter);
        m.insert("BP073", perf::check_orderby_before_shuffle);
        m.insert("BP074", perf::check_single_withcolumn);
        m.insert("BP080", perf::check_pandas_pyspark_mix);
        m.insert("BP081", perf::check_pandas_roundtrip);
        m.insert("BP090", perf::check_monotonically_increasing_id_join);
        m.insert("BP091", perf::check_current_timestamp_in_cache);
        m.insert("BP094", perf::check_input_file_name_as_key);
        m.insert("BP100", perf::check_python_udf_photon);
        m.insert("BP102", perf::check_photon_incompatible_expr);
        m.insert("BP110", perf::check_broadcast_streaming);
        m.insert("BP112", perf::check_tojson_collect);
        m.insert("BJ002", perf::check_self_join_no_alias);
        m.insert("BQ004", sql::check_correlated_subquery);
        m.insert("BD010", delta::check_overwrite_without_replace_where);
        m.insert("BD013", delta::check_csv_json_analytical_write);
        m.insert("BD014", delta::check_parquet_write_databricks);
        m.insert("BD020", delta::check_optimize_without_where);
        m.insert("BD021", delta::check_merge_without_partition_predicate);
        m.insert("BD022", delta::check_merge_update_star_no_filter);
        m.insert("BD026", delta::check_convert_to_delta_no_optimize);
        m.insert("BD032", delta::check_too_many_cluster_keys);
        m.insert("BS001", streaming::check_writestream_no_checkpoint);
        m.insert("BS002", streaming::check_readstream_no_trigger);
        m.insert("BS003", streaming::check_event_time_no_watermark);
        m.insert("BS004", streaming::check_foreach_batch_no_idempotency);
        m.insert("BS006", streaming::check_stream_static_join_non_delta);
        m.insert("BU001", governance::check_two_part_table_name);
        m.insert("SDP006", sdp::check_materialized_view_incremental);
        m
    })
}

pub fn analyze_context_for_rule(
    rule_code: &str,
    source: &str,
    tracker: Option<&ImportMap>,
) -> Vec<Finding> {
    let default_tracker = ImportMap::default();
    let tracker = tracker.unwrap_or(&default_tracker);
    let ctx = RuleContext::new(source, tracker);
    get_dispatch()
        .get(rule_code)
        .map(|f| f(source, &ctx))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unknown_rule_returns_empty() {
        let result = analyze_context_for_rule("ZZZZZZ", "some code", None);
        assert!(result.is_empty());
    }

    #[test]
    fn test_bp002_dispatched() {
        let long_line = "x".repeat(121);
        let findings = analyze_context_for_rule("BP002", &long_line, None);
        assert!(!findings.is_empty());
        assert_eq!(findings[0].code, "BP002");
    }

    #[test]
    fn test_bp001_dispatched() {
        let source = "# cell\n\ndf = spark.read.table('foo')\n# cell\n\ndf2 = df.select('*')";
        let findings = analyze_context_for_rule("BP001", source, None);
        assert!(findings.iter().all(|f| f.code == "BP001"));
    }

    #[test]
    fn test_dispatch_covers_all_registered_rules() {
        let dispatch = get_dispatch();
        assert!(dispatch.len() >= 35, "dispatch should cover all context rules");
    }
}
