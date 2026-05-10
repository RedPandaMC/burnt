use crate::rules::context_structs::RuleContext;
use crate::rules::finding::make_finding;
use crate::types::{Confidence, Finding, Severity};

pub(super) fn check_sdp_prohibited_ops(source: &str, ctx: &RuleContext) -> Vec<Finding> {
    let prohibited = ["write", "collect", "show", "display"];
    let is_in_sdp_context = ctx.is_sdp_context();

    source
        .lines()
        .enumerate()
        .filter_map(|(i, line)| {
            let trimmed = line.trim();
            let contains_sdp_ref = trimmed.contains("@sdp.")
                || trimmed.contains("sdp.")
                || trimmed.contains("@dlt.")
                || trimmed.contains("dlt.")
                || trimmed.contains("@dp.")
                || trimmed.contains("dp.");

            if !is_in_sdp_context && !contains_sdp_ref {
                return None;
            }

            prohibited
                .iter()
                .find(|&&op| trimmed.contains(&format!(".{}(", op)))
                .map(|&op| {
                    make_finding(
                        "BP022",
                        Severity::Error,
                        &format!(
                            "Prohibited operation (.{}()) inside Spark Declarative Pipeline function",
                            op
                        ),
                        "Remove this operation from SDP pipeline code",
                        (i + 1) as u32,
                        Confidence::High,
                    )
                })
        })
        .collect()
}

pub(super) fn check_materialized_view_incremental(
    source: &str,
    ctx: &RuleContext,
) -> Vec<Finding> {
    let has_dlt_table = source.split('\n').any(|line| {
        let trimmed = line.trim();
        if !trimmed.starts_with('@') {
            return false;
        }
        let dec_part = trimmed.trim_start_matches('@');
        let ns = dec_part.split('.').next().unwrap_or("");
        ctx.tracker.is_pipeline_ns(ns) || matches!(ns, "sdp" | "dlt" | "dp")
    });

    let has_incremental = source.contains("incremental") || source.contains("stream");

    if has_dlt_table && !has_incremental {
        vec![make_finding(
            "SDP006",
            Severity::Warning,
            "Materialized view defined without incremental strategy",
            "Consider incremental materialized view for large datasets",
            1,
            Confidence::Medium,
        )]
    } else {
        vec![]
    }
}
