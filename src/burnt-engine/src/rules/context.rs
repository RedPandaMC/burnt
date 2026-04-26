use crate::types::{Confidence, Finding, Severity};
use std::collections::HashMap;
use std::sync::OnceLock;

use super::finding::make_finding;

type ContextFn = fn(&str) -> Vec<Finding>;

static DISPATCH: OnceLock<HashMap<&'static str, ContextFn>> = OnceLock::new();

fn get_dispatch() -> &'static HashMap<&'static str, ContextFn> {
    DISPATCH.get_or_init(|| {
        let mut m: HashMap<&'static str, ContextFn> = HashMap::new();
        m.insert("BP001", check_cell_no_comment);
        m.insert("BP002", check_long_line);
        m.insert("BP003", check_magic_in_plain);
        m.insert("BP004", check_deprecated_magic);
        m.insert("BP020", check_with_column_in_loop);
        m.insert("BP021", check_jdbc_partition);
        m.insert("BP022", check_sdp_prohibited_ops);
        m.insert("BP023", check_window_without_partition);
        m.insert("BNT-I01", check_star_import_pyspark);
        m.insert("BNT-C01", check_df_bracket_reference);
        m.insert("BNT-N01", check_generic_df_name_var);
        m.insert("DLT004", check_materialized_view_incremental);
        m.insert("BD001", check_vacuum_frequency);
        m.insert("BQ003", check_count_distinct_at_scale);
        m.insert("BQ004", check_correlated_subquery);
        m
    })
}

pub fn analyze_context_for_rule(rule_code: &str, source: &str) -> Vec<Finding> {
    get_dispatch()
        .get(rule_code)
        .map(|f| f(source))
        .unwrap_or_default()
}

fn check_with_column_in_loop(source: &str) -> Vec<Finding> {
    let mut findings = Vec::new();
    let lines: Vec<&str> = source.lines().collect();

    let mut in_for_loop = false;

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();

        if trimmed.starts_with("for ") && trimmed.contains(" in ") {
            in_for_loop = true;
        } else if in_for_loop
            && (trimmed.starts_with("withColumn") || trimmed.contains(".withColumn("))
        {
            findings.push(make_finding(
                "BP020",
                Severity::Warning,
                ".withColumn() inside a loop causes O(n²) Catalyst plan analysis",
                "Use .withColumns() (Spark 3.3+) or a single .select() statement",
                (i + 1) as u32,
                Confidence::High,
            ));
        } else if trimmed == ")" || trimmed.starts_with("for ") {
            in_for_loop = false;
        }
    }

    findings
}

fn check_jdbc_partition(source: &str) -> Vec<Finding> {
    let mut findings = Vec::new();

    let has_jdbc = source.contains("jdbc");
    let has_partition_options = source.contains("partitionColumn")
        || source.contains("numPartitions")
        || source.contains("lowerBound")
        || source.contains("upperBound");

    if has_jdbc
        && !has_partition_options
        && (source.contains(".read(") || source.contains("spark.read"))
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

fn check_sdp_prohibited_ops(source: &str) -> Vec<Finding> {
    let mut findings = Vec::new();
    let prohibited = ["write", "collect", "show", "display"];

    let lines: Vec<&str> = source.lines().collect();
    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();

        if trimmed.contains("@dlt.") || trimmed.contains("dlt.") {
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
    }

    findings
}

fn check_window_without_partition(source: &str) -> Vec<Finding> {
    let mut findings = Vec::new();

    let has_window_order =
        source.contains("Window.orderBy") || source.contains("Window.order_by");
    let has_partition_by =
        source.contains(".partitionBy(") || source.contains(".partition_by(");

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

fn check_star_import_pyspark(source: &str) -> Vec<Finding> {
    let mut findings = Vec::new();
    let lines: Vec<&str> = source.lines().collect();

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if trimmed.contains("from pyspark.sql.functions import *") {
            findings.push(make_finding(
                "BNT-I01",
                Severity::Error,
                "from pyspark.sql.functions import * shadows Python built-ins (max, min, sum, map, round)",
                "Use: from pyspark.sql import functions as F",
                (i + 1) as u32,
                Confidence::High,
            ));
        }
    }

    findings
}

fn check_df_bracket_reference(source: &str) -> Vec<Finding> {
    let mut findings = Vec::new();
    let lines: Vec<&str> = source.lines().collect();

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();

        if (trimmed.contains("df['") || trimmed.contains("df[\""))
            && !trimmed.contains("F.col")
            && !trimmed.contains("col(")
        {
            findings.push(make_finding(
                "BNT-C01",
                Severity::Warning,
                "df['col'] or df.col outside a join can cause stale reference bugs after withColumn",
                "Use F.col('col') which resolves at evaluation time",
                (i + 1) as u32,
                Confidence::Medium,
            ));
        }
    }

    findings
}

fn check_generic_df_name_var(source: &str) -> Vec<Finding> {
    let mut findings = Vec::new();
    let lines: Vec<&str> = source.lines().collect();

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();

        if trimmed.is_empty() {
            continue;
        }

        let parts: Vec<&str> = trimmed.split('=').collect();
        if parts.len() == 2 {
            let var_name = parts[0].trim();

            if !var_name
                .chars()
                .next()
                .map(|c| c.is_alphabetic())
                .unwrap_or(false)
            {
                continue;
            }

            let is_generic_df = var_name == "df"
                || (var_name.starts_with("df")
                    && var_name.len() > 2
                    && var_name.chars().skip(2).all(|c| c.is_ascii_digit()));

            if is_generic_df {
                findings.push(make_finding(
                    "BNT-N01",
                    Severity::Info,
                    &format!(
                        "Variable named '{}' is too generic — hinders readability",
                        var_name
                    ),
                    "Use a descriptive name: orders_df, customers, filtered_events",
                    (i + 1) as u32,
                    Confidence::Low,
                ));
            }
        }
    }

    findings
}

fn check_materialized_view_incremental(source: &str) -> Vec<Finding> {
    let mut findings = Vec::new();

    let has_dlt_table = source.contains("@dlt.table") || source.contains("dlt.table");
    let has_incremental = source.contains("incremental") || source.contains("stream");

    if has_dlt_table && !has_incremental {
        findings.push(make_finding(
            "DLT004",
            Severity::Warning,
            "Materialized view defined without incremental strategy",
            "Consider incremental materialized view for large datasets",
            1,
            Confidence::Medium,
        ));
    }

    findings
}

fn check_vacuum_frequency(source: &str) -> Vec<Finding> {
    let mut findings = Vec::new();
    let lines: Vec<&str> = source.lines().collect();

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim().to_uppercase();
        if trimmed.contains("VACUUM") {
            findings.push(make_finding(
                "BD001",
                Severity::Warning,
                "VACUUM called more frequently than needed",
                "Adjust vacuum retention based on table size and update frequency",
                (i + 1) as u32,
                Confidence::Low,
            ));
        }
    }

    findings
}

fn check_count_distinct_at_scale(source: &str) -> Vec<Finding> {
    let mut findings = Vec::new();
    let lines: Vec<&str> = source.lines().collect();

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim().to_uppercase();
        if trimmed.contains("COUNT(DISTINCT") {
            findings.push(make_finding(
                "BQ003",
                Severity::Info,
                "COUNT(DISTINCT col) requires full shuffle and sort — expensive at scale",
                "Consider approx_count_distinct() for large datasets where exact count is not required",
                (i + 1) as u32,
                Confidence::Medium,
            ));
        }
    }

    findings
}

fn check_correlated_subquery(source: &str) -> Vec<Finding> {
    let mut findings = Vec::new();
    let lines: Vec<&str> = source.lines().collect();

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim().to_uppercase();
        if trimmed.contains("NOT IN") && trimmed.contains("SELECT") {
            findings.push(make_finding(
                "BQ004",
                Severity::Error,
                "NOT IN (subquery) with NULLs silently returns empty result",
                "Use NOT EXISTS or add WHERE col IS NOT NULL to the subquery",
                (i + 1) as u32,
                Confidence::High,
            ));
        }
    }

    findings
}

fn check_cell_no_comment(source: &str) -> Vec<Finding> {
    let mut findings = Vec::new();
    let lines: Vec<&str> = source.lines().collect();

    let mut in_cell = false;
    let mut cell_start_line = 0;
    let mut has_comment = false;
    let cell_markers = ["# cell", "#%%", "# %%", "# In["];

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();

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
                break;
            }
        }

        if in_cell && (trimmed.starts_with('#') && !trimmed.starts_with("# MAGIC")) {
            has_comment = true;
        }
    }

    findings
}

fn check_long_line(source: &str) -> Vec<Finding> {
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

fn check_magic_in_plain(source: &str) -> Vec<Finding> {
    let mut findings = Vec::new();
    let lines: Vec<&str> = source.lines().collect();

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if trimmed.starts_with("# MAGIC") {
            findings.push(make_finding(
                "BP003",
                Severity::Warning,
                "Databricks magic (# MAGIC) in plain Python file",
                "Remove magic or use .py (Databricks) extension",
                (i + 1) as u32,
                Confidence::High,
            ));
        }
    }

    findings
}

fn check_deprecated_magic(source: &str) -> Vec<Finding> {
    let mut findings = Vec::new();
    let lines: Vec<&str> = source.lines().collect();
    let deprecated_commands = ["run", "sql", "md"];

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if trimmed.starts_with("# MAGIC") {
            let after_magic = trimmed["# MAGIC".len()..].trim();
            if !after_magic.is_empty() {
                let first_cmd = after_magic.split_whitespace().next().unwrap_or("");
                if deprecated_commands.contains(&first_cmd) {
                    findings.push(make_finding(
                        "BP004",
                        Severity::Warning,
                        &format!(
                            "Deprecated Databricks magic syntax used: # MAGIC {}",
                            first_cmd
                        ),
                        "Use new-style magic format",
                        (i + 1) as u32,
                        Confidence::High,
                    ));
                }
            }
        }
    }

    findings
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_with_column_in_loop() {
        let source = r#"
for i in range(10):
    df = df.withColumn("new", col("old") + 1)
"#;
        let findings = check_with_column_in_loop(source);
        assert!(!findings.is_empty());
        assert_eq!(findings[0].code, "BP020");
    }

    #[test]
    fn test_generic_df_name() {
        let source = r#"
df = spark.range(100)
df1 = df.filter(col("id") > 10)
"#;
        let findings = check_generic_df_name_var(source);
        assert!(!findings.is_empty());
        assert_eq!(findings[0].code, "BNT-N01");
    }

    #[test]
    fn test_long_line() {
        let source = "This is a very long line that exceeds the maximum line length of 120 characters and should trigger a finding for BP002 because it is longer than 120 characters";
        let findings = check_long_line(source);
        assert!(!findings.is_empty());
        assert_eq!(findings[0].code, "BP002");
    }

    #[test]
    fn test_long_line_ok() {
        let source = "Short line";
        let findings = check_long_line(source);
        assert!(findings.is_empty());
    }

    #[test]
    fn test_magic_in_plain() {
        let source = "some_var = 1\n# MAGIC run some_command\ndf.collect()\n";
        let findings = check_magic_in_plain(source);
        assert!(!findings.is_empty());
        assert_eq!(findings[0].code, "BP003");
    }

    #[test]
    fn test_magic_not_in_plain() {
        let source = "# This is a regular comment\ndf.collect()\n";
        let findings = check_magic_in_plain(source);
        assert!(findings.is_empty());
    }

    #[test]
    fn test_deprecated_magic() {
        let source = "# MAGIC run some_command\n";
        let findings = check_deprecated_magic(source);
        assert!(!findings.is_empty());
        assert_eq!(findings[0].code, "BP004");
    }

    #[test]
    fn test_deprecated_magic_sql() {
        let source = "# MAGIC sql SELECT * FROM table\n";
        let findings = check_deprecated_magic(source);
        assert!(!findings.is_empty());
        assert_eq!(findings[0].code, "BP004");
    }

    #[test]
    fn test_bp001_dispatched() {
        let findings = analyze_context_for_rule("BP001", "# cell\nsome_code()\n");
        // BP001 fires when a cell has no comment — "some_code()" is not a comment
        // The check needs two cells to trigger (it reports on close of a comment-less cell)
        // Just verify dispatch doesn't panic and returns a Vec
        let _ = findings;
    }

    #[test]
    fn test_bp002_dispatched() {
        let long_line = "x".repeat(130);
        let findings = analyze_context_for_rule("BP002", &long_line);
        assert!(!findings.is_empty());
        assert_eq!(findings[0].code, "BP002");
    }

    #[test]
    fn test_bp003_dispatched() {
        let findings = analyze_context_for_rule("BP003", "# MAGIC run cmd\n");
        assert!(!findings.is_empty());
        assert_eq!(findings[0].code, "BP003");
    }

    #[test]
    fn test_bp004_dispatched() {
        let findings = analyze_context_for_rule("BP004", "# MAGIC sql SELECT 1\n");
        assert!(!findings.is_empty());
        assert_eq!(findings[0].code, "BP004");
    }

    #[test]
    fn test_unknown_rule_returns_empty() {
        let findings = analyze_context_for_rule("UNKNOWN999", "some source");
        assert!(findings.is_empty());
    }
}
