use crate::types::Finding;
use std::collections::HashSet;

pub struct ContextAnalyzer {
    in_loop: bool,
    loop_depth: u32,
    in_sdp_context: bool,
    generic_df_names: HashSet<String>,
    star_imports: Vec<(String, u32)>,
}

impl ContextAnalyzer {
    pub fn new() -> Self {
        Self {
            in_loop: false,
            loop_depth: 0,
            in_sdp_context: false,
            generic_df_names: HashSet::new(),
            star_imports: Vec::new(),
        }
    }

    pub fn check_with_column_in_loop(&self) -> bool {
        self.in_loop && self.loop_depth > 0
    }

    pub fn check_generic_df_name(&self, name: &str) -> bool {
        let name_lower = name.to_lowercase();
        name_lower == "df"
            || name_lower.starts_with("df")
                && name_lower.chars().skip(2).all(|c| c.is_ascii_digit())
    }

    pub fn check_star_import(&self, module: &str) -> bool {
        module == "pyspark.sql.functions"
    }
}

impl Default for ContextAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

pub fn analyze_context_for_rule(
    rule_code: &str,
    source: &str,
    _context_config: &ContextConfig,
) -> Vec<Finding> {
    match rule_code {
        "BP020" => check_with_column_in_loop(source),
        "BP021" => check_jdbc_partition(source),
        "BP022" => check_sdp_prohibited_ops(source),
        "BP023" => check_window_without_partition(source),
        "BNT-I01" => check_star_import_pyspark(source),
        "BNT-C01" => check_df_bracket_reference(source),
        "BNT-N01" => check_generic_df_name_var(source),
        "DLT004" => check_materialized_view_incremental(source),
        "BD001" => check_vacuum_frequency(source),
        "BQ003" => check_count_distinct_at_scale(source),
        "BQ004" => check_correlated_subquery(source),
        "SQ001" | "SQ002" | "SQ003" => vec![],
        "BP001" => check_cell_no_comment(source),
        "BP002" => check_long_line(source),
        "BP003" => check_magic_in_plain(source),
        "BP004" => check_deprecated_magic(source),
        _ => vec![],
    }
}

#[derive(Debug, Clone)]
pub struct ContextConfig {
    pub rule_code: String,
    pub context_type: String,
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
            findings.push(Finding {
                rule_id: "BP020".to_string(),
                code: "BP020".to_string(),
                severity: crate::types::Severity::Warning,
                message: ".withColumn() inside a loop causes O(n²) Catalyst plan analysis"
                    .to_string(),
                suggestion: Some(
                    "Use .withColumns() (Spark 3.3+) or a single .select() statement".to_string(),
                ),
                line_number: Some((i + 1) as u32),
                column: None,
                confidence: crate::types::Confidence::High,
            });
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
        findings.push(Finding {
            rule_id: "BP021".to_string(),
            code: "BP021".to_string(),
            severity: crate::types::Severity::Error,
            message:
                "JDBC read missing required partition options — reads entire table on single thread"
                    .to_string(),
            suggestion: Some(
                "Add partitionColumn, numPartitions, lowerBound, and upperBound options"
                    .to_string(),
            ),
            line_number: Some(1),
            column: None,
            confidence: crate::types::Confidence::High,
        });
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
                    findings.push(Finding {
                        rule_id: "BP022".to_string(),
                        code: "BP022".to_string(),
                        severity: crate::types::Severity::Error,
                        message: format!("Prohibited operation (.{}()) inside Spark Declarative Pipeline function", op),
                        suggestion: Some("Remove this operation from SDP pipeline code".to_string()),
                        line_number: Some((i + 1) as u32),
                        column: None,
                        confidence: crate::types::Confidence::High,
                    });
                }
            }
        }
    }

    findings
}

fn check_window_without_partition(source: &str) -> Vec<Finding> {
    let mut findings = Vec::new();

    let has_window_order = source.contains("Window.orderBy") || source.contains("Window.order_by");
    let has_partition_by = source.contains(".partitionBy(") || source.contains(".partition_by(");

    if has_window_order && !has_partition_by {
        findings.push(Finding {
            rule_id: "BP023".to_string(),
            code: "BP023".to_string(),
            severity: crate::types::Severity::Warning,
            message: "Window.orderBy() without .partitionBy() causes global sort".to_string(),
            suggestion: Some(
                "Add .partitionBy() before .orderBy() or use .orderBy().limit()".to_string(),
            ),
            line_number: Some(1),
            column: None,
            confidence: crate::types::Confidence::High,
        });
    }

    findings
}

fn check_star_import_pyspark(source: &str) -> Vec<Finding> {
    let mut findings = Vec::new();
    let lines: Vec<&str> = source.lines().collect();

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if trimmed.contains("from pyspark.sql.functions import *") {
            findings.push(Finding {
                rule_id: "BNT-I01".to_string(),
                code: "BNT-I01".to_string(),
                severity: crate::types::Severity::Error,
                message: "from pyspark.sql.functions import * shadows Python built-ins (max, min, sum, map, round)".to_string(),
                suggestion: Some("Use: from pyspark.sql import functions as F".to_string()),
                line_number: Some((i + 1) as u32),
                column: None,
                confidence: crate::types::Confidence::High,
            });
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
            findings.push(Finding {
                rule_id: "BNT-C01".to_string(),
                code: "BNT-C01".to_string(),
                severity: crate::types::Severity::Warning,
                message: "df['col'] or df.col outside a join can cause stale reference bugs after withColumn".to_string(),
                suggestion: Some("Use F.col('col') which resolves at evaluation time".to_string()),
                line_number: Some((i + 1) as u32),
                column: None,
                confidence: crate::types::Confidence::Medium,
            });
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
                findings.push(Finding {
                    rule_id: "BNT-N01".to_string(),
                    code: "BNT-N01".to_string(),
                    severity: crate::types::Severity::Info,
                    message: format!(
                        "Variable named '{}' is too generic — hinders readability",
                        var_name
                    ),
                    suggestion: Some(
                        "Use a descriptive name: orders_df, customers, filtered_events".to_string(),
                    ),
                    line_number: Some((i + 1) as u32),
                    column: None,
                    confidence: crate::types::Confidence::Low,
                });
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
        findings.push(Finding {
            rule_id: "DLT004".to_string(),
            code: "DLT004".to_string(),
            severity: crate::types::Severity::Warning,
            message: "Materialized view defined without incremental strategy".to_string(),
            suggestion: Some(
                "Consider incremental materialized view for large datasets".to_string(),
            ),
            line_number: Some(1),
            column: None,
            confidence: crate::types::Confidence::Medium,
        });
    }

    findings
}

fn check_vacuum_frequency(source: &str) -> Vec<Finding> {
    let mut findings = Vec::new();
    let lines: Vec<&str> = source.lines().collect();

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim().to_uppercase();
        if trimmed.contains("VACUUM") {
            findings.push(Finding {
                rule_id: "BD001".to_string(),
                code: "BD001".to_string(),
                severity: crate::types::Severity::Warning,
                message: "VACUUM called more frequently than needed".to_string(),
                suggestion: Some(
                    "Adjust vacuum retention based on table size and update frequency".to_string(),
                ),
                line_number: Some((i + 1) as u32),
                column: None,
                confidence: crate::types::Confidence::Low,
            });
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
            findings.push(Finding {
                rule_id: "BQ003".to_string(),
                code: "BQ003".to_string(),
                severity: crate::types::Severity::Info,
                message: "COUNT(DISTINCT col) requires full shuffle and sort — expensive at scale".to_string(),
                suggestion: Some("Consider approx_count_distinct() for large datasets where exact count is not required".to_string()),
                line_number: Some((i + 1) as u32),
                column: None,
                confidence: crate::types::Confidence::Medium,
            });
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
            findings.push(Finding {
                rule_id: "BQ004".to_string(),
                code: "BQ004".to_string(),
                severity: crate::types::Severity::Error,
                message: "NOT IN (subquery) with NULLs silently returns empty result".to_string(),
                suggestion: Some(
                    "Use NOT EXISTS or add WHERE col IS NOT NULL to the subquery".to_string(),
                ),
                line_number: Some((i + 1) as u32),
                column: None,
                confidence: crate::types::Confidence::High,
            });
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
                    findings.push(Finding {
                        rule_id: "BP001".to_string(),
                        code: "BP001".to_string(),
                        severity: crate::types::Severity::Info,
                        message: "Cell has no comments".to_string(),
                        suggestion: Some("Add comments for clarity".to_string()),
                        line_number: Some((cell_start_line + 1) as u32),
                        column: None,
                        confidence: crate::types::Confidence::Low,
                    });
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
                severity: crate::types::Severity::Info,
                message: format!("Line exceeds {} characters", max_line_length),
                suggestion: Some("Break line for readability".to_string()),
                line_number: Some((i + 1) as u32),
                column: Some(max_line_length as u32),
                confidence: crate::types::Confidence::High,
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
            findings.push(Finding {
                rule_id: "BP003".to_string(),
                code: "BP003".to_string(),
                severity: crate::types::Severity::Warning,
                message: "Databricks magic (# MAGIC) in plain Python file".to_string(),
                suggestion: Some("Remove magic or use .py (Databricks) extension".to_string()),
                line_number: Some((i + 1) as u32),
                column: None,
                confidence: crate::types::Confidence::High,
            });
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
            let magic_len = "# MAGIC".len();
            let after_magic = trimmed[magic_len..].trim();
            if !after_magic.is_empty() {
                let first_cmd = after_magic.split_whitespace().next().unwrap_or("");
                if deprecated_commands.contains(&first_cmd) {
                    findings.push(Finding {
                        rule_id: "BP004".to_string(),
                        code: "BP004".to_string(),
                        severity: crate::types::Severity::Warning,
                        message: format!(
                            "Deprecated Databricks magic syntax used: # MAGIC {}",
                            first_cmd
                        ),
                        suggestion: Some("Use new-style magic format".to_string()),
                        line_number: Some((i + 1) as u32),
                        column: None,
                        confidence: crate::types::Confidence::High,
                    });
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
        eprintln!(
            "DEBUG deprecated_magic: source={:?}, findings.len={}",
            source,
            findings.len()
        );
        assert!(!findings.is_empty());
        assert_eq!(findings[0].code, "BP004");
    }

    #[test]
    fn test_deprecated_magic_sql() {
        let source = "# MAGIC sql SELECT * FROM table\n";
        let findings = check_deprecated_magic(source);
        eprintln!(
            "DEBUG deprecated_magic_sql: source={:?}, findings.len={}",
            source,
            findings.len()
        );
        assert!(!findings.is_empty());
        assert_eq!(findings[0].code, "BP004");
    }
}
