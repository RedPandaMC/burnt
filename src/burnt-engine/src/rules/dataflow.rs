use crate::types::Finding;
use std::collections::{HashMap, HashSet};

pub struct DataflowTracker {
    cache_operations: HashMap<String, Vec<u32>>,
    unpersist_operations: HashSet<String>,
    action_operations: HashMap<String, Vec<u32>>,
    df_names: HashSet<String>,
}

impl DataflowTracker {
    pub fn new() -> Self {
        Self {
            cache_operations: HashMap::new(),
            unpersist_operations: HashSet::new(),
            action_operations: HashMap::new(),
            df_names: HashSet::new(),
        }
    }

    pub fn track_cache(&mut self, df_name: &str, line: u32) {
        self.cache_operations
            .entry(df_name.to_string())
            .or_insert_with(Vec::new)
            .push(line);
    }

    pub fn track_unpersist(&mut self, df_name: &str) {
        self.unpersist_operations.insert(df_name.to_string());
    }

    pub fn track_action(&mut self, df_name: &str, line: u32, action: &str) {
        if ["collect", "count", "show", "take", "first"].contains(&action) {
            self.action_operations
                .entry(df_name.to_string())
                .or_insert_with(Vec::new)
                .push(line);
        }
    }

    pub fn check_cache_without_unpersist(&self) -> Vec<Finding> {
        let mut findings = Vec::new();

        for (df_name, cache_lines) in &self.cache_operations {
            if !self.unpersist_operations.contains(df_name) && !cache_lines.is_empty() {
                for &line in cache_lines {
                    findings.push(Finding {
                        rule_id: "BP030".to_string(),
                        code: "BP030".to_string(),
                        severity: crate::types::Severity::Warning,
                        message: ".cache() with no .unpersist() in the same scope — potential memory leak".to_string(),
                        suggestion: Some("Call .unpersist() when the cached DataFrame is no longer needed".to_string()),
                        line_number: Some(line),
                        column: None,
                        confidence: crate::types::Confidence::High,
                    });
                }
            }
        }

        findings
    }

    pub fn check_single_use_cache(&self) -> Vec<Finding> {
        let mut findings = Vec::new();

        for (df_name, cache_lines) in &self.cache_operations {
            let action_count = self
                .action_operations
                .get(df_name)
                .map(|v| v.len())
                .unwrap_or(0);
            if action_count == 1 && !cache_lines.is_empty() {
                for &line in cache_lines {
                    findings.push(Finding {
                        rule_id: "BP031".to_string(),
                        code: "BP031".to_string(),
                        severity: crate::types::Severity::Info,
                        message:
                            ".cache() on a DataFrame used only once adds overhead with no benefit"
                                .to_string(),
                        suggestion: Some(
                            "Remove .cache() if the DataFrame is only used in one action"
                                .to_string(),
                        ),
                        line_number: Some(line),
                        column: None,
                        confidence: crate::types::Confidence::Medium,
                    });
                }
            }
        }

        findings
    }

    pub fn check_repeated_actions_no_cache(&self) -> Vec<Finding> {
        let mut findings = Vec::new();

        for (df_name, action_lines) in &self.action_operations {
            let action_count = action_lines.len();
            if action_count >= 2 && !self.cache_operations.contains_key(df_name) {
                for &line in action_lines {
                    findings.push(Finding {
                        rule_id: "BP032".to_string(),
                        code: "BP032".to_string(),
                        severity: crate::types::Severity::Warning,
                        message: format!("Same DataFrame has {} action calls without .cache() — plan executed multiple times", action_count),
                        suggestion: Some("Call .cache() before the first action and .unpersist() afterward".to_string()),
                        line_number: Some(line),
                        column: None,
                        confidence: crate::types::Confidence::High,
                    });
                }
            }
        }

        findings
    }
}

impl Default for DataflowTracker {
    fn default() -> Self {
        Self::new()
    }
}

pub fn analyze_dataflow_for_rule(rule_code: &str, source: &str) -> Vec<Finding> {
    let _ = rule_code;
    check_dataflow_rules(source)
}

fn extract_df_assignment(line: &str) -> Option<String> {
    if line.contains(" = ")
        && (line.contains(".read")
            || line.contains(".table")
            || line.contains(".range")
            || line.contains(".createDataFrame"))
    {
        let parts: Vec<&str> = line.split('=').collect();
        if parts.len() >= 2 {
            let var_name = parts[0].trim();
            if var_name
                .chars()
                .next()
                .map(|c| c.is_alphabetic())
                .unwrap_or(false)
            {
                return Some(var_name.to_string());
            }
        }
    }
    None
}

fn extract_action(line: &str) -> Option<&'static str> {
    let actions = ["collect", "count", "show", "take", "first", "write"];
    for action in &actions {
        if line.contains(&format!(".{}(", action)) {
            return Some(action);
        }
    }
    None
}

pub fn check_dataflow_rules(source: &str) -> Vec<Finding> {
    let mut all_findings = Vec::new();

    let lines: Vec<&str> = source.lines().collect();
    let mut cache_ops: HashMap<String, Vec<u32>> = HashMap::new();
    let mut unpersist_ops: HashSet<String> = HashSet::new();
    let mut action_ops: HashMap<String, Vec<u32>> = HashMap::new();

    let known_dfs: Vec<String> = cache_ops.keys().chain(action_ops.keys()).cloned().collect();
    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        let line_num = (i + 1) as u32;

        let parts: Vec<&str> = trimmed.split('=').collect();
        if parts.len() >= 2 {
            let var_name = parts[0].trim();
            if var_name
                .chars()
                .next()
                .map(|c| c.is_alphabetic())
                .unwrap_or(false)
            {
                if trimmed.contains(".cache()") {
                    cache_ops
                        .entry(var_name.to_string())
                        .or_insert_with(Vec::new)
                        .push(line_num);
                } else if trimmed.contains(".unpersist()") {
                    unpersist_ops.insert(var_name.to_string());
                } else if let Some(_action) = extract_action(trimmed) {
                    action_ops
                        .entry(var_name.to_string())
                        .or_insert_with(Vec::new)
                        .push(line_num);
                }
            }
        }

        let known_dfs: Vec<String> = cache_ops.keys().chain(action_ops.keys()).cloned().collect();
        for df_name in &known_dfs {
            if trimmed.contains(&format!("{}.cache()", df_name)) {
                cache_ops
                    .entry(df_name.clone())
                    .or_insert_with(Vec::new)
                    .push(line_num);
            } else if trimmed.contains(&format!("{}.unpersist()", df_name)) {
                unpersist_ops.insert(df_name.clone());
            } else if let Some(_action) = extract_action(trimmed) {
                if trimmed.contains(&format!("{}.", df_name)) {
                    action_ops
                        .entry(df_name.clone())
                        .or_insert_with(Vec::new)
                        .push(line_num);
                }
            }
        }
    }

    for (df_name, lines) in &cache_ops {
        if !unpersist_ops.contains(df_name) && !lines.is_empty() {
            all_findings.push(Finding {
                rule_id: "BP030".to_string(),
                code: "BP030".to_string(),
                severity: crate::types::Severity::Warning,
                message: ".cache() with no .unpersist() in the same scope — potential memory leak"
                    .to_string(),
                suggestion: Some(
                    "Call .unpersist() when the cached DataFrame is no longer needed".to_string(),
                ),
                line_number: Some(lines[0]),
                column: None,
                confidence: crate::types::Confidence::High,
            });
        }

        let act_count = action_ops.get(df_name).map(|v| v.len()).unwrap_or(0);
        if act_count == 1 && !lines.is_empty() {
            all_findings.push(Finding {
                rule_id: "BP031".to_string(),
                code: "BP031".to_string(),
                severity: crate::types::Severity::Info,
                message: ".cache() on a DataFrame used only once adds overhead with no benefit"
                    .to_string(),
                suggestion: Some(
                    "Remove .cache() if the DataFrame is only used in one action".to_string(),
                ),
                line_number: Some(lines[0]),
                column: None,
                confidence: crate::types::Confidence::Medium,
            });
        }
    }

    for (df_name, action_lines) in &action_ops {
        let act_count = action_lines.len();
        if act_count >= 2 && !cache_ops.contains_key(df_name) {
            for &ln in action_lines {
                all_findings.push(Finding {
                    rule_id: "BP032".to_string(),
                    code: "BP032".to_string(),
                    severity: crate::types::Severity::Warning,
                    message: format!("Same DataFrame has {} action calls without .cache() — plan executed multiple times", act_count),
                    suggestion: Some("Call .cache() before the first action and .unpersist() afterward".to_string()),
                    line_number: Some(ln),
                    column: None,
                    confidence: crate::types::Confidence::High,
                });
            }
        }
    }

    all_findings
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_without_unpersist() {
        let source = r#"
cached_df = df.filter(col("x") > 10).cache()
result = cached_df.collect()
"#;
        let findings = check_dataflow_rules(source);
        let bp030: Vec<_> = findings.iter().filter(|f| f.code == "BP030").collect();
        assert!(!bp030.is_empty());
    }

    #[test]
    fn test_single_use_cache() {
        let source = r#"
cached_df = df.filter(col("x") > 10).cache()
result = cached_df.collect()
"#;
        let findings = check_dataflow_rules(source);
        let bp031: Vec<_> = findings.iter().filter(|f| f.code == "BP031").collect();
        assert!(!bp031.is_empty());
    }
}
