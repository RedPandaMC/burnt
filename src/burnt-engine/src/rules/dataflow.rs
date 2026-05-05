use crate::types::{Confidence, Finding, Severity};
use std::collections::{BTreeSet, HashMap, HashSet};

use super::finding::make_finding;

fn extract_action(line: &str) -> Option<&'static str> {
    let actions = ["collect", "count", "show", "take", "first", "write"];
    for action in &actions {
        if line.contains(&format!(".{}(", action)) {
            return Some(action);
        }
    }
    None
}

pub fn check_dataflow_rules(rule_code: &str, source: &str) -> Vec<Finding> {
    match rule_code {
        "BP030" | "BP031" | "BP032" => check_cache_lifecycle(rule_code, source),
        _ => vec![],
    }
}

fn check_cache_lifecycle(rule_code: &str, source: &str) -> Vec<Finding> {
    let mut findings = Vec::new();

    let lines: Vec<&str> = source.lines().collect();
    let mut cache_ops: HashMap<String, BTreeSet<u32>> = HashMap::new();
    let mut unpersist_ops: HashSet<String> = HashSet::new();
    let mut action_ops: HashMap<String, BTreeSet<u32>> = HashMap::new();
    let mut known_dfs: HashSet<String> = HashSet::new();

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
                    cache_ops.entry(var_name.to_string()).or_default().insert(line_num);
                    known_dfs.insert(var_name.to_string());
                } else if trimmed.contains(".unpersist()") {
                    unpersist_ops.insert(var_name.to_string());
                } else if extract_action(trimmed).is_some() {
                    action_ops.entry(var_name.to_string()).or_default().insert(line_num);
                    known_dfs.insert(var_name.to_string());
                }
            }
        }

        for df_name in &known_dfs {
            if trimmed.contains(&format!("{}.cache()", df_name)) {
                cache_ops.entry(df_name.clone()).or_default().insert(line_num);
            } else if trimmed.contains(&format!("{}.unpersist()", df_name)) {
                unpersist_ops.insert(df_name.clone());
            } else if extract_action(trimmed).is_some()
                && trimmed.contains(&format!("{}.", df_name))
            {
                action_ops.entry(df_name.clone()).or_default().insert(line_num);
            }
        }
    }

    for (df_name, cache_lines) in &cache_ops {
        let first_line = cache_lines.iter().next().copied().unwrap_or(1);

        if rule_code == "BP030" && !unpersist_ops.contains(df_name) {
            findings.push(make_finding(
                "BP030",
                Severity::Warning,
                ".cache() with no .unpersist() in the same scope — potential memory leak",
                "Call .unpersist() when the cached DataFrame is no longer needed",
                first_line,
                Confidence::High,
            ));
        }

        if rule_code == "BP031" {
            let act_count = action_ops.get(df_name).map(|s| s.len()).unwrap_or(0);
            if act_count == 1 {
                findings.push(make_finding(
                    "BP031",
                    Severity::Info,
                    ".cache() on a DataFrame used only once adds overhead with no benefit",
                    "Remove .cache() if the DataFrame is only used in one action",
                    first_line,
                    Confidence::Medium,
                ));
            }
        }
    }

    if rule_code == "BP032" {
        for (df_name, action_lines) in &action_ops {
            let act_count = action_lines.len();
            if act_count >= 2 && !cache_ops.contains_key(df_name) {
                for &ln in action_lines {
                    findings.push(make_finding(
                        "BP032",
                        Severity::Warning,
                        &format!(
                            "Same DataFrame has {} action calls without .cache() — plan executed multiple times",
                            act_count
                        ),
                        "Call .cache() before the first action and .unpersist() afterward",
                        ln,
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
    fn test_cache_without_unpersist() {
        let source = r#"
cached_df = df.filter(col("x") > 10).cache()
result = cached_df.collect()
"#;
        let findings = check_dataflow_rules("BP030", source);
        let bp030: Vec<_> = findings.iter().filter(|f| f.code == "BP030").collect();
        assert!(!bp030.is_empty());
    }

    #[test]
    fn test_single_use_cache() {
        let source = r#"
cached_df = df.filter(col("x") > 10).cache()
result = cached_df.collect()
"#;
        let findings = check_dataflow_rules("BP031", source);
        let bp031: Vec<_> = findings.iter().filter(|f| f.code == "BP031").collect();
        assert!(!bp031.is_empty());
    }
}
