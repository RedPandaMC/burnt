use std::path::Path;

fn main() {
    pyo3_build_config::use_pyo3_cfgs();

    let out_dir = std::env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("registry.rs");
    let tests_dest_path = Path::new(&out_dir).join("generated_tests.rs");

    let rules_dir = Path::new("rules");

    let mut all_rules = Vec::new();
    let mut test_cases = Vec::new();

    if rules_dir.exists() {
        collect_rules_recursive(rules_dir, &mut all_rules, &mut test_cases);
    }

    let registry_code = generate_registry_code(&all_rules, &test_cases);
    let tests_code = generate_tests_code(&test_cases);

    std::fs::write(&dest_path, registry_code).expect("Failed to write registry.rs");
    std::fs::write(&tests_dest_path, tests_code).expect("Failed to write generated_tests.rs");

    println!("cargo:rerun-if-changed=rules/");
    println!("cargo:rerun-if-changed=build.rs");
}

fn collect_rules_recursive(
    dir: &Path,
    rules: &mut Vec<String>,
    tests: &mut Vec<(String, String, Vec<String>, Vec<String>)>,
) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_rules_recursive(&path, rules, tests);
            } else if path.extension().and_then(|s| s.to_str()) == Some("toml") {
                if let Ok(content) = std::fs::read_to_string(&path) {
                    if let Some((rule_code, test_case)) = parse_rule_file(&content) {
                        rules.push(rule_code);
                        if let Some((code, language, pass_tests, fail_tests)) = test_case {
                            tests.push((language, code, pass_tests, fail_tests));
                        }
                    }
                }
            }
        }
    }
}

// (code, language, pass_tests, fail_tests)
type TestCase = (String, String, Vec<String>, Vec<String>);
type RuleParseResult = Option<(String, Option<TestCase>)>;

fn parse_rule_file(content: &str) -> RuleParseResult {
    let value: toml::Value = toml::from_str(content).ok()?;

    let rule = value.get("rule")?;
    let query = value.get("query");
    let context = value.get("context");
    let dataflow = value.get("dataflow");

    let id = rule.get("id")?.as_str()?.to_string();
    let code = rule.get("code")?.as_str()?.to_string();
    let severity = rule.get("severity")?.as_str()?;
    let language = rule.get("language")?.as_str()?.to_string();
    let desc = rule.get("description")?.as_str()?.to_string();
    let suggestion = rule
        .get("suggestion")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let category = rule.get("category").and_then(|v| v.as_str()).unwrap_or("");

    let tags: Vec<String> = rule
        .get("tags")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let detect_pattern: Option<String> = query
        .and_then(|q| q.get("detect"))
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string());

    let exclude_pattern: Option<String> = query
        .and_then(|q| q.get("exclude"))
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string());

    let has_query = detect_pattern.is_some();

    let pass_tests: Vec<String> = value
        .get("tests")
        .and_then(|t| t.get("pass"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let fail_tests: Vec<String> = value
        .get("tests")
        .and_then(|t| t.get("fail"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let test_case = if !pass_tests.is_empty() || !fail_tests.is_empty() {
        Some((code.clone(), language.clone(), pass_tests, fail_tests))
    } else {
        None
    };

    if !has_query && context.is_none() && dataflow.is_none() {
        eprintln!(
            "Warning: Rule {} has no queries, context, or dataflow - skipping",
            code
        );
        return None;
    }

    let severity_variant = match severity.to_lowercase().as_str() {
        "error" => "Error",
        "warning" => "Warning",
        _ => "Info",
    };

    fn escape(s: &str) -> String {
        s.replace('\\', "\\\\").replace('"', "\\\"")
    }

    let mut pattern_entries: Vec<String> = Vec::new();
    if let Some(p) = &detect_pattern {
        let esc = escape(p);
        pattern_entries.push(format!(
            "QueryPattern {{ match_pattern: \"{esc}\".to_string(), is_negative: false }}"
        ));
    }
    if let Some(p) = &exclude_pattern {
        let esc = escape(p);
        pattern_entries.push(format!(
            "QueryPattern {{ match_pattern: \"{esc}\".to_string(), is_negative: true }}"
        ));
    }
    let patterns_str = format!("vec![{}]", pattern_entries.join(", "));

    let tags_str = if tags.is_empty() {
        String::from("vec![]")
    } else {
        let tag_strs: Vec<String> = tags
            .iter()
            .map(|s| format!("\"{}\".to_string()", escape(s)))
            .collect();
        format!("vec![{}]", tag_strs.join(", "))
    };

    let desc_escaped = escape(&desc);
    let suggestion_escaped = escape(suggestion);

    let has_context = if context.is_some() { "true" } else { "false" };
    let has_dataflow = if dataflow.is_some() { "true" } else { "false" };

    let rule_code = format!(
        "CompiledRule {{\n\
            id: \"{id}\".to_string(),\n\
            code: \"{code}\".to_string(),\n\
            severity: Severity::{severity_variant},\n\
            language: \"{language}\".to_string(),\n\
            description: \"{desc}\".to_string(),\n\
            suggestion: \"{suggestion}\".to_string(),\n\
            category: \"{category}\".to_string(),\n\
            tags: {tags},\n\
            patterns: {patterns},\n\
            has_context: {has_context},\n\
            has_dataflow: {has_dataflow},\n\
        }}",
        desc = desc_escaped,
        suggestion = suggestion_escaped,
        tags = tags_str,
        patterns = patterns_str,
    );

    Some((rule_code, test_case))
}

fn generate_registry_code(
    rules: &[String],
    _tests: &[(String, String, Vec<String>, Vec<String>)],
) -> String {
    let rules_list = rules.join(",\n");

    format!(
        "use std::sync::OnceLock;\n\
         use crate::types::{{RuleEntry, Severity, CompiledRule, QueryPattern}};\n\
         \n\
         static REGISTRY_CACHE: OnceLock<Vec<RuleEntry>> = OnceLock::new();\n\
         static COMPILED_RULES_CACHE: OnceLock<Vec<CompiledRule>> = OnceLock::new();\n\
         \n\
         pub fn load_registry() -> Vec<RuleEntry> {{\n\
             REGISTRY_CACHE.get_or_init(|| {{\n\
                 load_compiled_rules()\n\
                     .into_iter()\n\
                     .map(|r| RuleEntry {{\n\
                         id: r.id.clone(),\n\
                         code: r.code.clone(),\n\
                         severity: r.severity.clone(),\n\
                         language: r.language.clone(),\n\
                         description: r.description.clone(),\n\
                         suggestion: r.suggestion.clone(),\n\
                         category: r.category.clone(),\n\
                         tags: r.tags.clone(),\n\
                      }})\n\
                      .collect()\n\
              }}).clone()\n\
          }}\n\
         \n\
         pub fn load_compiled_rules() -> Vec<CompiledRule> {{\n\
             COMPILED_RULES_CACHE.get_or_init(|| {{\n\
                 vec![\n\
                     {rules_list}\n\
                 ]\n\
             }}).clone()\n\
         }}"
    )
}


fn generate_tests_code(tests: &[(String, String, Vec<String>, Vec<String>)]) -> String {
    let mut test_fns = String::new();

    test_fns.push_str("use super::*;\n\n");

    for (language, code, pass_cases, fail_cases) in tests {
        let test_name = format!(
            "test_{}_{}",
            language.to_lowercase(),
            code.to_lowercase().replace('-', "_")
        );
        let pass_str = pass_cases
            .iter()
            .map(|s| format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let fail_str = fail_cases
            .iter()
            .map(|s| format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")))
            .collect::<Vec<_>>()
            .join(", ");

        let test_block = format!(
            "#[test]\n\
             fn {tn}() {{\n\
                  let pass_cases = vec![{pass}] as Vec<&str>;\n\
                  let fail_cases = vec![{fail}] as Vec<&str>;\n\
                  \n\
                  for source in pass_cases {{\n\
                      let findings = run(source, \"{lang}\").unwrap();\n\
                      assert!(!findings.iter().any(|f| f.code == \"{code}\"),\n\
                          \"Rule {code} should NOT fire for: {{}}\", source);\n\
                  }}\n\
                  \n\
                  for source in fail_cases {{\n\
                      let findings = run(source, \"{lang}\").unwrap();\n\
                      assert!(findings.iter().any(|f| f.code == \"{code}\"),\n\
                          \"Rule {code} SHOULD fire for: {{}}\", source);\n\
                  }}\n\
              }}\n",
            tn = test_name,
            lang = language,
            code = code,
            pass = pass_str,
            fail = fail_str
        );
        test_fns.push_str(&test_block);
    }

    test_fns
}
