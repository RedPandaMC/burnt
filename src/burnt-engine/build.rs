use std::{env, fs, path::Path};

fn main() {
    pyo3_build_config::use_pyo3_cfgs();

    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("registry.rs");
    let tests_dest_path = Path::new(&out_dir).join("generated_tests.rs");

    let rules_dir = Path::new("rules");

    let mut all_rules = Vec::new();
    let mut test_cases = Vec::new();

    if rules_dir.exists() {
        for tier in &[1, 2, 3] {
            for lang in &["python", "sql", "notebook", "all"] {
                let tier_dir = rules_dir.join(format!("tier{}", tier)).join(lang);
                if tier_dir.exists() {
                    collect_rules_from_dir(&tier_dir, &mut all_rules, &mut test_cases, *tier, lang);
                }
            }
        }
    }

    let registry_code = generate_registry_code(&all_rules, &test_cases);
    let tests_code = generate_tests_code(&test_cases);

    fs::write(&dest_path, registry_code).expect("Failed to write registry.rs");
    fs::write(&tests_dest_path, tests_code).expect("Failed to write generated_tests.rs");

    println!("cargo:rerun-if-changed=rules/");
    println!("cargo:rerun-if-changed=build.rs");
}

fn collect_rules_from_dir(
    dir: &Path,
    rules: &mut Vec<String>,
    tests: &mut Vec<(String, String, Vec<String>, Vec<String>)>,
    tier: u8,
    language: &str,
) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("toml") {
                if let Ok(content) = fs::read_to_string(&path) {
                    if let Some((rule_code, test_case)) = parse_rule_file(&content, tier, language)
                    {
                        rules.push(rule_code);
                        if let Some((code, pass_tests, fail_tests)) = test_case {
                            tests.push((language.to_string(), code, pass_tests, fail_tests));
                        }
                    }
                }
            }
        }
    }
}

type TestCase = (String, Vec<String>, Vec<String>);
type RuleParseResult = Option<(String, Option<TestCase>)>;

fn parse_rule_file(content: &str, tier: u8, language: &str) -> RuleParseResult {
    let value: toml::Value = toml::from_str(content).ok()?;

    let rule = value.get("rule")?;
    let query = value.get("query");
    let context = value.get("context");
    let dataflow = value.get("dataflow");

    let id = rule.get("id")?.as_str()?.to_string();
    let code = rule.get("code")?.as_str()?.to_string();
    let severity = rule.get("severity")?.as_str()?;
    let desc = rule.get("description")?.as_str()?.to_string();
    let suggestion = rule
        .get("suggestion")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let category = rule.get("category").and_then(|v| v.as_str()).unwrap_or("");

    let (detect_patterns, exclude, has_patterns): (Vec<String>, Option<String>, bool) =
        if let Some(q) = query {
            if let Some(detect_val) = q.get("detect") {
                let patterns: Vec<String> = if let Some(arr) = detect_val.as_array() {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                } else if let Some(s) = detect_val.as_str() {
                    vec![s.to_string()]
                } else {
                    vec![]
                };
                let exclude_val: Option<String> = q
                    .get("exclude")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let has_patterns = !patterns.is_empty();
                (patterns, exclude_val, has_patterns)
            } else {
                (vec![], None, false)
            }
        } else {
            (vec![], None, false)
        };

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
        Some((code.clone(), pass_tests, fail_tests))
    } else {
        None
    };

    let patterns_str = if has_patterns {
        detect_patterns
            .iter()
            .map(|p| {
                format!(
                    "QueryPattern {{ match_pattern: r#\"{p}\"#.to_string(), is_negative: false }}"
                )
            })
            .collect::<Vec<_>>()
            .join(",\n                ")
    } else {
        String::new()
    };

    let exclude_str = if has_patterns {
        exclude
            .map(|e| format!(",\n                QueryPattern {{ match_pattern: r#\"{e}\"#.to_string(), is_negative: true }}"))
            .unwrap_or_default()
    } else {
        String::new()
    };

    let severity_variant = match severity.to_lowercase().as_str() {
        "error" => "Error",
        "warning" => "Warning",
        _ => "Info",
    };

    if !has_patterns && context.is_none() && dataflow.is_none() {
        eprintln!(
            "Warning: Rule {} has no patterns, context, or dataflow - skipping",
            code
        );
        return None;
    }

    if context.is_some() {
        eprintln!(
            "Note: Rule {} has context block - Tier 2 processing needed",
            code
        );
    }
    if dataflow.is_some() {
        eprintln!(
            "Note: Rule {} has dataflow block - Tier 3 processing needed",
            code
        );
    }

    if context.is_some() {
        eprintln!(
            "Note: Rule {} has context block - Tier 2 processing needed",
            code
        );
    }
    if dataflow.is_some() {
        eprintln!(
            "Note: Rule {} has dataflow block - Tier 3 processing needed",
            code
        );
    }

    let patterns_vec = if has_patterns {
        format!(
            "vec![\n                {}{}\n            ]",
            patterns_str, exclude_str
        )
    } else {
        String::from("vec![]")
    };

    let rule_code = format!(
        "CompiledRule {{\n\
            id: \"{id}\".to_string(),\n\
            code: \"{code}\".to_string(),\n\
            severity: Severity::{severity_variant},\n\
            language: \"{language}\".to_string(),\n\
            description: \"{desc}\".to_string(),\n\
            suggestion: \"{suggestion}\".to_string(),\n\
            category: \"{category}\".to_string(),\n\
            tier: {tier},\n\
            patterns: {patterns},\n\
        }}",
        patterns = patterns_vec
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
                         tier: r.tier,\n\
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
