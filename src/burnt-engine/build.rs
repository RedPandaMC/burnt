use std::collections::HashSet;
use std::path::Path;

fn main() {
    pyo3_build_config::use_pyo3_cfgs();

    let out_dir = std::env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("registry.rs");
    let tests_dest_path = Path::new(&out_dir).join("generated_tests.rs");

    let rules_dir = Path::new("rules");

    let mut all_rules = Vec::new();
    let mut test_cases = Vec::new();
    let mut seen_codes: HashSet<String> = HashSet::new();

    if rules_dir.exists() {
        collect_rules_recursive(rules_dir, &mut all_rules, &mut test_cases, &mut seen_codes);
    }

    let registry_code = generate_registry_code(&all_rules);
    let tests_code = generate_tests_code(&test_cases);

    std::fs::write(&dest_path, registry_code).expect("Failed to write registry.rs");
    std::fs::write(&tests_dest_path, tests_code).expect("Failed to write generated_tests.rs");

    println!("cargo:rerun-if-changed=build.rs");
}

fn collect_rules_recursive(
    dir: &Path,
    rules: &mut Vec<String>,
    tests: &mut Vec<(String, String, Vec<String>, Vec<String>)>,
    seen_codes: &mut HashSet<String>,
) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_rules_recursive(&path, rules, tests, seen_codes);
            } else if path.extension().and_then(|s| s.to_str()) == Some("toml") {
                println!("cargo:rerun-if-changed={}", path.display());

                if let Ok(content) = std::fs::read_to_string(&path) {
                    match parse_rule_file(&content, &path) {
                        Ok(Some((rule_code, test_case))) => {
                            if let Some(ref code) = extract_code_from_rule(&rule_code) {
                                if !seen_codes.insert(code.clone()) {
                                    eprintln!(
                                        "Warning: duplicate rule code '{}' in '{}' — skipping duplicate",
                                        code,
                                        path.display()
                                    );
                                    continue;
                                }
                            }
                            rules.push(rule_code);
                            if let Some((code, language, pass_tests, fail_tests)) = test_case {
                                tests.push((language, code, pass_tests, fail_tests));
                            }
                        }
                        Ok(None) => {}
                        Err(msg) => {
                            panic!(
                                "Malformed rule TOML in '{}': {}\n\
                                 Correct the file before resuming development.",
                                path.display(),
                                msg
                            );
                        }
                    }
                }
            }
        }
    }
}

fn extract_code_from_rule(rule_code: &str) -> Option<String> {
    rule_code.find("code: \"").and_then(|i| {
        let rest = &rule_code[i + 7..];
        rest.find('"').map(|j| rest[..j].to_string())
    })
}

// (code, language, pass_tests, fail_tests)
type TestCase = (String, String, Vec<String>, Vec<String>);
type RuleParseResult = Option<(String, Option<TestCase>)>;

fn parse_rule_file(content: &str, path: &Path) -> Result<RuleParseResult, String> {
    let value =
        toml::from_str::<toml::Value>(content).map_err(|e| format!("TOML parse error: {}", e))?;

    let rule = value
        .get("rule")
        .ok_or_else(|| format!("missing required `[rule]` section in '{}'", path.display()))?;
    let graph = value.get("graph");

    let id = rule
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("rule.id is missing or not a string in '{}'", path.display()))?;
    let code = rule.get("code").and_then(|v| v.as_str()).ok_or_else(|| {
        format!(
            "rule.code is missing or not a string in '{}'",
            path.display()
        )
    })?;
    let severity = rule
        .get("severity")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            format!(
                "rule.severity is missing or not a string in '{}'",
                path.display()
            )
        })?;
    let language = rule
        .get("language")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            format!(
                "rule.language is missing or not a string in '{}'",
                path.display()
            )
        })?;
    let desc = rule
        .get("description")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            format!(
                "rule.description is missing or not a string in '{}'",
                path.display()
            )
        })?;
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

    let requires_catalog: bool = rule
        .get("requires_catalog")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

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
        Some((
            code.to_string(),
            language.to_string(),
            pass_tests,
            fail_tests,
        ))
    } else {
        None
    };

    if graph.is_none() {
        eprintln!("Warning: Rule {} has no [graph] block — skipping", code);
        return Ok(None);
    }

    let graph_detect: Option<String> = graph
        .and_then(|g| g.get("detect"))
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string());
    let graph_exclude: Option<String> = graph
        .and_then(|g| g.get("exclude"))
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string());
    let has_graph = graph_detect.is_some();

    let graph_finding = graph.and_then(|g| g.get("finding"));
    let graph_finding_severity: Option<String> = graph_finding
        .and_then(|f| f.get("severity"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let graph_finding_confidence: Option<String> = graph_finding
        .and_then(|f| f.get("confidence"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let graph_finding_message: Option<String> = graph_finding
        .and_then(|f| f.get("message"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let graph_finding_suggestion: Option<String> = graph_finding
        .and_then(|f| f.get("suggestion"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let graph_finding_line: Option<String> = graph_finding
        .and_then(|f| f.get("line"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let severity_variant = match severity.to_lowercase().as_str() {
        "error" => "Error",
        "warning" => "Warning",
        _ => "Info",
    };

    if let Some(ref detect) = graph_detect {
        validate_predicate_names(detect, path);
    }

    fn escape(s: &str) -> String {
        s.replace('\\', "\\\\").replace('"', "\\\"")
    }

    let tags_str = if tags.is_empty() {
        String::from("vec![]")
    } else {
        let tag_strs: Vec<String> = tags
            .iter()
            .map(|s| format!("\"{}\".to_string()", escape(s)))
            .collect();
        format!("vec![{}]", tag_strs.join(", "))
    };

    let desc_escaped = escape(desc);
    let suggestion_escaped = escape(suggestion);

    let has_graph_bool = if has_graph { "true" } else { "false" };
    let has_catalog_bool = if requires_catalog { "true" } else { "false" };

    fn opt_string_literal(opt: Option<&str>) -> String {
        match opt {
            Some(s) => format!(
                "Some(\"{}\".to_string())",
                s.replace('\\', "\\\\").replace('"', "\\\"")
            ),
            None => "None".to_string(),
        }
    }
    let graph_detect_literal = escape(graph_detect.as_deref().unwrap_or(""));
    let graph_exclude_literal = opt_string_literal(graph_exclude.as_deref());
    let graph_severity_literal = opt_string_literal(graph_finding_severity.as_deref());
    let graph_confidence_literal = opt_string_literal(graph_finding_confidence.as_deref());
    let graph_message_literal = opt_string_literal(graph_finding_message.as_deref());
    let graph_suggestion_literal = opt_string_literal(graph_finding_suggestion.as_deref());
    let graph_line_literal = opt_string_literal(graph_finding_line.as_deref());

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
            has_graph: {has_graph_bool},\n\
            has_catalog: {has_catalog_bool},\n\
            graph_detect: \"{graph_detect}\".to_string(),\n\
            graph_exclude: {graph_exclude},\n\
            graph_finding_severity: {graph_severity},\n\
            graph_finding_confidence: {graph_confidence},\n\
            graph_finding_message: {graph_message},\n\
            graph_finding_suggestion: {graph_suggestion},\n\
            graph_finding_line: {graph_line},\n\
        }}",
        id = id,
        code = code,
        desc = desc_escaped,
        suggestion = suggestion_escaped,
        tags = tags_str,
        graph_detect = graph_detect_literal,
        graph_exclude = graph_exclude_literal,
        graph_severity = graph_severity_literal,
        graph_confidence = graph_confidence_literal,
        graph_message = graph_message_literal,
        graph_suggestion = graph_suggestion_literal,
        graph_line = graph_line_literal,
    );

    Ok(Some((rule_code, test_case)))
}

fn validate_predicate_names(detect: &str, path: &Path) {
    let known_predicates: HashSet<&'static str> = [
        // Composition
        "and",
        "or",
        "not",
        "xor",
        "implies",
        // Value / string
        "eq?",
        "not-eq?",
        "match?",
        "not-match?",
        "in",
        "starts-with",
        "ends-with",
        "contains",
        "kind",
        // Numeric
        "gt",
        "gte",
        "lt",
        "lte",
        "eq",
        // Quantifiers
        "count",
        "all",
        "any",
        "none",
        "exists",
        "exists-here",
        "unique",
        // Overlay / runtime
        "has-overlay",
        "has-provenance",
        "observed-bytes-gt",
        "table-spec-size-gt",
        // Bindings / dataflow
        "binds",
        "reads",
        "shares-receiver",
        // Value extraction
        "value-of",
        "method-of",
        "method-chain-of",
        "line-of",
        "column-of",
        "fqn-of",
        "overlay-of",
        // Conditional
        "when",
        // Traversal
        "descendants",
        "ancestors",
        "siblings",
        "receiver-of",
        "callees-of",
        // Expansion / stubs
        "fires-rule",
        "prop",
        "not-receiver-of",
        "kwargs/missing",
        "kwargs/has",
        // Node-level scope
        "in-loop",
        "method-chain-contains",
        "source-of",
        "self-join?",
        // Rule-specific predicates
        "join-type-mismatch",
        "arg-is-dynamic",
        // Head-kinds (used as subjects, not predicates per se)
        "call",
        "atom",
        "assign",
        "name",
        "import",
        "invoke",
    ]
    .into_iter()
    .collect();

    let mut pos = 0;
    let bytes = detect.as_bytes();
    while pos < bytes.len() {
        match bytes[pos] {
            b'"' => {
                pos += 1;
                while pos < bytes.len() && bytes[pos] != b'"' {
                    if bytes[pos] == b'\\' {
                        pos += 1;
                    }
                    pos += 1;
                }
                pos += 1;
            }
            b'#' => {
                pos += 1;
                let start = pos;
                while pos < bytes.len()
                    && !bytes[pos].is_ascii_whitespace()
                    && bytes[pos] != b'('
                    && bytes[pos] != b')'
                    && bytes[pos] != b'@'
                {
                    pos += 1;
                }
                if start < pos {
                    let name = &detect[start..pos];
                    if !known_predicates.contains(name) {
                        eprintln!(
                            "Warning: unknown predicate '#{}' in rule file '{}' — this will fail at match time",
                            name,
                            path.display()
                        );
                    }
                }
            }
            _ => pos += 1,
        }
    }
}

fn generate_registry_code(rules: &[String]) -> String {
    let rules_list = rules.join(",\n");

    format!(
        "use std::sync::OnceLock;\n\
         use crate::types::{{RuleEntry, Severity, CompiledRule}};\n\
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
