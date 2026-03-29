use std::{env, fs, path::Path};

fn main() {
    pyo3_build_config::use_pyo3_cfgs();

    // Generate registry module from rules/registry.toml
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("registry.rs");

    // Read registry.toml if it exists
    let registry_path = Path::new("rules/registry.toml");

    // Create enhanced registry parser that handles both old and new formats
    let code = r#"use std::sync::OnceLock;
use crate::types::{RuleEntry, Severity, CompiledRule, QueryPattern};

static REGISTRY_CACHE: OnceLock<Vec<RuleEntry>> = OnceLock::new();
static COMPILED_RULES_CACHE: OnceLock<Vec<CompiledRule>> = OnceLock::new();

pub fn load_registry() -> Vec<RuleEntry> {
    REGISTRY_CACHE.get_or_init(|| {
        parse_registry_toml(include_str!("../../../../../rules/registry.toml"))
    }).clone()
}

pub fn load_compiled_rules() -> Vec<CompiledRule> {
    COMPILED_RULES_CACHE.get_or_init(|| {
        parse_enhanced_registry_toml(include_str!("../../../../../rules/registry_with_queries.toml"))
    }).clone()
}

fn parse_registry_toml(toml_content: &str) -> Vec<RuleEntry> {
    use crate::types::Severity;
    
    let value: toml::Value = match toml::from_str(toml_content) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Failed to parse registry.toml: {}", e);
            return Vec::new();
        }
    };
    
    let empty_vec: Vec<toml::Value> = Vec::new();
    let rules = value.get("rules")
        .and_then(|v| v.as_array())
        .map_or(&empty_vec, |v| v);
    
    let mut entries = Vec::new();
    for rule in rules {
        let id = rule.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let code = rule.get("code").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let severity_str = rule.get("severity").and_then(|v| v.as_str()).unwrap_or("Info");
        let severity = match severity_str.to_lowercase().as_str() {
            "error" => Severity::Error,
            "warning" => Severity::Warning,
            _ => Severity::Info,
        };
        let language = rule.get("language").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let description = rule.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let suggestion = rule.get("suggestion").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let category = rule.get("category").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let tier = rule.get("tier").and_then(|v| v.as_integer()).unwrap_or(1) as u8;
        
        entries.push(RuleEntry {
            id,
            code,
            severity,
            language,
            description,
            suggestion,
            category,
            tier,
        });
    }
    
    entries
}

fn parse_enhanced_registry_toml(toml_content: &str) -> Vec<CompiledRule> {
    #[derive(serde::Deserialize)]
    struct RawPattern {
        #[serde(rename = "match", default)]
        match_pattern: String,
        #[serde(default)]
        negative: bool,
    }

    #[derive(serde::Deserialize)]
    struct RawRule {
        id: String,
        code: String,
        severity: String,
        language: String,
        description: String,
        suggestion: String,
        category: String,
        tier: u8,
        #[serde(default)]
        patterns: Vec<RawPattern>,
    }

    #[derive(serde::Deserialize)]
    struct Registry {
        #[serde(default)]
        rules: Vec<RawRule>,
    }

    let registry: Registry = match toml::from_str(toml_content) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Failed to parse enhanced registry.toml: {}", e);
            return Vec::new();
        }
    };

    registry.rules.into_iter().map(|raw_rule| {
        let severity = match raw_rule.severity.to_lowercase().as_str() {
            "error" => Severity::Error,
            "warning" => Severity::Warning,
            _ => Severity::Info,
        };

        let patterns = raw_rule.patterns.into_iter()
            .filter(|p| !p.match_pattern.is_empty())
            .map(|p| QueryPattern {
                match_pattern: p.match_pattern,
                is_negative: p.negative,
            })
            .collect();

        CompiledRule {
            id: raw_rule.id,
            code: raw_rule.code,
            severity,
            language: raw_rule.language,
            description: raw_rule.description,
            suggestion: raw_rule.suggestion,
            category: raw_rule.category,
            tier: raw_rule.tier,
            patterns,
        }
    }).collect()
}"#;

    fs::write(&dest_path, code).expect("Failed to write registry.rs");

    // Watch both registry files for changes
    if registry_path.exists() {
        println!("cargo:rerun-if-changed=rules/registry.toml");
    }
    let enhanced_path = Path::new("rules/registry_with_queries.toml");
    if enhanced_path.exists() {
        println!("cargo:rerun-if-changed=rules/registry_with_queries.toml");
    }
    println!("cargo:rerun-if-changed=build.rs");
}
