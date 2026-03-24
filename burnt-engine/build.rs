use std::{env, fs, path::Path};

fn main() {
    pyo3_build_config::use_pyo3_cfgs();
    
    // Generate registry module from rules/registry.toml
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("registry.rs");
    
    // Read registry.toml if it exists
    let registry_path = Path::new("rules/registry.toml");
    
    // Create empty registry for now - will be loaded at runtime
    let code = r#"use std::sync::OnceLock;

static REGISTRY_CACHE: OnceLock<Vec<crate::types::RuleEntry>> = OnceLock::new();

pub fn load_registry() -> Vec<crate::types::RuleEntry> {
    REGISTRY_CACHE.get_or_init(|| {
        parse_registry_toml(include_str!("../../../../../rules/registry.toml"))
    }).clone()
}

fn parse_registry_toml(toml_content: &str) -> Vec<crate::types::RuleEntry> {
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
        
        entries.push(crate::types::RuleEntry {
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
}"#;
    
    fs::write(&dest_path, code).expect("Failed to write registry.rs");
    
    if registry_path.exists() {
        println!("cargo:rerun-if-changed=rules/registry.toml");
    }
    println!("cargo:rerun-if-changed=build.rs");
}