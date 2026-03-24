use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Binding {
    pub name: String,
    pub defined_at_line: u32,
    pub used_at_lines: Vec<u32>,
    pub kind: String,
}

pub fn analyze_bindings(source: &str) -> Vec<Binding> {
    let _ = source;
    vec![]
}