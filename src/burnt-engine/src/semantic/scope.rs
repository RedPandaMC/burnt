use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scope {
    pub name: String,
    pub variables: Vec<String>,
    pub parent: Option<String>,
}

pub fn analyze_scope(source: &str) -> Vec<Scope> {
    let _ = source;
    vec![]
}
