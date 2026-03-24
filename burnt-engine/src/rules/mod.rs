use pyo3::prelude::*;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Finding {
    pub rule_id: String,
    pub code: String,
    pub severity: String,
    pub message: String,
    pub suggestion: Option<String>,
    pub line_number: Option<u32>,
    pub column: Option<u32>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Rule {
    pub id: String,
    pub code: String,
    pub severity: String,
    pub language: String,
    pub description: String,
    pub suggestion: String,
    pub category: String,
}

#[pyclass]
#[derive(Clone)]
pub struct Finding {
    #[pyo3(get)]
    pub rule_id: String,
    #[pyo3(get)]
    pub code: String,
    #[pyo3(get)]
    pub severity: String,
    #[pyo3(get)]
    pub message: String,
    #[pyo3(get)]
    pub suggestion: Option<String>,
    #[pyo3(get)]
    pub line_number: Option<u32>,
    #[pyo3(get)]
    pub column: Option<u32>,
}

#[pyclass]
#[derive(Clone)]
pub struct Rule {
    #[pyo3(get)]
    pub id: String,
    #[pyo3(get)]
    pub code: String,
    #[pyo3(get)]
    pub severity: String,
    #[pyo3(get)]
    pub language: String,
    #[pyo3(get)]
    pub description: String,
    #[pyo3(get)]
    pub suggestion: String,
    #[pyo3(get)]
    pub category: String,
}

pub fn run(_source: &str, _language: &str) -> Result<Vec<Finding>, PyErr> {
    Ok(vec![])
}

pub fn list_all() -> Vec<Rule> {
    vec![]
}