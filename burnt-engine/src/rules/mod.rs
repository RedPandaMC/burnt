use pyo3::prelude::*;
use crate::types::{Finding as TypesFinding, RuleEntry, Severity, Confidence};

mod registry {
    include!(concat!(env!("OUT_DIR"), "/registry.rs"));
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
    #[pyo3(get)]
    pub confidence: String,
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
    #[pyo3(get)]
    pub tier: u8,
}

pub fn run(_source: &str, _language: &str) -> Result<Vec<TypesFinding>, PyErr> {
    Ok(vec![])
}

pub fn list_all() -> Vec<RuleEntry> {
    registry::load_registry()
}

#[pyfunction]
pub fn get_registry_count() -> usize {
    registry::load_registry().len()
}