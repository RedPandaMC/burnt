use pyo3::prelude::*;

mod graph;
mod ingestion;
mod parse;
mod rules;
mod semantic;
mod types;

use graph::{CostGraph, PipelineGraph, Finding, Rule};

#[pyfunction]
fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

#[pyfunction]
fn check(source: &str, language: Option<&str>) -> Result<CostGraph, PyErr> {
    let lang = language.unwrap_or("auto");
    
    if source.contains("import dlt") || source.contains("from dlt import") || 
       source.contains("CREATE STREAMING TABLE") || source.contains("@dlt.table") {
        return Ok(PipelineGraph::from_dlt(source).into());
    }
    
    if source.trim().to_uppercase().starts_with("SELECT") || 
       source.trim().to_uppercase().starts_with("WITH") {
        return Ok(CostGraph::from_sql(source).into());
    }
    
    CostGraph::from_python(source)
}

#[pyfunction]
fn run_rules(source: &str, language: Option<&str>) -> Result<Vec<Finding>, PyErr> {
    let lang = language.unwrap_or("auto");
    rules::run(source, lang)
}

#[pyfunction]
fn list_rules() -> Vec<Rule> {
    rules::list_all()
}

#[pymodule]
fn burnt_engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(check, m)?)?;
    m.add_function(wrap_pyfunction!(run_rules, m)?)?;
    m.add_function(wrap_pyfunction!(list_rules, m)?)?;
    
    m.add_class::<CostGraph>()?;
    m.add_class::<PipelineGraph>()?;
    m.add_class::<Finding>()?;
    m.add_class::<Rule>()?;
    
    Ok(())
}