use pyo3::prelude::*;

mod graph;
mod ingestion;
mod parse;
mod rules;
mod semantic;
mod types;

use graph::{CostGraphPy, PipelineGraphPy, PyCostNode, PyCostEdge, PyPipelineTable};
use rules::{Finding, Rule};
use types::{Cell, CellKind, AnalysisMode, Severity, Confidence, RuleEntry, RuleTable};

#[pyfunction]
fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

#[pyfunction]
#[pyo3(signature = (source, language=None))]
fn check(source: &str, language: Option<&str>) -> PyResult<PyObject> {
    let lang = language.unwrap_or("auto");
    
    Python::with_gil(|py| {
        if source.contains("import dlt") || source.contains("from dlt import") || 
           source.contains("CREATE STREAMING TABLE") || source.contains("@dlt.table") {
            use crate::graph::PipelineGraph;
            let pg = PipelineGraph::from_dlt(source);
            let pg_py: PipelineGraphPy = pg.into();
            return Ok(pg_py.into_py(py));
        }
        
        if source.trim().to_uppercase().starts_with("SELECT") || 
           source.trim().to_uppercase().starts_with("WITH") {
            use crate::graph::CostGraph;
            let cg = CostGraph::from_sql(source)?;
            let cg_py: CostGraphPy = cg.into();
            return Ok(cg_py.into_py(py));
        }
        
        use crate::graph::CostGraph;
        let cg = CostGraph::from_python(source)?;
        let cg_py: CostGraphPy = cg.into();
        Ok(cg_py.into_py(py))
    })
}

#[pyfunction]
#[pyo3(signature = (source, language=None))]
fn run_rules(source: &str, language: Option<&str>) -> Result<Vec<types::Finding>, PyErr> {
    let lang = language.unwrap_or("auto");
    rules::run(source, lang)
}

#[pyfunction]
fn list_rules() -> Vec<RuleEntry> {
    rules::list_all()
}

#[pyfunction]
pub fn get_registry_count() -> usize {
    rules::get_registry_count()
}

#[pymodule]
fn burnt_engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(check, m)?)?;
    m.add_function(wrap_pyfunction!(run_rules, m)?)?;
    m.add_function(wrap_pyfunction!(list_rules, m)?)?;
    m.add_function(wrap_pyfunction!(get_registry_count, m)?)?;
    
    m.add_class::<CostGraphPy>()?;
    m.add_class::<PipelineGraphPy>()?;
    m.add_class::<PyCostNode>()?;
    m.add_class::<PyCostEdge>()?;
    m.add_class::<PyPipelineTable>()?;
    m.add_class::<Finding>()?;
    m.add_class::<Rule>()?;
    m.add_class::<Cell>()?;
    m.add_class::<CellKind>()?;
    m.add_class::<AnalysisMode>()?;
    m.add_class::<Severity>()?;
    m.add_class::<Confidence>()?;
    m.add_class::<RuleEntry>()?;
    m.add_class::<RuleTable>()?;
    
    Ok(())
}