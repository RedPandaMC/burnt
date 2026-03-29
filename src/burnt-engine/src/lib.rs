use pyo3::prelude::*;

mod detect;
mod graph;
mod ingestion;
mod parse;
pub mod rules;
mod semantic;
mod types;

use detect::detect_mode_from_source;
use graph::{CostGraphPy, PipelineGraphPy, PyCostEdge, PyCostNode, PyPipelineTable};
use rules::{Finding, Rule};
use types::{AnalysisMode, Cell, CellKind, Confidence, RuleEntry, RuleTable, Severity};

#[pyfunction]
fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

#[pyfunction]
#[pyo3(signature = (source, language=None))]
fn check(source: &str, language: Option<&str>) -> PyResult<PyObject> {
    let _ = language;
    let mode = detect_mode_from_source(source);

    Python::with_gil(|py| match mode {
        AnalysisMode::Dlt => {
            use crate::graph::PipelineGraph;
            let pg = PipelineGraph::from_dlt(source);
            let pg_py: PipelineGraphPy = pg.into();
            Ok(pg_py.into_py(py))
        }
        AnalysisMode::Sql => {
            use crate::graph::CostGraph;
            let cg = CostGraph::from_sql(source)?;
            let cg_py: CostGraphPy = cg.into();
            Ok(cg_py.into_py(py))
        }
        AnalysisMode::Python => {
            use crate::graph::CostGraph;
            let cg = CostGraph::from_python(source)?;
            let cg_py: CostGraphPy = cg.into();
            Ok(cg_py.into_py(py))
        }
    })
}

#[pyfunction]
#[pyo3(signature = (source, language=None))]
fn run_rules(source: &str, language: Option<&str>) -> PyResult<Vec<types::Finding>> {
    let lang = language.unwrap_or("auto");
    rules::run(source, lang).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e))
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
