#![allow(clippy::useless_conversion)]
use pyo3::prelude::*;

mod detect;
mod graph;
mod ingestion;
mod parse;
pub mod rules;
mod semantic;
mod types;

use detect::detect_mode_from_source;
use graph::{CostGraph, CostGraphPy, PipelineGraph, PipelineGraphPy};
use ingestion::files::ingest_file;
use types::{
    AnalysisMode, AnalysisResultPy, Cell, CellKind, PyCostEdge, PyCostNode, PyGraph, PyPipeline,
    PyPipelineTable, RuleEntry, RuleTable,
};

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
            let pg = PipelineGraph::from_dlt(source);
            let pg_py: PipelineGraphPy = pg.into();
            Ok(pg_py.into_py(py))
        }
        AnalysisMode::Sql => {
            let cg = CostGraph::from_sql(source)?;
            let cg_py: CostGraphPy = cg.into();
            Ok(cg_py.into_py(py))
        }
        AnalysisMode::Python => {
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
    rules::run(source, lang).map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

#[pyfunction]
fn list_rules() -> Vec<RuleEntry> {
    rules::list_all()
}

#[pyfunction]
pub fn get_registry_count() -> usize {
    rules::get_registry_count()
}

fn build_graph_and_pipeline(
    mode: &AnalysisMode,
    source: &str,
) -> PyResult<(Option<PyGraph>, Option<PyPipeline>)> {
    match mode {
        AnalysisMode::Dlt => {
            let pg = PipelineGraph::from_dlt(source);
            Ok((None, Some(PyPipeline::from_pipeline(pg))))
        }
        AnalysisMode::Sql => {
            let cg = CostGraph::from_sql(source)?;
            Ok((Some(PyGraph::from_cost_graph(cg)), None))
        }
        AnalysisMode::Python => {
            let cg = CostGraph::from_python(source)?;
            Ok((Some(PyGraph::from_cost_graph(cg)), None))
        }
    }
}

#[pyfunction]
#[pyo3(signature = (source, path=None))]
fn analyze_source(py: Python<'_>, source: &str, path: Option<&str>) -> PyResult<AnalysisResultPy> {
    py.allow_threads(|| {
        let mode = detect_mode_from_source(source);
        let findings = rules::run(source, mode.as_lang_str()).unwrap_or_default();

        let cell = Cell {
            kind: match mode {
                AnalysisMode::Dlt => CellKind::Python,
                AnalysisMode::Sql => CellKind::Sql,
                AnalysisMode::Python => CellKind::Python,
            },
            source: source.to_string(),
            byte_offset: 0,
            line_offset: 0,
            origin_path: path.map(std::path::PathBuf::from),
        };

        let (graph, pipeline) = build_graph_and_pipeline(&mode, source)?;

        Ok(AnalysisResultPy {
            mode: mode.to_string(),
            graph,
            pipeline,
            findings,
            cells: vec![cell],
            path: path.map(String::from),
        })
    })
}

#[pyfunction]
fn analyze_file(py: Python<'_>, path: &str) -> PyResult<AnalysisResultPy> {
    py.allow_threads(|| {
        let source_file = ingest_file(path).map_err(pyo3::exceptions::PyIOError::new_err)?;
        let mode = detect_mode_from_source(&source_file.content);
        let findings = rules::run(&source_file.content, mode.as_lang_str()).unwrap_or_default();
        let (graph, pipeline) = build_graph_and_pipeline(&mode, &source_file.content)?;

        Ok(AnalysisResultPy {
            mode: mode.to_string(),
            graph,
            pipeline,
            findings,
            cells: source_file.cells,
            path: Some(path.to_string()),
        })
    })
}

#[pyfunction]
fn analyze_directory(py: Python<'_>, path: &str) -> PyResult<Vec<AnalysisResultPy>> {
    py.allow_threads(|| {
        use rayon::prelude::*;

        let dir = std::path::Path::new(path);
        if !dir.exists() {
            return Err(pyo3::exceptions::PyIOError::new_err(format!(
                "Directory not found: {}",
                path
            )));
        }
        if !dir.is_dir() {
            return Err(pyo3::exceptions::PyIOError::new_err(format!(
                "Not a directory: {}",
                path
            )));
        }

        let entries: Vec<_> = std::fs::read_dir(dir)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .map(|e| e.path())
            .filter(|p| {
                let ext = p.extension().and_then(|e| e.to_str()).unwrap_or("");
                ext == "py" || ext == "sql" || ext == "ipynb"
            })
            .collect();

        let results: Vec<AnalysisResultPy> = entries
            .par_iter()
            .filter_map(|path| analyze_file_internal(&path.to_string_lossy()))
            .collect();

        Ok(results)
    })
}

fn analyze_file_internal(path: &str) -> Option<AnalysisResultPy> {
    let source_file = ingest_file(path).ok()?;
    let mode = detect_mode_from_source(&source_file.content);
    let findings = rules::run(&source_file.content, mode.as_lang_str()).unwrap_or_default();
    let (graph, pipeline) = build_graph_and_pipeline(&mode, &source_file.content).ok()?;

    Some(AnalysisResultPy {
        mode: mode.to_string(),
        graph,
        pipeline,
        findings,
        cells: source_file.cells,
        path: Some(path.to_string()),
    })
}

#[pymodule]
fn _engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(check, m)?)?;
    m.add_function(wrap_pyfunction!(run_rules, m)?)?;
    m.add_function(wrap_pyfunction!(list_rules, m)?)?;
    m.add_function(wrap_pyfunction!(get_registry_count, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_source, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_file, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_directory, m)?)?;

    m.add_class::<CostGraphPy>()?;
    m.add_class::<PipelineGraphPy>()?;
    m.add_class::<PyCostNode>()?;
    m.add_class::<PyCostEdge>()?;
    m.add_class::<PyPipelineTable>()?;
    m.add_class::<types::Finding>()?;
    m.add_class::<rules::Rule>()?;
    m.add_class::<Cell>()?;
    m.add_class::<CellKind>()?;
    m.add_class::<AnalysisMode>()?;
    m.add_class::<types::Severity>()?;
    m.add_class::<types::Confidence>()?;
    m.add_class::<RuleEntry>()?;
    m.add_class::<RuleTable>()?;
    m.add_class::<AnalysisResultPy>()?;
    m.add_class::<PyGraph>()?;
    m.add_class::<PyPipeline>()?;

    Ok(())
}
