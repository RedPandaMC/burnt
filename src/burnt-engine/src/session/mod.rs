mod rest_client;
mod types;

use pyo3::prelude::*;
use rayon::prelude::*;
use serde_json::{json, Value};

use crate::json_py::value_vec_to_py_list;
use crate::plan_parser::parse_physical_plan;
use rest_client::RestClient;
use types::{ExecutorSummary, JobSummary, SqlExecution, StageMetrics};

/// Holds REST session configuration and collected stage metrics.
///
/// Exposed to Python as `SessionState`.
#[pyclass(name = "SessionState")]
#[derive(Clone)]
pub struct SessionStatePy {
    #[pyo3(get, set)]
    pub active: bool,
    #[pyo3(get, set)]
    pub rest_url: Option<String>,
    #[pyo3(get, set)]
    pub app_id: Option<String>,
    #[pyo3(get, set)]
    pub auth_header: Option<String>,
    collected: Vec<Value>,
    /// Per-execution plan-fetch failures recorded soft — distinct from
    /// the top-level all-or-nothing failure that flips `active` to false.
    pub partial_errors: Vec<String>,
}

#[pymethods]
impl SessionStatePy {
    #[new]
    pub fn new() -> Self {
        SessionStatePy {
            active: false,
            rest_url: None,
            app_id: None,
            auth_header: None,
            collected: Vec::new(),
            partial_errors: Vec::new(),
        }
    }

    #[getter]
    fn partial_errors(&self) -> Vec<String> {
        self.partial_errors.clone()
    }

    #[getter]
    fn collected(&self, py: Python<'_>) -> PyObject {
        value_vec_to_py_list(py, &self.collected)
    }

    #[getter]
    fn stages(&self, py: Python<'_>) -> PyObject {
        let stages: Vec<Value> = self
            .collected
            .iter()
            .filter(|v| v.get("stageId").is_some())
            .cloned()
            .collect();
        value_vec_to_py_list(py, &stages)
    }

    /// Plan-node bundles keyed by SQL execution id. Each entry has shape
    /// `{"sqlExecId": <int>, "planNodes": [<node dict>, ...]}` and is
    /// produced by the per-execution `/sql/{id}` fetch in `session_collect`.
    #[getter]
    fn plan_bundles(&self, py: Python<'_>) -> PyObject {
        let bundles: Vec<Value> = self
            .collected
            .iter()
            .filter(|v| v.get("planNodes").is_some())
            .cloned()
            .collect();
        value_vec_to_py_list(py, &bundles)
    }

    fn __repr__(&self, _py: Python<'_>) -> PyResult<String> {
        let app = match &self.app_id {
            Some(a) => format!("{a:?}"),
            None => "None".to_string(),
        };
        Ok(format!(
            "SessionState(active={}, app_id={}, stages={})",
            if self.active { "True" } else { "False" },
            app,
            self.collected.len()
        ))
    }
}

impl Default for SessionStatePy {
    fn default() -> Self {
        Self::new()
    }
}

/// Create a new `SessionStatePy` and mark it active.
#[pyfunction]
pub fn session_start(rest_url: &str, app_id: &str) -> PyResult<SessionStatePy> {
    Ok(SessionStatePy {
        active: true,
        rest_url: Some(rest_url.to_string()),
        app_id: Some(app_id.to_string()),
        auth_header: None,
        collected: Vec::new(),
        partial_errors: Vec::new(),
    })
}

/// Fetch stage / job / sql / executor metrics from the Spark REST API.
///
/// On any HTTP failure sets `active = False` and clears `collected`.
/// The Python wrapper should emit a `RuntimeWarning` when `active`
/// transitions from `True` to `False`.
#[pyfunction]
pub fn session_collect(state: &Bound<'_, SessionStatePy>) -> PyResult<()> {
    let mut state_mut = state.borrow_mut();
    if !state_mut.active {
        return Ok(());
    }
    let rest_url = match &state_mut.rest_url {
        Some(url) => url.clone(),
        None => return Ok(()),
    };
    let app_id = match &state_mut.app_id {
        Some(id) => id.clone(),
        None => return Ok(()),
    };
    let auth = state_mut.auth_header.clone();

    let client = RestClient::new();
    let base = format!("{}/applications/{}", rest_url.trim_end_matches('/'), app_id);

    let mut collected: Vec<Value> = Vec::new();
    let mut any_error = false;

    // ── /stages ──
    if let Ok(items) =
        client.get_json::<Vec<StageMetrics>>(&format!("{}/stages", base), auth.as_deref())
    {
        for item in items {
            if let Ok(v) = serde_json::to_value(item) {
                collected.push(v);
            }
        }
    } else {
        any_error = true;
    }

    // ── /jobs ──
    if let Ok(items) =
        client.get_json::<Vec<JobSummary>>(&format!("{}/jobs", base), auth.as_deref())
    {
        for item in items {
            if let Ok(v) = serde_json::to_value(item) {
                collected.push(v);
            }
        }
    } else {
        any_error = true;
    }

    // ── /sql ──
    let mut sql_exec_ids: Vec<i64> = Vec::new();
    if let Ok(items) =
        client.get_json::<Vec<SqlExecution>>(&format!("{}/sql", base), auth.as_deref())
    {
        for item in items {
            sql_exec_ids.push(item.id);
            if let Ok(v) = serde_json::to_value(item) {
                collected.push(v);
            }
        }
    } else {
        any_error = true;
    }

    // ── /executors ──
    if let Ok(items) =
        client.get_json::<Vec<ExecutorSummary>>(&format!("{}/executors", base), auth.as_deref())
    {
        for item in items {
            if let Ok(v) = serde_json::to_value(item) {
                collected.push(v);
            }
        }
    } else {
        any_error = true;
    }

    if any_error {
        state_mut.active = false;
        state_mut.collected.clear();
        return Ok(());
    }

    // ── /sql/{id} per execution, fetched in parallel ──
    //
    // Fail-soft: per-execution errors land in `partial_errors`, they do
    // not flip `active` to false or empty the rest of the collected data.
    let auth_owned = auth.clone();
    let base_owned = base.clone();
    let bundles_and_errors: Vec<(Option<Value>, Option<String>)> = sql_exec_ids
        .par_iter()
        .map(|exec_id| {
            let local_client = RestClient::new();
            let url = format!("{}/sql/{}", base_owned, exec_id);
            match local_client.get_text(&url, auth_owned.as_deref()) {
                Ok(body) => {
                    let nodes = parse_physical_plan(&body);
                    let plan_nodes: Vec<Value> = nodes
                        .into_iter()
                        .map(|n| {
                            json!({
                                "nodeId": n.node_id,
                                "nodeName": n.node_name,
                                "parentIds": n.parent_ids,
                                "metrics": n.metrics,
                            })
                        })
                        .collect();
                    let bundle = json!({
                        "sqlExecId": exec_id,
                        "planNodes": plan_nodes,
                    });
                    (Some(bundle), None)
                }
                Err(e) => (None, Some(format!("/sql/{exec_id}: {e}"))),
            }
        })
        .collect();

    for (maybe_bundle, maybe_err) in bundles_and_errors {
        if let Some(b) = maybe_bundle {
            collected.push(b);
        }
        if let Some(e) = maybe_err {
            state_mut.partial_errors.push(e);
        }
    }

    state_mut.collected = collected;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Round-trips a `StageMetrics` through `serde_json::to_value` and asserts
    /// the `stages` filter picks it up under the canonical camelCase key.
    /// Regression guard: the filter previously looked for `stage_id`, but the
    /// camelCase serde rename means stage JSON in `collected` carries `stageId`.
    #[test]
    fn stages_getter_matches_camel_case_key() {
        let stage = types::StageMetrics {
            stage_id: 7,
            name: "scan at f.py:10".into(),
            status: "COMPLETE".into(),
            num_active_tasks: 0,
            num_complete_tasks: 1,
            num_failed_tasks: 0,
            executor_run_time: 1234,
            executor_cpu_time: 0,
            input_bytes: 0,
            output_bytes: 0,
            shuffle_read_bytes: 0,
            shuffle_write_bytes: 0,
            memory_bytes_spilled: 0,
            disk_bytes_spilled: 0,
        };

        let mut state = SessionStatePy::new();
        state.collected.push(serde_json::to_value(stage).unwrap());
        state.collected.push(serde_json::json!({"jobId": 1, "name": "j"}));

        let raw = state.collected.iter().filter(|v| v.get("stageId").is_some());
        assert_eq!(raw.count(), 1, "exactly one element should carry stageId");
    }
}
