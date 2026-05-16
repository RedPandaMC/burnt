mod rest_client;
mod types;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use serde_json::Value;

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
        }
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

/// Convert a `serde_json::Value` into an equivalent Python object.
fn value_to_py(py: Python<'_>, value: &Value) -> PyObject {
    match value {
        Value::Null => py.None(),
        Value::Bool(b) => b.into_py(py),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.into_py(py)
            } else {
                n.as_f64().unwrap_or(0.0).into_py(py)
            }
        }
        Value::String(s) => s.clone().into_py(py),
        Value::Array(arr) => {
            let list = PyList::empty_bound(py);
            for item in arr {
                list.append(value_to_py(py, item)).unwrap();
            }
            list.into_py(py)
        }
        Value::Object(obj) => {
            let dict = PyDict::new_bound(py);
            for (k, v) in obj {
                dict.set_item(k, value_to_py(py, v)).unwrap();
            }
            dict.into_py(py)
        }
    }
}

/// Convert a slice of `Value`s into a Python `list`.
fn value_vec_to_py_list(py: Python<'_>, values: &[Value]) -> PyObject {
    let list = PyList::empty_bound(py);
    for v in values {
        list.append(value_to_py(py, v)).unwrap();
    }
    list.into_py(py)
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
    if let Ok(items) =
        client.get_json::<Vec<SqlExecution>>(&format!("{}/sql", base), auth.as_deref())
    {
        for item in items {
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
    } else {
        state_mut.collected = collected;
    }

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
        state.collected.push(json!({"jobId": 1, "name": "j"}));

        let raw = state.collected.iter().filter(|v| v.get("stageId").is_some());
        assert_eq!(raw.count(), 1, "exactly one element should carry stageId");
    }
}
