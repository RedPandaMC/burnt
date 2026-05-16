//! Single canonical `serde_json::Value` → `PyObject` converter.
//!
//! Previously duplicated across `session::mod` and `plan_parser`; centralising
//! it here keeps the behaviour identical on both sides of the PyO3 boundary
//! and makes the conversion easy to test in isolation.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use serde_json::Value;

/// Convert a `serde_json::Value` into an equivalent Python object.
///
/// Numbers prefer `i64` when representable; otherwise fall back to `f64`.
/// Allocation errors during list/dict construction are surfaced via
/// `expect` with a descriptive message — they only occur on Python
/// allocator failure, which is unrecoverable anyway.
pub fn value_to_py(py: Python<'_>, value: &Value) -> PyObject {
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
                list.append(value_to_py(py, item))
                    .expect("PyList::append failed under stable allocator");
            }
            list.into_py(py)
        }
        Value::Object(obj) => {
            let dict = PyDict::new_bound(py);
            for (k, v) in obj {
                dict.set_item(k, value_to_py(py, v))
                    .expect("PyDict::set_item failed under stable allocator");
            }
            dict.into_py(py)
        }
    }
}

/// Convert a slice of `Value`s into a Python `list`.
pub fn value_vec_to_py_list(py: Python<'_>, values: &[Value]) -> PyObject {
    let list = PyList::empty_bound(py);
    for v in values {
        list.append(value_to_py(py, v))
            .expect("PyList::append failed under stable allocator");
    }
    list.into_py(py)
}
