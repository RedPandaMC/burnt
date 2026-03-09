# Task: RuntimeBackend — SparkBackend + RestBackend + Auto-Detection

---

## Metadata

```yaml
id: s1-01-runtime-backend
status: todo
sprint: 1
priority: critical
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Build the dual-mode runtime backend that lets burnt run natively inside Databricks notebooks (via `SparkSession`) or externally (via REST API). This is the #1 blocker for the flagship `advise_current_session()` feature.

Without this, the tool is limited to offline static estimation — it can't access system tables, EXPLAIN plans, or session metrics when running inside Databricks.

### Files to Read

```
src/burnt/tables/connection.py    # Existing REST client (becomes RestBackend base)
src/burnt/core/protocols.py       # Existing protocol classes
src/burnt/core/config.py          # Settings with env vars
DESIGN.md § "RuntimeBackend"          # Architecture spec
```

### Files to Create

```
src/burnt/runtime/__init__.py
src/burnt/runtime/backend.py       # Backend protocol
src/burnt/runtime/spark_backend.py # In-cluster SparkSession backend
src/burnt/runtime/rest_backend.py  # External REST API backend (wraps existing DatabricksClient)
src/burnt/runtime/auto.py          # auto_backend() detection
tests/unit/test_runtime_backend.py
```

---

## Specification

### Backend Protocol (`runtime/backend.py`)

```python
from typing import Protocol, runtime_checkable

@runtime_checkable
class Backend(Protocol):
    def execute_sql(self, sql: str, warehouse_id: str | None = None) -> list[dict]: ...
    def get_cluster_config(self, cluster_id: str) -> ClusterConfig: ...
    def get_recent_queries(self, limit: int = 100) -> list[QueryRecord]: ...
    def describe_table(self, table_name: str) -> DeltaTableInfo: ...
    def get_session_metrics(self) -> dict[str, Any]: ...
```

### SparkBackend (`runtime/spark_backend.py`)

- Accepts a `SparkSession` instance
- `execute_sql()` → `spark.sql(query).collect()` → list of Row dicts
- `get_session_metrics()` → reads `SparkContext.statusTracker()` for active/completed stages
- `describe_table()` → `spark.sql("DESCRIBE DETAIL table")` → parse into `DeltaTableInfo`
- `get_recent_queries()` → `spark.sql("SELECT * FROM system.query.history ...")` with table_registry support
- Must handle `ImportError` for pyspark gracefully (optional dep pattern)

### RestBackend (`runtime/rest_backend.py`)

- Wraps the existing `DatabricksClient` from `tables/connection.py`
- Same `Backend` protocol interface
- `get_session_metrics()` → raises `NotAvailableError("Session metrics require in-cluster execution")`

### Auto-Detection (`runtime/auto.py`)

```python
def auto_backend() -> Backend | None:
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return SparkBackend(...)
    elif os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"):
        return RestBackend(...)
    else:
        return None
```

### Current Notebook Path Detection (in `auto.py`)

```python
def current_notebook_path() -> str | None:
    # 1. SparkConf: spark.databricks.notebook.path
    # 2. dbutils context
    # 3. ipynbname (local Jupyter)
    # 4. inspect.stack() for .py scripts
```

---

## Acceptance Criteria

- [ ] `Backend` protocol defined with `@runtime_checkable`
- [ ] `SparkBackend` implements all protocol methods using `spark.sql()`
- [ ] `RestBackend` wraps existing `DatabricksClient` and implements protocol
- [ ] `auto_backend()` correctly detects: in-cluster (DBR env var), external (HOST+TOKEN), offline (None)
- [ ] `current_notebook_path()` returns path via SparkConf, dbutils, ipynbname, or stack inspection
- [ ] PySpark import is guarded — `ImportError` handled gracefully with helpful message
- [ ] `EstimationPipeline` updated to accept a `Backend` instead of raw `DatabricksClient`
- [ ] All existing tests still pass
- [ ] New unit tests cover auto-detection logic and backend method routing

---

## Verification

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

---

## Handoff

```yaml
status: todo
```
