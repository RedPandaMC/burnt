# Task: TableRegistry + RuntimeBackend for Enterprise & In-Cluster Use

---

## Metadata

```yaml
id: p4a-03-table-registry-runtime-backend
status: todo
phase: 4C
priority: high
agent: ~
blocked_by: [p4a-02-estimation-pipeline, p4a-01b-remaining-bugs]
created_by: planner
```

---

## Context

### Goal

Two related enterprise blockers:
1. **TableRegistry**: 8 hardcoded `system.*` paths in 3 files. Enterprise environments use curated governance views. Without a configurable registry, burnt fails with permissions errors on every enterprise deployment.
2. **RuntimeBackend**: The package only works via REST from outside a cluster. 70% of Databricks usage is from notebooks inside the cluster, where `spark.sql()` is already available. Create a dual-mode backend and auto-detection.

### Files to read (executor reads ONLY these)

```
# Required
src/burnt/core/config.py
src/burnt/core/__init__.py
src/burnt/tables/billing.py
src/burnt/tables/queries.py
src/burnt/tables/compute.py
src/burnt/tables/connection.py
src/burnt/estimators/pipeline.py   # created in p4a-02
src/burnt/__init__.py
tests/unit/tables/

# Reference
files/02-ARCHITECTURE-GAPS.md    # §2.1 RuntimeBackend design, §2.5 top-level API
files/03-ENTERPRISE-SUPPORT.md   # Full TableRegistry design, config channels, column overrides
```

### Background

**TableRegistry** (from `files/03-ENTERPRISE-SUPPORT.md §3.2`):

```python
# src/burnt/core/table_registry.py
from dataclasses import dataclass, field

@dataclass(frozen=True)
class TableRegistry:
    billing_usage: str = "system.billing.usage"
    billing_list_prices: str = "system.billing.list_prices"
    query_history: str = "system.query.history"
    compute_node_types: str = "system.compute.node_types"
    compute_clusters: str = "system.compute.clusters"
    compute_node_timeline: str = "system.compute.node_timeline"
    lakeflow_jobs: str = "system.lakeflow.jobs"
    lakeflow_job_run_timeline: str = "system.lakeflow.job_run_timeline"
    predictive_optimization: str = "system.storage.predictive_optimization_operations_history"
    column_overrides: dict[str, dict[str, str]] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "TableRegistry":
        """Load overrides from BURNT_TABLE_* environment variables."""
        import os
        overrides = {}
        mapping = {
            "BURNT_TABLE_BILLING_USAGE": "billing_usage",
            "BURNT_TABLE_BILLING_LIST_PRICES": "billing_list_prices",
            "BURNT_TABLE_QUERY_HISTORY": "query_history",
            "BURNT_TABLE_COMPUTE_NODE_TYPES": "compute_node_types",
            "BURNT_TABLE_COMPUTE_CLUSTERS": "compute_clusters",
            "BURNT_TABLE_COMPUTE_NODE_TIMELINE": "compute_node_timeline",
        }
        for env_key, attr in mapping.items():
            if val := os.environ.get(env_key):
                overrides[attr] = val
        return cls(**overrides)
```

All `tables/*.py` modules should accept `registry: TableRegistry = TableRegistry()` and use `registry.billing_usage` instead of the hardcoded string.

**RuntimeBackend** (from `files/02-ARCHITECTURE-GAPS.md §2.2`):

```python
# src/burnt/runtime/__init__.py — Protocol
class RuntimeBackend(Protocol):
    def execute_sql(self, sql: str) -> list[dict[str, Any]]: ...
    def explain_cost(self, query: str) -> str: ...
    def describe_detail(self, table_name: str) -> dict[str, Any]: ...
    @property
    def is_connected(self) -> bool: ...

# SparkBackend — in-cluster
class SparkBackend:
    def __init__(self):
        from pyspark.sql import SparkSession
        self._spark = SparkSession.getActiveSession()
        if self._spark is None:
            raise RuntimeError("No active SparkSession")

    def execute_sql(self, sql: str) -> list[dict[str, Any]]:
        return [row.asDict() for row in self._spark.sql(sql).collect()]

    def explain_cost(self, query: str) -> str:
        return self._spark.sql(f"EXPLAIN COST {query}").collect()[0][0]

    def describe_detail(self, table_name: str) -> dict[str, Any]:
        rows = self._spark.sql(f"DESCRIBE DETAIL {table_name}").collect()
        return rows[0].asDict() if rows else {}

# RestBackend — external
class RestBackend:
    def __init__(self, client: DatabricksClient, warehouse_id: str): ...
    # delegates to DatabricksClient.execute_sql()

# Auto-detection
def auto_backend() -> RuntimeBackend | None:
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        try:
            return SparkBackend()
        except Exception:
            return None
    settings = Settings()
    if settings.workspace_url and settings.token:
        return RestBackend(DatabricksClient(settings), settings.warehouse_id or "")
    return None
```

`SparkBackend` must have `pyspark` as an optional import (not in pyproject.toml deps) — guard with try/except ImportError.

**Top-level API** (from `files/02-ARCHITECTURE-GAPS.md §2.6` + DESIGN.md §"Interaction Modes"):

```python
# src/burnt/__init__.py — add:
from burnt.estimators.pipeline import EstimationPipeline
from burnt.core.table_registry import TableRegistry
from burnt.core.models import CellEstimate

def estimate(query: str, cluster=None, registry=None, backend=None) -> CostEstimate:
    """Estimate cost of a SQL query or PySpark code string. Works in all 5 modes."""
    ...

def estimate_file(path: str, cluster=None, registry=None, backend=None) -> CostEstimate:
    """Estimate cost of a .sql, .py, .ipynb, or .dbc file."""
    ...

def estimate_notebook(path: str, cluster=None, registry=None, backend=None) -> CostEstimate:
    """Estimate cost of a notebook by path."""
    ...

def estimate_current_notebook(cluster=None, registry=None, backend=None) -> CostEstimate:
    """Estimate cost of the currently-running notebook (Mode 5). Detects path automatically."""
    path = current_notebook_path()
    if path is None:
        raise RuntimeError("Cannot detect current notebook path. Use estimate_notebook('/path/to/nb.ipynb') instead.")
    return estimate_notebook(path, cluster=cluster, registry=registry, backend=backend)

def estimate_cells(cluster=None, registry=None, backend=None) -> list[CellEstimate]:
    """Per-cell cost breakdown of the currently-running notebook (Mode 5)."""
    ...

def display(cell: int | None = None) -> None:
    """Render cost breakdown as a rich table. Uses displayHTML() in Databricks, rich in terminal."""
    ...

def current_notebook_path() -> str | None:
    """Detect path of currently-running notebook. Returns None if not detectable."""
    # 1. Databricks SparkConf
    # 2. dbutils context
    # 3. ipynbname (optional dep)
    # 4. inspect.stack() caller's __file__
    ...
```

`CellEstimate` model (add to `core/models.py`):
```python
@dataclass
class CellEstimate:
    index: int           # 1-based cell number
    language: str        # "sql" | "python" | "scala" | "markdown"
    source: str          # raw cell source
    estimated_dbu: float
    cost_usd: float
    confidence: str      # "high" | "medium" | "low"
    summary: str         # one-line description of most expensive operation
    anti_patterns: list  # list[AntiPattern]
```

---

## Acceptance Criteria

- [ ] `src/burnt/core/table_registry.py` exists with `TableRegistry` frozen dataclass
- [ ] `TableRegistry.from_env()` reads `BURNT_TABLE_*` env vars
- [ ] `tables/billing.py`, `tables/queries.py`, `tables/compute.py` all accept `registry` param
- [ ] No hardcoded `system.billing.usage` etc. in table query strings (use `registry.*`)
- [ ] `src/burnt/runtime/` package exists with `RuntimeBackend` Protocol, `SparkBackend`, `RestBackend`, `auto_backend()`
- [ ] `SparkBackend` raises `ImportError` with clear message if pyspark not installed
- [ ] `SparkBackend` raises `RuntimeError` if no active SparkSession
- [ ] `RestBackend` delegates to existing `DatabricksClient`
- [ ] `auto_backend()` returns `SparkBackend` when `DATABRICKS_RUNTIME_VERSION` env var is set
- [ ] `burnt.estimate("SELECT 1")` works from Python (top-level API)
- [ ] `burnt.estimate_file("query.sql")` works
- [ ] `burnt.estimate_notebook("/path/nb.ipynb")` works
- [ ] `burnt.estimate_current_notebook()` works when `DATABRICKS_RUNTIME_VERSION` env var is set (mock in test)
- [ ] `burnt.estimate_cells()` returns `list[CellEstimate]` for current notebook
- [ ] `burnt.current_notebook_path()` returns None (not raises) when no notebook context
- [ ] `CellEstimate` dataclass exists in `core/models.py` with all required fields
- [ ] `burnt.TableRegistry` exported from `__init__.py`
- [ ] All tests pass, zero lint errors

---

## Verification

### Commands

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
# Top-level API works offline
python -c "import burnt; r = burnt.estimate('SELECT 1'); print(r)"
# Registry env var respected
BURNT_TABLE_BILLING_USAGE=governance.cost.v_billing python -c "
from burnt.core.table_registry import TableRegistry
r = TableRegistry.from_env()
assert r.billing_usage == 'governance.cost.v_billing'
print('OK')
"
```

---

## Handoff

### Result

[Executor fills this in when done.]

```
status: todo
```

### Blocked reason

Blocked by `p4a-02-estimation-pipeline` — pipeline must exist before threading the backend through it.
