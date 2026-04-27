```yaml
id: PX-02-sparkmeasure-session
status: todo
phase: X
priority: critical
agent: ~
blocked_by: [PX-01-remove-dead-code]
created_by: planner
```

## Context

### Goal

Replace the broken SparkListener implementation in `_session.py` with sparkMeasure, and add a REST API fallback. The current implementation is non-functional: it passes a Python object to a JVM method that expects a Java interface, and falls back to `statusTracker()` which cannot capture shuffle bytes, spill, or CPU time.

### Files to modify

```
# Required
src/burnt/_session.py          (complete rewrite)
pyproject.toml                 (add spark optional extra)

# Reference
DESIGN.md §5 (Session Lifecycle)
tasks/P2/10-cost-estimation.md (SessionState shape it depends on)
```

### Background

**Why the current implementation is broken:**
1. `sc._jsc.sc().addSparkListener(listener)` — py4j cannot pass a Python object to a JVM method expecting a Java `SparkListener` interface. The call either throws or registers something inert.
2. `statusTracker()` returns only: `numCompletedTasks`, `numActiveTasks`. No bytes, no CPU time.

**sparkMeasure fix:**
sparkMeasure registers a native Scala `SparkListener` internally via py4j, solving the JVM interface problem. It exposes a clean Python API:

```python
from sparkmeasure import StageMetrics

stm = StageMetrics(spark)
stm.begin()
# user runs code
stm.end()
rows = stm.create_df().collect()  # list of Row objects
```

Each row has: `stageId`, `stageName`, `executorRunTime`, `executorCpuTime`, `shuffleReadBytes`, `shuffleWriteBytes`, `memoryBytesSpilled`, `diskBytesSpilled`, `inputBytes`, `outputBytes`.

**REST API fallback** (when sparkMeasure not installed):
```python
import requests
url = spark.sparkContext.uiWebUrl + "/api/v1/applications"
# GET {url}/{app_id}/stages → list of stage dicts with same field names
```

**SessionState shape (after this task):**
```python
class SessionState:
    active: bool
    _stm: StageMetrics | None       # sparkMeasure handle
    _ui_url: str | None             # for REST fallback
    collected: list[dict]           # populated at end() call
```

---

## Acceptance Criteria

- [ ] `pyproject.toml` has `spark = ["sparkmeasure>=2.0"]` under `[project.optional-dependencies]`
- [ ] `start_session()` with no active Spark → returns `SessionState(active=False)`, emits no error, no warning
- [ ] `start_session()` with Spark + sparkMeasure installed → `SessionState(active=True)`, `stm.begin()` called
- [ ] `start_session()` with Spark but no sparkMeasure → `warnings.warn("sparkmeasure not installed, install burnt[spark] for runtime metrics")`, attempts REST fallback
- [ ] `check()` calls `_session.end(state)` which: calls `stm.end()` + `stm.create_df().collect()` (or REST poll) → populates `state.collected`
- [ ] `state.collected` is a `list[dict]` with keys: `stage_id`, `name`, `executor_run_time_ms`, `shuffle_read_bytes`, `shuffle_write_bytes`, `memory_bytes_spilled`, `disk_bytes_spilled`, `input_bytes`
- [ ] Unit test: mock SparkSession + mock `StageMetrics` → `collected` populated correctly
- [ ] Unit test: no Spark → `SessionState(active=False)`, no exception
- [ ] Unit test: Spark present but sparkMeasure import fails → warning emitted, REST attempted
- [ ] No `contextlib.suppress(Exception)` — only catch `ImportError` for missing Spark/sparkMeasure

## Verification

```bash
uv run pytest tests/unit/test_session.py -v
uv run ruff check src/burnt/_session.py

# With Spark available (integration):
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local').getOrCreate()
import burnt
burnt.start_session()
spark.range(1000).groupBy().count().collect()
result = burnt.check('tests/fixtures/e2e/cross_join.py')
print(result.compute_seconds)
"
```

### Integration Check

- [ ] `burnt.start_session()` in a Databricks notebook (with `pip install burnt[spark]`) captures stage data after `burnt.check()` is called
