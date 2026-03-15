# Task: Spark Listener Infrastructure (sparkMeasure Core + Native Fallback)

---

## Metadata

```yaml
id: s5-00-listener-infrastructure
status: todo
sprint: 5
priority: critical
agent: ~
blocked_by: [s1-01-runtime-backend]
created_by: planner
```

---

## Context

### Goal

Implement `src/burnt/runtime/listener.py` and `src/burnt/runtime/harvest.py` — the real-time metrics capture layer that instruments an active SparkSession. sparkMeasure is a **core dependency** that provides Spark Listener-based stage/task metrics with sub-second latency. A native Python fallback listener handles cases where the sparkMeasure JVM jar isn't on the classpath.

This replaces the previous design of passively reading `system.query.history` (which has 1–5 min ingestion delay) as the primary inference-time signal source.

### Files to Read

```
tasks/r7-ml-architecture-v4.md          # Architecture decisions
src/burnt/runtime/auto.py               # auto_backend() detection
src/burnt/runtime/__init__.py            # Backend protocol
src/burnt/__init__.py                    # burnt.init() entry point
```

### Background

**sparkMeasure** (github.com/LucaCanali/sparkMeasure) is built on the Spark Listener interface. Listeners transport executor Task Metrics from executors to the driver. sparkMeasure collects at stage-completion granularity.

**Available stage-level metrics from sparkMeasure:**
- `executorRunTime` (ms) — cumulative executor wall-clock time
- `executorCpuTime` (ns) — actual CPU time (excludes I/O waits)
- `jvmGCTime` (ms) — garbage collection time
- `bytesRead` / `bytesWritten` — I/O totals
- `shuffleBytesRead` / `shuffleBytesWritten` — shuffle I/O
- `shuffleRecordsRead` / `shuffleRecordsWritten` — shuffle record counts
- `diskBytesSpilled` / `memoryBytesSpilled` — memory pressure signals
- `peakExecutionMemory` — peak executor memory
- `recordsRead` / `recordsWritten` — row counts
- `numTasks` — task count

**Limitation:** sparkMeasure's Python wrapper requires the JVM class `ch.cern.sparkmeasure.StageMetrics` on the Spark driver classpath. If unavailable, burnt must fall back to a native Python listener with reduced metric coverage.

**Limitation:** sparkMeasure captures metrics only for successfully executed tasks. Resources used by failed tasks are not collected. PySpark UDF resource usage outside the JVM is not accounted for.

---

## Specification

### Data Models (`src/burnt/runtime/listener.py`)

```python
@dataclass(frozen=True, slots=True)
class CapturedStageMetrics:
    """Metrics captured from a single completed Spark stage."""
    stage_id: int
    stage_name: str
    num_tasks: int
    executor_run_time_ms: int = 0
    executor_cpu_time_ns: int = 0       # sparkMeasure only
    jvm_gc_time_ms: int = 0             # sparkMeasure only
    input_bytes: int = 0
    output_bytes: int = 0
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    disk_bytes_spilled: int = 0
    memory_bytes_spilled: int = 0
    peak_execution_memory: int = 0      # sparkMeasure only
    records_read: int = 0
    records_written: int = 0
    completion_time_ms: int = 0


@dataclass
class SessionMetrics:
    """Aggregated metrics across all stages in a session window."""
    stages: list[CapturedStageMetrics]
    collector_mode: str  # "sparkmeasure" | "native_listener" | "query_history"
    
    # Aggregation properties:
    # total_input_bytes, total_shuffle_read_bytes, total_shuffle_write_bytes,
    # total_disk_spill_bytes, total_memory_spill_bytes, total_executor_run_time_ms,
    # total_executor_cpu_time_ns, total_jvm_gc_time_ms, total_tasks,
    # peak_execution_memory_max
    
    def since(self, timestamp_ms: int) -> SessionMetrics:
        """Filter to stages completed after a given timestamp."""
    
    def for_query(self, query_start_ms: int, query_end_ms: int) -> SessionMetrics:
        """Filter to stages within a query's execution window."""
```

### Collector Classes

```python
class SparkMeasureCollector:
    """Primary collector: wraps sparkMeasure StageMetrics.
    
    Provides continuous background collection via Spark Listener.
    On attach(), registers the listener. All subsequent Spark actions
    have their stage metrics captured automatically.
    """
    
    def __init__(self, spark: SparkSession): ...
    def attach(self) -> None: ...
    def is_available(self) -> bool: ...
    def get_session_metrics(self) -> SessionMetrics: ...
    def get_aggregate(self) -> dict: ...
    def reset(self) -> None: ...


class NativeListener:
    """Fallback collector: pure Python Spark listener.
    
    Registers via spark.sparkContext._gateway.jvm to intercept
    onStageCompleted events. Captures a subset of metrics
    (no CPU time, GC time, or peak memory).
    """
    
    def __init__(self, spark: SparkSession): ...
    def attach(self) -> None: ...
    def get_session_metrics(self) -> SessionMetrics: ...
    def reset(self) -> None: ...
```

### Harvest Module (`src/burnt/runtime/harvest.py`)

```python
def harvest_session_metrics(
    spark: SparkSession,
    collector: SparkMeasureCollector | NativeListener,
    session_start_ms: int,
    *,
    enrich_from_query_history: bool = True,
) -> SessionMetrics:
    """Combine listener metrics with optional query.history enrichment.
    
    Priority:
    1. Listener metrics (real-time, <1s delay)
    2. system.query.history (1-5 min delay, has read_partitions)
    3. Cross-validate: if both available, warn on >20% divergence
    
    The enrichment adds read_partitions and from_results_cache
    (not available from listeners) when query.history has caught up.
    """
```

### `burnt.init()` Integration

```python
# src/burnt/__init__.py

_state = _BurntState()  # module-level singleton

def init(spark: SparkSession) -> None:
    """Initialize burnt metrics collection on the active SparkSession.
    
    Must be called once per notebook session before advise_current_session().
    
    1. Attempts SparkMeasureCollector (requires JVM jar)
    2. Falls back to NativeListener (pure Python, always works)
    3. Records session start timestamp
    4. Logs collector mode
    """
```

### Dependency Configuration

```toml
# pyproject.toml
[project]
dependencies = [
    "pydantic>=2.0,<3",
    "pydantic-settings>=2.0",
    "sparkmeasure>=0.24",
]
```

The `sparkmeasure` PyPI package (~50KB) is a core dependency. The JVM jar (`ch.cern.sparkmeasure:spark-measure_2.12`) must be on the Spark driver classpath for full functionality. If the JVM class is not loaded, burnt detects this at `init()` time and falls back to `NativeListener`.

---

## Acceptance Criteria

- [ ] `src/burnt/runtime/listener.py` created
  - `CapturedStageMetrics` dataclass with all fields
  - `SessionMetrics` dataclass with aggregation properties and filtering methods
  - `SparkMeasureCollector` — attaches, captures, returns `SessionMetrics`
  - `NativeListener` — pure Python fallback, same `SessionMetrics` output
- [ ] `src/burnt/runtime/harvest.py` created
  - `harvest_session_metrics()` combines listener + optional query.history
  - Cross-validation warning when sources diverge >20%
- [ ] `burnt.init(spark)` in `__init__.py`
  - Auto-detects sparkMeasure JVM availability
  - Falls back to NativeListener gracefully
  - Logs collector mode ("sparkmeasure" or "native_listener")
  - Records `session_start_ms`
- [ ] `sparkmeasure>=0.24` added to core dependencies in `pyproject.toml`
- [ ] New unit tests: `tests/unit/runtime/test_listener.py`
  - `SessionMetrics` aggregation properties
  - `SessionMetrics.since()` timestamp filtering
  - `SparkMeasureCollector.is_available()` when JVM class missing
  - `NativeListener` captures stage data (mock SparkContext)
  - `harvest_session_metrics()` merge logic
- [ ] All existing tests pass: `uv run pytest -m unit -v`
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

```bash
uv run pytest -m unit -v tests/unit/runtime/test_listener.py
uv run pytest -m unit -v 2>&1 | tail -5
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

### Integration Check

- [ ] In a Databricks notebook: `import burnt; burnt.init(spark)` logs collector mode
- [ ] After running a query, `burnt._state.collector.get_session_metrics()` returns non-empty `SessionMetrics`

---

## Handoff

```yaml
status: todo
```
