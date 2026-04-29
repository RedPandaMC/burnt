```yaml
id: P2-02-rest-session-client
status: todo
phase: 2
priority: critical
agent: ~
blocked_by: [P2-01a]
created_by: planner
```

## Context

### Goal

Implement a working session client using the Spark monitoring REST API. The old
implementation used `sc._jsc.sc().addSparkListener(listener)` — py4j cannot pass a
Python object to a JVM method expecting a Java interface, so this was always broken.
sparkMeasure is not a replacement: it is dropped entirely. The pivot to the native
REST API is the correct path.

### Files to modify

```
src/burnt/core/session_cost.py   (primary — moved from intelligence/)
src/burnt/__init__.py            (start_session() docstring)
src/burnt/cli/main.py            (any sparkMeasure import remnants)
tests/unit/test_session.py       (new or updated)
```

### Background

**REST endpoint discovery (in priority order):**

1. **Databricks:** check `dbruntime.dbutils` importability → construct driver-proxy URL:
   `https://{DATABRICKS_HOST}/driver-proxy-api/o/0/{CLUSTER_ID}/40001/api/v1`
   (cluster ID from `spark.conf.get("spark.databricks.clusterUsageTags.clusterId")`)
2. **Generic Spark UI:** `spark.sparkContext.uiWebUrl + "/api/v1"`
3. **No URL found:** `start_session()` returns `SessionState(active=False)`, no error.

**REST calls at `check()` time:**

```python
GET /applications/{app_id}/stages
GET /applications/{app_id}/jobs
GET /applications/{app_id}/sql
GET /applications/{app_id}/executors
```

Where `app_id` = `spark.sparkContext.applicationId`.

**Normalised metric keys per stage:**

```python
{
    "stage_id": int,
    "name": str,
    "executor_run_time_ms": int,
    "shuffle_read_bytes": int,
    "shuffle_write_bytes": int,
    "memory_bytes_spilled": int,
    "disk_bytes_spilled": int,
    "input_bytes": int,
}
```

Map from REST response keys (`executorRunTime` → `executor_run_time_ms`, etc.).

**SessionState shape:**

```python
class SessionState:
    active: bool
    _rest_url: str | None     # resolved at start_session() time
    _app_id: str | None
    collected: list[dict]     # populated at check() time
```

---

## Acceptance Criteria

- [ ] `start_session()` with no Spark → `SessionState(active=False)`, no error, no warning
- [ ] `start_session()` with Spark (Databricks env) → `_rest_url` set to driver-proxy URL
- [ ] `start_session()` with Spark (non-Databricks) → `_rest_url` set to `uiWebUrl`-based URL
- [ ] `check()` GETs `/stages`, `/jobs`, `/sql`, `/executors` using the resolved URL
- [ ] `state.collected` is `list[dict]` with normalised snake_case keys (see above)
- [ ] No `sparkMeasure` / `StageMetrics` / `sparkmeasure` import anywhere in `src/`
- [ ] Unit test: mock `requests.get` → `collected` populated correctly
- [ ] Unit test: no Spark → `SessionState(active=False)`, no exception
- [ ] Unit test: `requests.get` raises `ConnectionError` → warning emitted, `collected = []`
- [ ] `uv run ruff check src/burnt/core/session_cost.py` passes

## Verification

```bash
uv run pytest tests/unit/test_session.py -v
uv run ruff check src/burnt/core/session_cost.py

# Smoke test (Spark available locally):
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local').getOrCreate()
import burnt
burnt.start_session()
spark.range(1000).groupBy().count().collect()
result = burnt.check('tests/fixtures/e2e/cross_join.py')
print(result.compute_seconds)
"

# No sparkMeasure left:
grep -r 'sparkmeasure\|StageMetrics' src/ && echo FAIL || echo OK
```
