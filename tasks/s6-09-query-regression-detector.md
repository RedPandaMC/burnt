# Task: Query Regression Detector

---

## Metadata

```yaml
id: s6-09-query-regression-detector
status: todo
phase: 6
priority: high
agent: ~
blocked_by: [s3-02-fingerprint-lookup, s4-04-table-registry]
created_by: planner
```

---

## Context

### Goal

Add `burnt.detect_query_regressions(warehouse_id, lookback_days, baseline_days, threshold_pct)`
that identifies queries that have become significantly more expensive than their historical
baseline. Uses fingerprint-based grouping to compare cost distributions across time windows.
Provides heuristic probable cause analysis to help engineers understand why a query regressed.

### Files to read

```
# Required
src/burnt/tables/queries.py        ← fingerprint_sql, find_similar_queries
src/burnt/tables/billing.py
src/burnt/core/table_registry.py
src/burnt/core/models.py
src/burnt/__init__.py

# Reference
DESIGN.md
tasks/s3-02-fingerprint-lookup.md   ← fingerprint infrastructure
tasks/s4-04-table-registry.md
```

### Background

**Module location:** `src/burnt/tables/regression.py`

**Detection algorithm:**

```
1. Group queries in system.query.history by fingerprint
2. For each fingerprint with ≥ 5 executions in both windows:
   baseline_p50 = median(duration_ms for run in baseline_window)
   recent_p50   = median(duration_ms for run in recent_window)
   regression_factor = recent_p50 / baseline_p50
   if regression_factor > (1 + threshold_pct / 100):
       flag as regression
```

**`QueryRegression` model:**
```python
@dataclass
class QueryRegression:
    fingerprint: str
    sample_query_text: str               # first 200 chars of a recent execution
    warehouse_id: str
    baseline_p50_ms: float
    recent_p50_ms: float
    regression_factor: float             # e.g. 2.5 = 2.5× slower
    estimated_cost_delta_usd_per_month: float
    probable_causes: list[str]
    confidence: str                      # "high" | "medium" | "low"
    first_seen: datetime                 # when regression started
    execution_count_recent: int
    execution_count_baseline: int
```

**Probable cause heuristics:**

| Signal | Probable cause |
|--------|----------------|
| `read_bytes` increased proportionally | `"data volume growth"` |
| `read_bytes` unchanged but duration increased | `"query plan change (likely stale statistics)"` |
| New table appeared in recent executions | `"new join added to query"` |
| Regression coincides with schema change event | `"schema change impact"` |
| Regression coincides with job onboarding spike in billing | `"resource contention"` |

**`QueryRegressionReport` model:**
```python
@dataclass
class QueryRegressionReport:
    warehouse_id: str
    baseline_days: int
    lookback_days: int
    threshold_pct: float
    regressions: list[QueryRegression]
    total_estimated_cost_delta_usd_per_month: float
    top_regression: QueryRegression | None    # largest regression_factor
```

---

## Acceptance Criteria

- [ ] `src/burnt/tables/regression.py` exists with `detect_query_regressions()` function
- [ ] `QueryRegression` and `QueryRegressionReport` models in `src/burnt/core/models.py`
- [ ] Fingerprint grouping uses `fingerprint_sql()` from `tables/queries.py`
- [ ] Requires ≥ 5 executions in each window before flagging a regression
- [ ] `regression_factor` = `recent_p50_ms / baseline_p50_ms`
- [ ] Only flags regressions above `threshold_pct` (default 50%, i.e. 1.5×)
- [ ] All 5 probable cause heuristics implemented
- [ ] `first_seen` set to earliest execution in recent window above baseline p50
- [ ] `estimated_cost_delta_usd_per_month` extrapolated from per-execution cost increase
- [ ] `burnt.detect_query_regressions()` exported from `src/burnt/__init__.py`
- [ ] `QueryRegressionReport.display()` shows ranked regression table with probable causes
- [ ] Unit tests: clear regression (3×), no regression, below minimum executions, all 5 heuristics
- [ ] All existing tests still pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

### Commands

```bash
uv run pytest -m unit -v -k "query_regression"
uv run ruff check src/ tests/
```

### Integration Check

- [ ] With mock query history (5 baseline executions at 1s, 5 recent at 3s, same fingerprint): `detect_query_regressions(threshold_pct=50)` returns 1 `QueryRegression` with `regression_factor ≈ 3.0`.

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked on s3-02 (fingerprint infrastructure must be in place) and s4-04 (table registry).
