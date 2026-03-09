# Task: Integrate Delta metadata scan size into cost estimation

---

## Metadata

```yaml
id: p4-02-delta-scan-size
status: in-progress
phase: 4
priority: medium
agent: kimi
blocked_by: [p4-01-wire-explain-into-cli, p4a-01-critical-bug-fixes]
created_by: planner
```

---

## Context

### Goal

Extend `HybridEstimator` and `CostEstimator` to accept a `DeltaTableInfo` per referenced table, and use `total_size_bytes` as the scan size input instead of the EXPLAIN estimate when available. Tables referenced in the SQL (from `extract_tables()`) can be looked up via `parse_describe_detail()` or `read_delta_log()`.

### Files to read

```
# Required
src/burnt/estimators/hybrid.py
src/burnt/estimators/static.py
src/burnt/parsers/delta.py
src/burnt/parsers/sql.py        # extract_tables()
src/burnt/core/models.py
src/burnt/tables/connection.py
```

### Background

Currently `_explain_dbu()` in `HybridEstimator` uses `plan.total_size_bytes` from EXPLAIN. Delta metadata gives exact sizes from `_delta_log` without executing anything. Priority:

1. `DeltaTableInfo.total_size_bytes` (exact, from Delta log) — highest priority
2. `ExplainPlan.total_size_bytes` (estimated from optimizer stats)
3. SQL complexity score (pure static fallback)

The `estimate()` signature should be extended:
```python
def estimate(
    self,
    query: str,
    cluster: ClusterConfig,
    explain_plan: ExplainPlan | None = None,
    historical: list[QueryRecord] | None = None,
    delta_tables: dict[str, DeltaTableInfo] | None = None,  # table_name → info
) -> CostEstimate:
```

When `delta_tables` is provided, sum sizes for tables found in `extract_tables(query)`.

---

## Acceptance Criteria

- [ ] `HybridEstimator.estimate()` accepts `delta_tables` kwarg
- [ ] When `delta_tables` provided: scan DBU uses Delta sizes, not EXPLAIN sizes
- [ ] Partial `delta_tables` (only some tables): sum known tables, add EXPLAIN remainder
- [ ] `_explain_dbu()` updated to accept `override_size_bytes` parameter
- [ ] New unit tests in `tests/unit/estimators/test_hybrid_delta.py`
- [ ] Tests cover: delta only, delta + explain blend, missing tables fallback
- [ ] All 263+ existing tests still pass
- [ ] `uv run ruff check src/ tests/` exits 0

---

## Verification

```bash
uv run pytest -m unit -v tests/unit/estimators/test_hybrid_delta.py
uv run pytest -m unit -v 2>&1 | tail -3
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

---

## Handoff

### Result

[Executor: fill in after completion]

### Blocked reason

[If blocked, explain here]
