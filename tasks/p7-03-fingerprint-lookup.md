# Task: Wire historical fingerprint lookup into estimate command

---

## Metadata

```yaml
id: p4-03-fingerprint-lookup
status: todo
phase: 4
priority: high
agent: ~
blocked_by: [p4-01-wire-explain-into-cli, p4a-01-critical-bug-fixes, p4-02-delta-scan-size]
created_by: planner
```

---

## Context

### Goal

When `--warehouse-id` is set and a Databricks connection is available, look up the SQL fingerprint in `system.query.history` before running EXPLAIN. If matching historical executions exist, pass them to `HybridEstimator` as the `historical` argument. This gives the highest-accuracy estimates for recurring queries.

### Files to read

```
# Required
src/burnt/cli/main.py            (after p4-01)
src/burnt/tables/queries.py      # fingerprint_sql, find_similar_queries
src/burnt/estimators/hybrid.py
src/burnt/core/config.py
src/burnt/tables/connection.py
```

### Background

Lookup flow in the CLI:
1. Compute `fingerprint_sql(query)`
2. Call `find_similar_queries(client, fingerprint, warehouse_id, limit=20)`
3. If results: pass to `HybridEstimator(..., historical=records)`
4. If none: proceed with EXPLAIN-only or static

The output table should show `Signal: historical (N executions)` when historical records are used.

---

## Acceptance Criteria

- [ ] CLI fingerprints the query before running EXPLAIN
- [ ] If `find_similar_queries` returns ≥1 record: `historical` passed to `HybridEstimator`
- [ ] Output shows number of historical matches used
- [ ] Historical lookup failure (network error) is caught, warning printed, continues without historical
- [ ] Unit tests in `tests/unit/cli/test_fingerprint_lookup.py`
- [ ] All existing tests still pass
- [ ] `uv run ruff check src/ tests/` exits 0

---

## Verification

```bash
uv run pytest -m unit -v tests/unit/cli/
uv run pytest -m unit -v 2>&1 | tail -3
uv run ruff check src/ tests/
```

---

## Handoff

### Result

[Executor: fill in after completion]

### Blocked reason

[If blocked, explain here]
