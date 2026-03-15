# Task: Wire historical fingerprint lookup into estimate command

---

## Metadata

```yaml
id: s3-02-fingerprint-lookup
status: todo
sprint: 3
priority: high
agent: ~
blocked_by: [s1-01-runtime-backend, s3-01-delta-scan-integration]
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

### Fuzzy matching (second-tier)

After exact fingerprint match fails, implement a second-tier fuzzy match using sqlglot
AST edit distance. Compute the AST of the candidate query and compare against a sample
of recent query ASTs from `system.query.history`. Queries within edit distance ≤ 3 (e.g.,
one added WHERE clause, one changed column name) are considered "structurally similar"
and can be used for historical cost estimation with a lower confidence weight.

**Implementation:**
```python
def find_similar_queries_fuzzy(
    client: DatabricksClient,
    query_ast: Expression,
    warehouse_id: str,
    max_edit_distance: int = 3,
    sample_size: int = 100,
) -> list[QueryRecord]:
    """Second-tier: AST edit distance match against recent queries."""
    recent = client.get_recent_queries(warehouse_id=warehouse_id, limit=sample_size)
    matches = []
    for record in recent:
        candidate_ast = sqlglot.parse_one(record.query_text, error_level="ignore")
        if candidate_ast and ast_edit_distance(query_ast, candidate_ast) <= max_edit_distance:
            matches.append(record)
    return matches
```

The `ast_edit_distance()` function uses tree edit distance on sqlglot `Expression` trees.
A simple implementation counts node insertions, deletions, and relabellings.

Confidence weight for fuzzy matches: `0.6` (vs `1.0` for exact matches). The `HybridEstimator`
should accept a `historical_confidence_weight` parameter to scale the historical signal.

---

## Acceptance Criteria

- [ ] CLI fingerprints the query before running EXPLAIN
- [ ] If `find_similar_queries` returns ≥1 record: `historical` passed to `HybridEstimator`
- [ ] Output shows number of historical matches used
- [ ] Historical lookup failure (network error) is caught, warning printed, continues without historical
- [ ] `find_similar_queries_fuzzy()` implemented in `src/burnt/tables/queries.py`
- [ ] Fuzzy match activates only when exact match returns 0 results
- [ ] Fuzzy match output shown as `Signal: historical-fuzzy (N similar queries, confidence: medium)`
- [ ] `ast_edit_distance()` function implemented and unit-tested
- [ ] Unit tests: exact match path, fuzzy match path (≤3 edits), no-match path (>3 edits falls through)
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
