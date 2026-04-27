```yaml
id: P2-08-explain-enrichment
status: todo
phase: 2
priority: medium
agent: ~
blocked_by: [PX-02-sparkmeasure-session]
created_by: planner
```

## Context

### Goal

Verify that `parsers/explain.py` works with standard Spark `EXPLAIN EXTENDED` output (not Databricks-specific `EXPLAIN COST`), and wire it into `_check.run()` as a fallback enrichment source when no sparkMeasure session data is available.

The Rust engine builds a `CostGraph` with `estimated_input_bytes = None` on most nodes. When runtime data is unavailable, EXPLAIN output is the best static approximation for table scan sizes. This task makes that path real and tested.

### Files to read

```
# Required
src/burnt/parsers/explain.py
src/burnt/_check/__init__.py
src/burnt/graph/model.py

# Reference
DESIGN.md §5 (Session Lifecycle)
```

### Background

Spark `EXPLAIN EXTENDED` emits a text plan with scan sizes, join types, and shuffle indicators. Example fragment:

```
== Physical Plan ==
*(2) HashAggregate(keys=[region#10], ...)
+- Exchange hashpartitioning(region#10, 200)
   +- *(1) FileScan parquet orders[...]
           EstimatedSize: 4294967296
```

`EstimatedSize` → `CostNode.estimated_input_bytes`. Join type (HashJoin vs BroadcastHashJoin vs SortMergeJoin) → `shuffle_required`. Exchange node → `shuffle_required = True`.

---

## Acceptance Criteria

- [ ] `explain.py` parses standard `EXPLAIN EXTENDED` output and extracts: scan node sizes (`EstimatedSize`), join types, Exchange nodes (shuffle indicators)
- [ ] Returns structured data with: `kind` (scan/join/exchange), `estimated_bytes: int | None`, `shuffle_required: bool`
- [ ] `_check.run()`: when session has no sparkMeasure data, calls explain enrichment to populate `estimated_input_bytes` on matching graph nodes
- [ ] Graceful on non-Spark EXPLAIN formats (Databricks EXPLAIN COST, empty string, malformed) — returns empty list, no exception
- [ ] Unit tests in `tests/unit/parsers/test_explain_enrichment.py` with 3 fixture strings:
  - A HashAggregate + FileScan plan (expects 1 scan node with EstimatedSize parsed correctly)
  - A SortMergeJoin plan (expects `shuffle_required=True`)
  - An empty string (expects empty result, no exception)

## Verification

```bash
uv run pytest tests/unit/parsers/test_explain_enrichment.py -v
uv run ruff check src/burnt/parsers/explain.py
```

### Integration Check

- [ ] `burnt check tests/fixtures/e2e/cross_join.py` completes without traceback whether or not EXPLAIN data is present
