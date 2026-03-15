# Task: Partition Pruning Estimation

---

## Metadata

```yaml
id: s3-04-partition-pruning
status: todo
phase: 3
priority: critical
agent: ~
blocked_by: [s3-01-delta-scan-integration]
created_by: planner
```

---

## Context

### Goal

For date-partitioned tables with date-range filters — the most common analytics pattern — the static estimator currently assumes 100% of the table is scanned. A `SELECT ... WHERE event_date >= '2024-01-01' AND event_date < '2024-02-01'` on a 2-year table scans roughly 4% of data, not 100%. Implementing partition pruning estimation can improve accuracy by 10–100× for this class of queries, making it the single highest-impact accuracy feature on the roadmap.

### Files to read

```
# Required
src/burnt/estimators/hybrid.py
src/burnt/estimators/pipeline.py
src/burnt/parsers/sql.py
src/burnt/parsers/delta.py
src/burnt/core/models.py          ← DeltaTableInfo

# Reference
DESIGN.md
tasks/s3-01-delta-scan-integration.md
```

### Background

When `DeltaTableInfo` includes `partition_columns`, and the SQL `WHERE` clause filters on
those columns, the estimated scan bytes can be multiplied by the partition selectivity fraction
instead of using the full table size.

**Selectivity estimation model:**

```python
def estimate_partition_selectivity(
    table_info: DeltaTableInfo,
    predicates: list[Expression],  # sqlglot WHERE clause nodes
) -> float:
    """Returns fraction of table scanned (0.0–1.0) after partition pruning."""
    partition_cols = set(table_info.partition_columns)
    relevant = [p for p in predicates if references_any_column(p, partition_cols)]
    if not relevant:
        return 1.0  # full scan

    # Equality predicate: col = value → 1 / num_distinct_values
    # Range predicate (date >= X AND date < Y): fraction of range covered
    # IN-list: len(values) / num_distinct_values
    # Multiple predicates on same column: multiplicative (assume independence)
```

**Data sources:**
- `DESCRIBE DETAIL <table>` → `partitionColumns`, `numFiles`, `sizeInBytes`
- `SHOW PARTITIONS <table>` → actual distinct partition values (enables range fraction computation)

**cheapest signal path:** `DESCRIBE DETAIL` returns `partitionColumns`. `SHOW PARTITIONS`
returns key values. Combined, you can compute selectivity without log scanning.

**Implementation location:** New function `estimate_partition_selectivity()` in
`src/burnt/estimators/partition.py`. Called from `HybridEstimator` when `DeltaTableInfo`
has non-empty `partition_columns` and the SQL has WHERE clause predicates.

**Integration with scan size:**
```python
selectivity = estimate_partition_selectivity(table_info, where_predicates)
effective_scan_bytes = table_info.size_in_bytes * selectivity
# Use effective_scan_bytes instead of full size_in_bytes in the estimator
```

---

## Acceptance Criteria

- [ ] `src/burnt/estimators/partition.py` exists with `estimate_partition_selectivity(table_info, predicates) -> float`
- [ ] Equality predicate (`col = 'value'`) on partition column returns `1 / num_distinct_partitions`
- [ ] Date range predicate (`date >= X AND date < Y`) returns fraction of total date range covered
- [ ] IN-list predicate (`col IN (a, b, c)`) returns `len(values) / num_distinct_partitions`
- [ ] Non-partitioned table (empty `partition_columns`) returns `1.0`
- [ ] No WHERE clause predicates returns `1.0` (full scan)
- [ ] `HybridEstimator` calls `estimate_partition_selectivity()` when `DeltaTableInfo` has partition columns and SQL has WHERE clause
- [ ] Selectivity multiplies `scan_bytes` before cost calculation
- [ ] Unit tests cover all predicate types above; include a 2-year daily-partitioned table with 1-week filter (expected selectivity ≈ 0.014)
- [ ] All existing tests still pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

### Commands

```bash
uv run pytest -m unit -v -k "partition_pruning or partition_selectivity"
uv run ruff check src/ tests/
```

### Integration Check

- [ ] Estimate a query `SELECT * FROM events WHERE event_date >= '2024-01-01' AND event_date < '2024-08-01'` against a mock `DeltaTableInfo` with `partition_columns=["event_date"]` and 365 distinct date partitions. Assert `selectivity ≈ 0.5` (7 of 12 months, or 212/365 days depending on implementation).

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked on s3-01 (delta scan integration — `DeltaTableInfo` must be populated before selectivity can be applied).
