# Task: Implement Level 1 AST-Based Lint Rules

---

## Metadata

```yaml
id: s2-04-ast-lint-rules
status: in-progress
phase: 2
priority: high
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Implement comprehensive AST-based lint rules for PySpark, SQL, and Spark Declarative Pipelines (SDP). These rules catch frequent performance killers without requiring a live Spark session.

### Files to read

```
# Required
src/burnt/parsers/antipatterns.py    # Current implementation (string-based PySpark, sqlglot SQL)
src/burnt/parsers/pyspark.py         # Python AST parsing (PySparkVisitor — use this!)
src/burnt/parsers/sql.py             # SQL parsing (sqlglot)
tests/unit/parsers/test_antipatterns.py

# Reference
tasks/research/lint_research.md
DESIGN.md
```

### Background

**Current state (as of sprint 2):**

`_detect_pyspark_antipatterns()` in `antipatterns.py` uses **string matching** (e.g. `".collect()" in source`), not the `PySparkVisitor` AST from `parsers/pyspark.py`. It needs to be rewritten to use the AST visitor for correctness and to enable the remaining rules.

`_detect_sql_antipatterns()` correctly uses sqlglot AST via `detect_operations()`.

---

## Rules Status

| Rule | Severity | Status | Notes |
|------|----------|--------|-------|
| `cross_join` | WARNING | ✅ done | SQL, sqlglot AST |
| `select_star` | ERROR | ⚠️ done (severity wrong) | Implemented as `select_star_no_limit` at INFO, spec says ERROR |
| `order_by_no_limit` | WARNING | ✅ done | Not in original spec, good addition |
| `collect_without_limit` | ERROR | ⚠️ done (string-based) | Works but uses `in source` not AST |
| `python_udf` | ERROR | ⚠️ done (severity wrong) | Implemented at WARNING, spec says ERROR; string-based (`@udf in source`) |
| `toPandas` | ERROR | ⚠️ done (severity wrong) | Implemented at WARNING, spec says ERROR; string-based |
| `repartition_one` | WARNING | ⚠️ done (string-based) | Correct severity; string-based |
| `pandas_udf` | WARNING | ❌ not done | Warn when `@pandas_udf` used instead of native Spark functions |
| `count_without_filter` | WARNING | ❌ not done | `.count()` with no preceding `.filter()` or `.where()` |
| `withColumn_in_loop` | WARNING | ❌ not done | `.withColumn()` inside a for/while loop → use `.withColumns()` |
| `jdbc_incomplete_partition` | ERROR | ❌ not done | JDBC read missing partitionColumn/numPartitions/lowerBound/upperBound |
| `sdp_prohibited_ops` | ERROR | ❌ not done | `collect()`, `count()`, `toPandas()` inside `@dp.table` / `@dp.materialized_view` |

---

## Remaining Work

### 1. Fix severity mismatches (small but important)

- `select_star` → change from INFO to ERROR and rename pattern name from `select_star_no_limit` to `select_star` (check test impact)
- `python_udf` → change from WARNING to ERROR
- `toPandas` → change from WARNING to ERROR

### 2. Migrate PySpark detection to use `PySparkVisitor`

Rewrite `_detect_pyspark_antipatterns()` to parse with `ast.parse()` via `PySparkVisitor` instead of string matching. The `PySparkVisitor` in `parsers/pyspark.py` already tracks method calls and decorators — extend it to produce `AntiPattern` objects.

This unblocks `count_without_filter` and `withColumn_in_loop` (which require understanding code structure, not just substring presence).

### 3. Add missing rules

**`pandas_udf` (WARNING):**
```python
# Detects: @pandas_udf decorated functions — suggest native Spark alternatives
# False positive guard: if used alongside UDF replacement, don't flag both
```

**`count_without_filter` (WARNING):**
```python
# Detects: .count() call without a preceding .filter() or .where() in the chain
# e.g. df.count()  ← flag
# e.g. df.filter(...).count()  ← ok
```

**`withColumn_in_loop` (WARNING):**
```python
# Detects: .withColumn() call inside a for or while loop body
# Requires AST loop detection: visit For/While nodes, check for withColumn in body
```

**`jdbc_incomplete_partition` (ERROR):**
```python
# Detects: spark.read.format("jdbc").load() or spark.read.jdbc(...)
# without ALL of: partitionColumn, numPartitions, lowerBound, upperBound
# String scan for format("jdbc") + option() calls, check all 4 keys present
```

**`sdp_prohibited_ops` (ERROR):**
```python
# Detects: collect(), count(), toPandas() inside functions decorated with
# @dp.table or @dp.materialized_view (Delta Live Tables / DLT pipeline functions)
# Requires: detect @dp.table/@dp.materialized_view decorators, then check body
```

---

## Acceptance Criteria

- [x] `cross_join` detected (SQL)
- [x] `select_star_no_limit` detected (SQL)  ← rename to `select_star` + change severity to ERROR
- [x] `order_by_no_limit` detected (SQL)
- [x] `collect_without_limit` detected (PySpark)  ← migrate to AST
- [x] `python_udf` detected (PySpark)  ← change severity to ERROR + migrate to AST
- [x] `toPandas` detected (PySpark)  ← change severity to ERROR + migrate to AST
- [x] `repartition_one` detected (PySpark)  ← migrate to AST
- [ ] `pandas_udf` implemented (WARNING)
- [ ] `count_without_filter` implemented (WARNING)
- [ ] `withColumn_in_loop` implemented (WARNING)
- [ ] `jdbc_incomplete_partition` implemented (ERROR)
- [ ] `sdp_prohibited_ops` implemented (ERROR)
- [ ] All PySpark rules use `PySparkVisitor` AST, not string matching
- [ ] Severity values match the table above
- [ ] No false positives on valid code patterns
- [ ] All existing tests pass
- [ ] New unit tests for each new rule
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

### Commands

```bash
uv run pytest tests/unit/parsers/test_antipatterns.py -v
uv run ruff check src/burnt/parsers/ tests/unit/parsers/
```

### Integration Check

- [ ] Run `burnt lint` on sample PySpark/SQL files and confirm all rules fire correctly

---

## Handoff

### Result

```yaml
status: in-progress
```

### Blocked reason

Not blocked. 7 of 12 rules exist but 5 are string-based and 3 have wrong severity. Pick up from "Remaining Work" above.
