# Task: Research EXPLAIN COST output format and parsing strategy

---

## Metadata

```yaml
id: p3-00-research-explain-cost
status: done
phase: 3
priority: high
agent: claude-sonnet-4-6
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Research the exact output format of Databricks `EXPLAIN COST` and `EXPLAIN EXTENDED` SQL commands. Document the JSON/text schema, identify which fields map to cost signals, and write a parsing specification that `p3-01-explain-parser` can implement without ambiguity. Output goes into `docs/explain-cost-schema.md`.

### Files to read

```
# Required
RESEARCH.md              # Existing research — check what's already documented
src/dburnrate/parsers/sql.py   # Current SQL parser — understand existing structure
src/dburnrate/core/models.py   # OperationInfo, QueryProfile — fields to populate

# Reference
CONCEPT.md
```

### Background

Databricks `EXPLAIN COST` returns a logical plan with per-node cost estimates. The output is a text blob (not JSON) containing lines like:

```
== Optimized Logical Plan ==
Aggregate [sum(price#10) AS total_price#12]    Statistics(sizeInBytes=8.0 B)
+- Filter (isnotnull(c_custkey#3) AND ...)     Statistics(sizeInBytes=13.5 MiB, rowCount=191.9K)
   +- Relation [c_custkey#3,...] parquet       Statistics(sizeInBytes=23.5 MiB, rowCount=1.5M)
```

Key signals to extract:
- `sizeInBytes` — estimated scan/shuffle size
- `rowCount` — estimated row count at each operator
- Join types: `BroadcastHashJoin`, `SortMergeJoin`, `ShuffledHashJoin`
- Shuffle ops: `Exchange`, `Sort`
- Operator depth (plan height = parallelism proxy)

`EXPLAIN EXTENDED` adds physical plan details (actual join strategy chosen).

---

## Acceptance Criteria

- [x] `docs/explain-cost-schema.md` created with:
  - [x] Example EXPLAIN COST output (at least 3 different query types: simple scan, join, aggregation)
  - [x] Regex/parse patterns for `Statistics(sizeInBytes=..., rowCount=...)`
  - [x] List of join operator strings to detect
  - [x] List of shuffle operator strings to detect
  - [x] Mapping: parsed fields → `OperationInfo` fields (`name`, `kind`, `weight`)
  - [x] Confidence scoring strategy: when stats are complete vs missing
  - [x] Edge cases: no stats available, CTE, subqueries
- [x] Document is precise enough that an executor can implement the parser without reading Databricks docs
- [x] No code written — this is a research/documentation task only

---

## Verification

### Commands

```bash
# Verify the doc exists and is non-trivial
wc -l docs/explain-cost-schema.md   # should be > 50 lines
```

### Expected output

`docs/explain-cost-schema.md` with complete parsing spec.

---

## Handoff

### Result

Created `docs/explain-cost-schema.md` (583 lines). The document covers:

1. **Five synthetic EXPLAIN COST examples** — simple aggregation scan, BroadcastHashJoin, SortMergeJoin with three shuffles, CTE+subquery, and no-statistics (ANALYZE not run). Each example explains the structural patterns visible in the output.

2. **Exact regex patterns** — HEADER_PATTERN, STATS_PATTERN (named groups: size_val, size_unit, row_val, row_unit), JOIN_PATTERN, SHUFFLE_LINE_PATTERN, and DEPTH_PATTERN. All patterns are ready to `re.compile()` directly.

3. **Unit normalization tables** — SIZE_MULTIPLIERS dict (B/KiB/MiB/GiB/TiB → bytes) and ROW_MULTIPLIERS dict (empty/K/M/B → integer). Includes disambiguation note: `B` in rowCount context means billions, not bytes.

4. **OperationInfo field mapping** — All 11 operator types mapped to kind + weight values, consistent with the weight scales already used in `sql.py`. Filter/Project/Window are documented as skip operators.

5. **Confidence scoring rules** — Three-tier: high (all rowCount present), medium (sizeInBytes > 8 bytes but incomplete), low (placeholder-only stats). Includes the 8-byte sentinel detection logic.

6. **Seven edge cases** — empty input, missing header, CTE nodes (WithCTE/CTERelationDef/CTERelationRef), subquery inlining, large rowCounts (>1B), EXPLAIN without COST, and multi-section EXPLAIN output (extract only the Optimized Logical Plan section).

7. **ExplainPlan Pydantic model spec** — Full field list with types and docstrings, plus the `parse_explain_cost(text: str) -> ExplainPlan` function signature and mapping to downstream `QueryProfile`/`CostEstimate` fields.

Key surprises found:
- The `B` suffix in `rowCount` means billions (not bytes), creating a lexical ambiguity with the `B` byte unit in `sizeInBytes`. The regex captures them in separate named groups to avoid confusion.
- Spark emits `Statistics(sizeInBytes=8.0 B)` as a placeholder when table metadata is missing — this is NOT a real file size estimate.
- CTE materialisation produces three distinct node types (`WithCTE`, `CTERelationDef`, `CTERelationRef`) that must all be handled.
- Multi-section EXPLAIN output requires isolating the Optimized Logical Plan section to avoid double-counting operators from the Physical Plan section.

### Blocked reason

N/A — completed successfully.
