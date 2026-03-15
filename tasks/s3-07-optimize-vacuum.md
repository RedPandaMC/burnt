# Task: OPTIMIZE and VACUUM Cost Estimation

---

## Metadata

```yaml
id: s3-07-optimize-vacuum
status: todo
phase: 3
priority: medium
agent: ~
blocked_by: [s3-01-delta-scan-integration]
created_by: planner
```

---

## Context

### Goal

When users write `OPTIMIZE <table>` or `VACUUM <table>` in their SQL, the current parser
treats them as unknown commands with zero estimated cost. These operations can be
significant: OPTIMIZE rewrites all files in a table (potentially expensive), while VACUUM
is compute-cheap but surfaces valuable storage savings. Add detection and estimation for
both operations, and add a proactive fragmentation warning when a table's file count
greatly exceeds the ideal.

### Files to read

```
# Required
src/burnt/parsers/sql.py
src/burnt/estimators/static.py
src/burnt/estimators/hybrid.py
src/burnt/parsers/delta.py        ← DeltaTableInfo (sizeInBytes, numFiles)
src/burnt/core/models.py          ← CostEstimate, warnings

# Reference
DESIGN.md
tasks/s3-01-delta-scan-integration.md
tasks/s3-04-partition-pruning.md   ← reuse partition logic for WHERE clause on OPTIMIZE
```

### Background

**OPTIMIZE cost model:**

```
rewrite_bytes = total_size_bytes                    # full table OPTIMIZE
             OR total_size_bytes × selectivity      # WITH WHERE clause (reuse partition pruning)
             × 1.2                                  # ZORDER BY: ~20% overhead

optimize_duration_s = rewrite_bytes / write_throughput_bps
                    # write_throughput_bps ≈ 200 MB/s (Delta write, SSD)

optimize_cost_usd = (optimize_duration_s / 3600)
                    × cluster_dbu_per_hour
                    × dbu_rate
                    + vm_cost_for_duration
```

**VACUUM cost model:**

VACUUM is metadata-only (file listing + deletion). Compute cost is negligible. Instead,
estimate **storage savings**:

```
orphaned_bytes = (numFiles - ideal_file_count) × avg_file_size
              where avg_file_size = sizeInBytes / numFiles
              and   ideal_file_count = sizeInBytes / (128 × 1e6)  # 128 MB target file size
storage_savings_usd_per_month = orphaned_bytes / 1e12 × cloud_storage_price_per_tb
                                 # Azure Blob hot: $0.018/GB/month ≈ $18/TB/month
```

**Fragmentation detection (proactive):**

After any table is resolved via `DESCRIBE DETAIL`, check:
```
fragmentation_ratio = numFiles / ideal_file_count
```
If `fragmentation_ratio > 3.0`, append a fragmentation warning to `CostEstimate.warnings`:
```
Table `events` has 12,000 files (ideal: 4,000).
OPTIMIZE would improve read performance ~3× and cost ~$2.40 to run.
```

**SQL parser integration:**

sqlglot parses `OPTIMIZE` and `VACUUM` as `Command` nodes. Match on command text and
route to the appropriate estimator. The table name is in the args of the Command node.

---

## Acceptance Criteria

- [ ] `SQL OPTIMIZE <table>` queries are detected by the SQL parser and routed to the OPTIMIZE estimator
- [ ] `SQL VACUUM <table>` queries are detected and routed to VACUUM estimator
- [ ] OPTIMIZE estimate includes compute cost (DBU + VM) based on `sizeInBytes` from `DESCRIBE DETAIL`
- [ ] OPTIMIZE with `ZORDER BY` applies 1.2× overhead multiplier
- [ ] OPTIMIZE with `WHERE` clause applies partition selectivity from `estimate_partition_selectivity()`
- [ ] VACUUM result reports `estimated_storage_savings_usd_per_month` instead of compute cost
- [ ] Fragmentation warning added to `CostEstimate.warnings` when `numFiles / ideal_file_count > 3.0` (for any resolved table, not just explicit OPTIMIZE)
- [ ] Fragmentation warning includes estimated OPTIMIZE cost and read performance improvement
- [ ] Unit tests cover: full OPTIMIZE, OPTIMIZE with WHERE, OPTIMIZE ZORDER BY, VACUUM savings, fragmentation detection
- [ ] All existing tests still pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

### Commands

```bash
uv run pytest -m unit -v -k "optimize or vacuum or fragmentation"
uv run ruff check src/ tests/
```

### Integration Check

- [ ] `burnt.estimate("OPTIMIZE events WHERE date >= '2024-01-01'")` — returns cost estimate (not error), uses only ~27% of table size due to monthly partition filter.
- [ ] `burnt.estimate("VACUUM events")` — returns result with `storage_savings_usd_per_month` populated (not compute cost).

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked on s3-01 (DESCRIBE DETAIL must be wired into the estimator pipeline to provide `sizeInBytes` and `numFiles`).
