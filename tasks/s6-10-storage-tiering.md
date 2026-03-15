# Task: Storage Cost Attribution and Tiering Recommendations

---

## Metadata

```yaml
id: s6-10-storage-tiering
status: todo
phase: 6
priority: medium
agent: ~
blocked_by: [s4-04-table-registry, s3-01-delta-scan-integration]
created_by: planner
```

---

## Context

### Goal

Add `burnt.storage_analysis(catalog, schemas)` that produces a per-table storage cost
breakdown and identifies cold tables that could be moved to cheaper storage tiers (cool or
archive). Uses `INFORMATION_SCHEMA.TABLES` for table sizes, `system.query.history` for
last-access timestamps, and Delta `DESCRIBE DETAIL` for file-level metadata.

### Files to read

```
# Required
src/burnt/parsers/delta.py         ← DESCRIBE DETAIL parsing
src/burnt/tables/queries.py        ← last access from system.query.history
src/burnt/core/table_registry.py
src/burnt/core/models.py
src/burnt/__init__.py

# Reference
DESIGN.md
tasks/s3-01-delta-scan-integration.md
tasks/s4-04-table-registry.md
```

### Background

**Module location:** `src/burnt/tables/storage.py`

**Data sources:**

| Source | Provides |
|--------|---------|
| `{catalog}.INFORMATION_SCHEMA.TABLES` | `table_name`, `table_schema`, `data_length`, `table_type` |
| `system.query.history` | Last query time per table (`table_catalog`, `table_schema`, `table_name`) |
| `DESCRIBE DETAIL <table>` | `numFiles`, `sizeInBytes`, `partitionColumns` (more accurate than INFORMATION_SCHEMA) |

**Storage pricing (Azure Blob):**

| Tier | Price per GB/month |
|------|-------------------|
| Hot | $0.018 |
| Cool | $0.01 |
| Archive | $0.002 |

Use `DESCRIBE DETAIL` when available (connected mode); fall back to `INFORMATION_SCHEMA.TABLES.data_length` in offline mode.

**Cold table classification:**

```
days_since_last_access = (today - last_query_timestamp).days
if days_since_last_access > 90:   → "cold" (recommend cool or archive tier)
if days_since_last_access > 30:   → "warm" (monitor)
if days_since_last_access <= 30:  → "hot" (keep as-is)
```

**`TableStorageRecord` model:**
```python
@dataclass
class TableStorageRecord:
    full_name: str                   # catalog.schema.table
    size_gb: float
    num_files: int | None            # from DESCRIBE DETAIL if available
    last_accessed: datetime | None   # None if never queried in history window
    days_since_access: int | None
    temperature: str                 # "hot" | "warm" | "cold" | "unknown"
    monthly_cost_usd: float          # at hot tier pricing
    potential_savings_cool_usd: float    # savings if moved to cool tier
    potential_savings_archive_usd: float # savings if moved to archive tier
    partition_columns: list[str]

@dataclass
class StorageAnalysisReport:
    catalog: str
    schemas: list[str]
    total_size_gb: float
    total_monthly_cost_usd: float
    tables: list[TableStorageRecord]
    cold_tables: list[TableStorageRecord]       # days_since_access > 90
    total_potential_savings_usd: float           # if all cold moved to cool
    recommendations: list[str]
```

**`INFORMATION_SCHEMA` query:**
```sql
SELECT table_catalog, table_schema, table_name, data_length
FROM {catalog}.INFORMATION_SCHEMA.TABLES
WHERE table_schema IN ({schemas})
  AND table_type IN ('MANAGED', 'EXTERNAL')
```

---

## Acceptance Criteria

- [ ] `src/burnt/tables/storage.py` exists with `storage_analysis()` function
- [ ] `TableStorageRecord` and `StorageAnalysisReport` models in `src/burnt/core/models.py`
- [ ] `INFORMATION_SCHEMA.TABLES` queried for table sizes within specified catalog/schemas
- [ ] `DESCRIBE DETAIL` used when available (connected mode) to get accurate `sizeInBytes`
- [ ] `last_accessed` populated from `system.query.history` (None if no history in lookback window)
- [ ] Temperature classification: hot (≤30d), warm (31–90d), cold (>90d), unknown (no history)
- [ ] `potential_savings_cool_usd` and `potential_savings_archive_usd` use Azure Blob pricing
- [ ] `cold_tables` list sorted by `size_gb` descending (biggest savings first)
- [ ] `TableRegistry` used for system table paths
- [ ] `burnt.storage_analysis()` exported from `src/burnt/__init__.py`
- [ ] `StorageAnalysisReport.display()` shows per-table breakdown and cold table summary
- [ ] Unit tests: mixed hot/cold tables, no history (all unknown), single schema, savings calculation
- [ ] All existing tests still pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

### Commands

```bash
uv run pytest -m unit -v -k "storage_analysis"
uv run ruff check src/ tests/
```

### Integration Check

- [ ] With mock tables (3 tables: 1 accessed today, 1 accessed 45 days ago, 1 never accessed): `storage_analysis("main", ["analytics"])` returns 1 cold, 1 warm, 1 unknown; `cold_tables` has 1 entry.

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked on s4-04 (table registry) and s3-01 (DESCRIBE DETAIL wired into pipeline for accurate file sizes).
