# Task: Schema Change Cost Impact Estimator

---

## Metadata

```yaml
id: s6-01-schema-change-impact
status: todo
phase: 6
priority: medium
agent: ~
blocked_by: [s4-04-table-registry, s5-06-uc-lineage-cost]
created_by: planner
```

---

## Context

### Goal

Add `burnt.schema_impact(table, change)` that estimates the cost impact of a planned
schema change — column addition, data type widening, column removal, or table rename —
by discovering downstream query consumers and modelling how the change affects their
execution cost. This turns schema governance from a correctness concern into a cost
concern too.

### Files to read

```
# Required
src/burnt/tables/lineage.py        ← estimate_lineage_impact() (s5-06)
src/burnt/tables/queries.py        ← fingerprint + history lookup
src/burnt/core/table_registry.py
src/burnt/core/models.py
src/burnt/__init__.py

# Reference
DESIGN.md
tasks/s5-06-uc-lineage-cost.md
```

### Background

**Module location:** `src/burnt/tables/schema.py`

**Schema change types and cost models:**

| Change | Detection | Cost impact model |
|--------|-----------|-------------------|
| `ADD COLUMN` (wide) | Column count × avg row size | Broader columnar scan; add 5–15% to scan cost |
| `CHANGE TYPE` (widening, e.g. INT→BIGINT) | Byte width diff | Minor scan cost increase |
| `DROP COLUMN` | — | Reduction in scan bytes; may improve existing queries |
| `RENAME COLUMN` | — | Breaks queries using old name; flag as `breaking_change=True` |
| `RENAME TABLE` | — | Breaks all downstream consumers; critical breaking change |
| `ADD NOT NULL CONSTRAINT` | — | Triggers full table scan for validation; one-time cost spike |

**`SELECT *` detection:** When a downstream consumer query uses `SELECT *` on the
changed table, the column addition/removal cost impact applies to the full column set.
Mark these queries specially: they're both cost-sensitive and fragile.

**`SchemaChange` input model:**
```python
@dataclass
class SchemaChange:
    change_type: Literal["add_column", "change_type", "drop_column", "rename_column",
                         "rename_table", "add_constraint"]
    column_name: str | None              # None for rename_table
    new_type: str | None                 # For change_type: new SQL type string
    new_name: str | None                 # For rename_column / rename_table
    estimated_table_size_gb: float | None  # Optional: if known
```

**`SchemaImpact` output model:**
```python
@dataclass
class SchemaImpact:
    table: str
    change: SchemaChange
    breaking_change: bool                # DROP/RENAME breaks downstream consumers
    affected_queries: int                # count from system.query.history
    monthly_cost_delta_usd: float        # positive = more expensive, negative = cheaper
    select_star_query_count: int         # queries using SELECT * on this table
    downstream_jobs: list[str]           # job_ids that read this table
    recommendations: list[str]
```

---

## Acceptance Criteria

- [ ] `src/burnt/tables/schema.py` exists with `schema_impact()` function
- [ ] `SchemaChange` and `SchemaImpact` models in `src/burnt/core/models.py`
- [ ] All 6 change types implemented with correct cost model
- [ ] `SELECT *` queries on the changed table detected and counted separately
- [ ] `breaking_change=True` for `rename_column`, `rename_table`, `drop_column`
- [ ] `affected_queries` count from `system.query.history` for the target table
- [ ] `monthly_cost_delta_usd` is negative for `drop_column` (cost reduction)
- [ ] `ADD NOT NULL CONSTRAINT` adds one-time validation cost spike note
- [ ] `burnt.schema_impact()` exported from `src/burnt/__init__.py`
- [ ] Unit tests cover all 6 change types; `SELECT *` detection; breaking change flag
- [ ] All existing tests still pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

### Commands

```bash
uv run pytest -m unit -v -k "schema_impact"
uv run ruff check src/ tests/
```

### Integration Check

- [ ] `burnt.schema_impact("main.analytics.events", SchemaChange(change_type="rename_table", new_name="main.analytics.events_v2"))` returns `SchemaImpact` with `breaking_change=True` and a non-empty `downstream_jobs` list.

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked on s4-04 (table registry) and s5-06 (lineage lookup reused here).
