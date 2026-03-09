# Task: TableRegistry Setup for Enterprise Support

---

## Metadata

```yaml
id: s4-04-table-registry
status: todo
sprint: 4
priority: normal
agent: ~
blocked_by: [s4-01-error-handling]
created_by: planner
```

---

## Context

### Goal

Implement the `TableRegistry` to allow enterprise environments to remap Databricks system tables to governance views. This ensures that the tool can be used even if users do not have direct access to the `system.*` catalog, but instead access views like `governance.cost_management.v_billing_usage`.

### Files to Read

```
DESIGN.md § "Enterprise Support: TableRegistry"
```

### Files to Create

```
src/burnt/core/table_registry.py
tests/unit/test_table_registry.py
```

### Files to Modify

```
src/burnt/tables/billing.py
src/burnt/tables/queries.py
src/burnt/tables/compute.py
src/burnt/core/config.py
```

---

## Specification

### TableRegistry Model (`core/table_registry.py`)

- Define a `TableRegistry` Pydantic model (or dataclass) mapping core system tables:
  - `billing_usage`: defaults to `"system.billing.usage"`
  - `billing_list_prices`: defaults to `"system.billing.list_prices"`
  - `query_history`: defaults to `"system.query.history"`
  - `compute_node_types`: defaults to `"system.compute.node_types"`
  - `compute_node_timeline`: defaults to `"system.compute.node_timeline"`
  - `lakeflow_jobs`: defaults to `"system.lakeflow.jobs"`
  - `lakeflow_job_run_timeline`: defaults to `"system.lakeflow.job_run_timeline"`
- Allow overriding these paths via environment variables (e.g., `burnt_TABLE_BILLING_USAGE=...`).
- Integrate the active registry into the `Backend` and table clients so all generated queries reference these dynamic table names instead of hardcoded strings.

---

## Acceptance Criteria

- [ ] `TableRegistry` model implemented with correct defaults.
- [ ] Environment variable overrides are properly loaded via `pydantic-settings` or manual fetching.
- [ ] Table query functions use the `TableRegistry` to format SQL instead of hardcoding `system.billing.usage` etc.
- [ ] Unit tests confirm that custom table paths are correctly injected into SQL queries.

---

## Verification

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

---

## Handoff

```yaml
status: todo
```