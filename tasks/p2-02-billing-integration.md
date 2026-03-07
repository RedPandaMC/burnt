# Task: Implement system.billing system table queries

---

## Metadata

```yaml
id: p2-02-billing-integration
status: todo
phase: 2
priority: high
agent: ~
blocked_by: [p2-01-databricks-connection]
created_by: planner
```

---

## Context

### Goal

Create `src/dburnrate/tables/billing.py` that queries `system.billing.usage` and `system.billing.list_prices` to fetch historical DBU consumption and live pricing. Expose two public functions: `get_historical_usage()` and `get_live_prices()`. These will feed into the estimator to ground-truth the static complexity model.

### Files to read

```
# Required
src/dburnrate/tables/connection.py   (from p2-01)
src/dburnrate/tables/__init__.py
src/dburnrate/core/models.py
src/dburnrate/core/exceptions.py
src/dburnrate/core/config.py

# Reference
RESEARCH.md   # Section: "system.billing.usage schema"
```

### Background

`system.billing.usage` columns of interest (select ONLY these — no SELECT *):
- `account_id`, `workspace_id`, `sku_name`, `cloud`, `usage_start_time`, `usage_end_time`
- `usage_quantity` (DBUs), `usage_unit`, `usage_metadata.cluster_id`, `usage_metadata.warehouse_id`

`system.billing.list_prices` columns of interest:
- `sku_name`, `cloud`, `currency_code`, `pricing.default` (USD per DBU), `price_start_time`, `price_end_time`

Cost attribution formula (from RESEARCH.md):
```
query_cost = (query_duration_ms / cluster_total_uptime_ms) * cluster_dbu_cost
```

Return types should use `Decimal` for all monetary values, never `float`.

---

## Acceptance Criteria

- [ ] `src/dburnrate/tables/billing.py` exists
- [ ] `get_historical_usage(client, warehouse_id, days=30) -> list[UsageRecord]` implemented
- [ ] `get_live_prices(client, sku_names) -> dict[str, Decimal]` implemented
- [ ] `UsageRecord` dataclass or Pydantic model defined in `models.py` or locally
- [ ] No `SELECT *` in any SQL query — explicit column list only
- [ ] All monetary values use `Decimal`, not `float`
- [ ] Unit tests in `tests/unit/tables/test_billing.py` with mocked `DatabricksClient`
- [ ] Tests cover: normal response, empty result, API error propagation
- [ ] `uv run pytest -m unit -v` passes
- [ ] `uv run ruff check src/ tests/` exits 0

---

## Verification

### Commands (run all, in order)

```bash
uv run pytest -m unit -v tests/unit/tables/test_billing.py -v
uv run pytest -m unit -v 2>&1 | tail -3
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

### Expected output

```
# All billing tests pass
# Total test count >= 122
All checks passed.
```

---

## Handoff

### Result

[Executor: fill in after completion]

### Blocked reason

[If blocked, explain here]
