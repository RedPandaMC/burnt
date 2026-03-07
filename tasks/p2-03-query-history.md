# Task: Implement system.query.history integration

---

## Metadata

```yaml
id: p2-03-query-history
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

Create `src/dburnrate/tables/queries.py` that queries `system.query.history` to fetch historical query execution metrics. Expose `get_query_history(client, warehouse_id, days=30) -> list[QueryRecord]` and `find_similar_queries(client, sql_fingerprint, warehouse_id) -> list[QueryRecord]`. These enable historical fingerprinting — matching new queries to known past queries for accurate cost estimation.

### Files to read

```
# Required
src/dburnrate/tables/connection.py   (from p2-01)
src/dburnrate/core/models.py
src/dburnrate/core/exceptions.py
src/dburnrate/core/config.py

# Reference
RESEARCH.md   # Section: "system.query.history fingerprinting"
```

### Background

`system.query.history` columns of interest (no SELECT *):
- `account_id`, `workspace_id`, `statement_id`, `executed_by`
- `statement_text`, `statement_type`
- `start_time`, `end_time`, `execution_duration_ms`, `compilation_duration_ms`
- `read_bytes`, `read_rows`, `produced_rows`, `written_bytes`
- `total_task_duration_ms`, `result_fetch_duration_ms`
- `compute.warehouse_id`, `compute.cluster_id`
- `error_message`, `status`

Query fingerprinting approach (from RESEARCH.md):
1. Normalize SQL: strip comments, normalize whitespace, lowercase keywords
2. Replace literals with `?` (use sqlglot's `normalize` or a simple regex)
3. SHA-256 hash of normalized SQL = fingerprint
4. Look up fingerprint in `system.query.history` to find historical p50/p95 duration

`find_similar_queries` should look up by normalized statement hash using `LIKE` or an exact match on a fingerprint column if available.

---

## Acceptance Criteria

- [ ] `src/dburnrate/tables/queries.py` exists
- [ ] `QueryRecord` dataclass/model defined with fields matching schema above
- [ ] `get_query_history(client, warehouse_id, days=30) -> list[QueryRecord]` implemented
- [ ] `find_similar_queries(client, sql_fingerprint, warehouse_id, limit=10) -> list[QueryRecord]` implemented
- [ ] `normalize_sql(sql: str) -> str` helper function (strips comments, normalizes whitespace, replaces literals with `?`)
- [ ] `fingerprint_sql(sql: str) -> str` returns SHA-256 hex of normalized SQL
- [ ] No `SELECT *` in any SQL query
- [ ] Duration fields stored as `int` (milliseconds), bytes as `int`
- [ ] Unit tests in `tests/unit/tables/test_queries.py` with mocked client
- [ ] Tests cover: `normalize_sql`, `fingerprint_sql`, `get_query_history`, `find_similar_queries`
- [ ] `uv run pytest -m unit -v` passes
- [ ] `uv run ruff check src/ tests/` exits 0

---

## Verification

### Commands (run all, in order)

```bash
uv run pytest -m unit -v tests/unit/tables/test_queries.py -v
uv run pytest -m unit -v 2>&1 | tail -3
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

### Expected output

```
# All query history tests pass
# Total test count >= 122
All checks passed.
```

---

## Handoff

### Result

[Executor: fill in after completion]

### Blocked reason

[If blocked, explain here]
