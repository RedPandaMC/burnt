# Task: Implement Databricks REST API connection client

---

## Metadata

```yaml
id: p2-01-databricks-connection
status: todo
phase: 2
priority: high
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Create `src/dburnrate/tables/connection.py` — a thin Databricks REST API client that handles workspace URL + PAT token authentication, connection pooling via `requests.Session`, retry logic (3 retries with exponential backoff on 429/5xx), and a simple rate limiter. This client will be used by all Phase 2 system-table modules (billing, queries, compute).

### Files to read

```
# Required
src/dburnrate/core/config.py
src/dburnrate/core/exceptions.py

# Reference
RESEARCH.md   # Section on authentication and API calls
pyproject.toml  # for dependency management
```

### Background

Databricks REST API uses Bearer token auth: `Authorization: Bearer <token>`. The workspace URL comes from `Settings` (already in `config.py`). All system table queries go through the SQL Statement Execution API: `POST /api/2.0/sql/statements`. Responses are async — poll `GET /api/2.0/sql/statements/{statement_id}` until `status.state` is `SUCCEEDED` or `FAILED`.

Key API details:
- Endpoint: `POST {workspace_url}/api/2.0/sql/statements`
- Body: `{"statement": "<SQL>", "warehouse_id": "<id>", "wait_timeout": "30s"}`
- Auth header: `Authorization: Bearer {token}`
- On 429 responses: respect `Retry-After` header
- Max retries: 3 with exponential backoff (1s, 2s, 4s)

Do NOT hardcode any URLs or tokens. Read them from `Settings`.

---

## Acceptance Criteria

- [ ] `src/dburnrate/tables/__init__.py` exists (can be empty)
- [ ] `src/dburnrate/tables/connection.py` exists with `DatabricksClient` class
- [ ] `DatabricksClient.__init__(self, settings: Settings)` — no hardcoded credentials
- [ ] `DatabricksClient.execute_sql(self, sql: str, warehouse_id: str) -> list[dict]` — returns rows as list of dicts
- [ ] Retry logic: 3 retries on 429/500/502/503/504 with exponential backoff
- [ ] All public methods have type hints and docstrings
- [ ] `src/dburnrate/core/exceptions.py` has `DatabricksConnectionError` and `DatabricksQueryError`
- [ ] Tests in `tests/unit/tables/test_connection.py` cover: success path, retry on 429, auth error, query failure
- [ ] `uv run pytest -m unit -v` passes (122+ tests)
- [ ] `uv run ruff check src/ tests/` exits 0

---

## Verification

### Commands (run all, in order)

```bash
uv run pytest -m unit -v tests/unit/tables/test_connection.py -v
uv run pytest -m unit -v 2>&1 | tail -3
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
uv run bandit -c pyproject.toml -r src/
```

### Expected output

```
# First command — all connection tests pass
PASSED tests/unit/tables/test_connection.py::...

# Second command
N passed in ...  (N >= 122)

# Third/fourth
All checks passed.
```

---

## Handoff

### Result

[Executor: fill in after completion]

### Blocked reason

[If blocked, explain here]
