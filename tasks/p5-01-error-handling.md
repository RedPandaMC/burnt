# Task: Implement comprehensive error handling

---

## Metadata

```yaml
id: p5-01-error-handling
status: todo
phase: 5
priority: high
agent: ~
blocked_by: [p4-01-wire-explain-into-cli, p4-03-fingerprint-lookup, p5-00-research-production-hardening]
created_by: planner
```

---

## Context

### Goal

Expand the exception hierarchy, add user-friendly error messages with recovery suggestions, and ensure all `tables/`, `estimators/`, and `cli/` code surfaces clean errors instead of raw tracebacks. Implement token redaction for security.

### Files to read

```
src/dburnrate/core/exceptions.py
src/dburnrate/tables/connection.py
src/dburnrate/tables/billing.py
src/dburnrate/tables/queries.py
src/dburnrate/tables/compute.py
src/dburnrate/estimators/hybrid.py
src/dburnrate/cli/main.py
docs/production-hardening-research.md   (from p5-00)
```

### Background

Current exception hierarchy in `exceptions.py`:
```python
class DburnrateError(Exception): ...
class ParseError(DburnrateError): ...
class ConfigError(DburnrateError): ...
class PricingError(DburnrateError): ...
class EstimationError(DburnrateError): ...
```

Extend with:
```python
class ConnectionError(DburnrateError): ...      # Databricks connectivity
class AuthenticationError(ConnectionError): ... # 401 — bad token
class RateLimitError(ConnectionError): ...      # 429 — backoff needed
class WarehouseError(ConnectionError): ...      # warehouse stopped/not found
class TableNotFoundError(EstimationError): ...  # table missing from catalog
class TimeoutError(ConnectionError): ...        # request timeout
```

Each exception must have:
- `message` — user-facing, actionable (e.g. "Token rejected (401). Check DBURNRATE_TOKEN.")
- `suggestion` — recovery step (e.g. "Run: export DBURNRATE_TOKEN=dapi...")
- Token values must be redacted from all error messages and tracebacks

CLI `main.py` must catch `DburnrateError` subclasses at the top level and print rich-formatted messages (red for error, yellow for suggestion) without full tracebacks in normal mode.

---

## Acceptance Criteria

- [ ] Exception hierarchy extended with `ConnectionError`, `AuthenticationError`, `RateLimitError`, `WarehouseError`, `TableNotFoundError`, `TimeoutError`
- [ ] Each exception has `message` and `suggestion` attributes
- [ ] `DatabricksClient` raises typed exceptions (not raw `requests.HTTPError`)
- [ ] 401 → `AuthenticationError` with redacted token hint
- [ ] 429 → `RateLimitError` with retry-after guidance
- [ ] 503/timeout → `WarehouseError` or `TimeoutError`
- [ ] CLI catches all `DburnrateError` at top level, prints clean message (no traceback unless `--debug`)
- [ ] Token strings never appear in error messages or exception `__str__`
- [ ] New unit tests in `tests/unit/core/test_exceptions.py` and `tests/unit/tables/test_connection_errors.py`
- [ ] All existing tests still pass
- [ ] `uv run ruff check src/ tests/` exits 0

---

## Verification

```bash
uv run pytest -m unit -v tests/unit/core/
uv run pytest -m unit -v tests/unit/tables/
uv run pytest -m unit -v 2>&1 | tail -5
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

---

## Handoff

### Result

[Executor: fill in after completion]

### Blocked reason

[If blocked, explain here]
