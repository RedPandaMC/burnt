# Task: Implement comprehensive error handling

---

## Metadata

```yaml
id: s4-01-error-handling
status: todo
sprint: 4
priority: high
agent: ~
blocked_by: [s3-03-pipeline-hardening]
created_by: planner
```

---

## Context

### Goal

Expand the exception hierarchy, add user-friendly error messages with recovery suggestions, and ensure all `tables/`, `estimators/`, and `cli/` code surfaces clean errors instead of raw tracebacks. Implement token redaction for security.

### Files to read

```
src/burnt/core/exceptions.py
src/burnt/tables/connection.py
src/burnt/tables/billing.py
src/burnt/tables/queries.py
src/burnt/tables/compute.py
src/burnt/estimators/hybrid.py
src/burnt/cli/main.py
docs/production-hardening-research.md   (from p5-00)
```

### Background

Current exception hierarchy in `exceptions.py`:
```python
class BurntError(Exception): ...
class ParseError(BurntError): ...
class ConfigError(BurntError): ...
class PricingError(BurntError): ...
class EstimationError(BurntError): ...
```

Extend with:
```python
class ConnectionError(BurntError): ...      # Databricks connectivity
class AuthenticationError(ConnectionError): ... # 401 — bad token
class RateLimitError(ConnectionError): ...      # 429 — backoff needed
class WarehouseError(ConnectionError): ...      # warehouse stopped/not found
class TableNotFoundError(EstimationError): ...  # table missing from catalog
class TimeoutError(ConnectionError): ...        # request timeout
```

Each exception must have:
- `message` — user-facing, actionable (e.g. "Token rejected (401). Check BURNT_TOKEN.")
- `suggestion` — recovery step (e.g. "Run: export BURNT_TOKEN=dapi...")
- Token values must be redacted from all error messages and tracebacks

CLI `main.py` must catch `BurntError` subclasses at the top level and print rich-formatted messages (red for error, yellow for suggestion) without full tracebacks in normal mode.

---

## Acceptance Criteria

- [ ] Exception hierarchy extended with `ConnectionError`, `AuthenticationError`, `RateLimitError`, `WarehouseError`, `TableNotFoundError`, `TimeoutError`
- [ ] Each exception has `message` and `suggestion` attributes
- [ ] `DatabricksClient` raises typed exceptions (not raw `requests.HTTPError`)
- [ ] 401 → `AuthenticationError` with redacted token hint
- [ ] 429 → `RateLimitError` with retry-after guidance
- [ ] 503/timeout → `WarehouseError` or `TimeoutError`
- [ ] CLI catches all `BurntError` at top level, prints clean message (no traceback unless `--debug`)
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
