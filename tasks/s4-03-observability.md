# Task: Add structured logging, debug mode, and timing metrics

---

## Metadata

```yaml
id: s4-03-observability
status: todo
sprint: 4
priority: medium
agent: ~
blocked_by: [s4-01-error-handling]
created_by: planner
```

---

## Context

### Goal

Add structured logging throughout the estimation pipeline, a `--debug` CLI flag that enables verbose output, and per-tier timing metrics in `CostEstimate`. This lets users and operators understand which estimation signal was used, how long each tier took, and what data was available.

### Files to read

```
src/burnt/cli/main.py
src/burnt/estimators/hybrid.py
src/burnt/estimators/static.py
src/burnt/tables/connection.py
src/burnt/core/models.py
docs/production-hardening-research.md   (from p5-00)
```

### Background

**Logging setup:**

Use Python stdlib `logging`. Add a `burnt` logger in `src/burnt/__init__.py`:
```python
import logging
logging.getLogger("burnt").addHandler(logging.NullHandler())
```

Library consumers configure handlers themselves (best practice). CLI configures a handler when `--debug` is set.

Log levels:
- `DEBUG` — full request URL (no token), response status, cache hit/miss, EXPLAIN plan text
- `INFO` — estimation signal used ("Using historical signal: 14 executions"), fallback triggered
- `WARNING` — fallback with reason ("EXPLAIN failed: warehouse stopped — using static")
- `ERROR` — unrecoverable error before raising exception

**Debug mode in CLI:**

```bash
uv run burnt estimate "SELECT ..." --debug
```

When `--debug`:
- Configure `logging.basicConfig(level=logging.DEBUG)` for `burnt` logger
- Show full traceback on error (not just user-friendly message)
- Print timing breakdown in output table

**Timing metrics in `CostEstimate`:**

Extend `CostEstimate.breakdown` to include timing:
```python
breakdown: dict[str, float] = {}
# e.g. {"complexity": 34.0, "tier1_ms": 12.3, "tier2_ms": 1840.5, "tier3_ms": 210.1}
```

Use `time.perf_counter()` in `HybridEstimator` to time each tier.

Output table shows timing when `--debug`:
```
│ Tier timing     │ static: 2ms, explain: 1840ms, history: 210ms │
```

---

## Acceptance Criteria

- [ ] `logging.NullHandler()` added to `burnt` root logger in `__init__.py`
- [ ] `DEBUG`/`INFO`/`WARNING` log calls added in `hybrid.py`, `connection.py`, `cli/main.py`
- [ ] Tokens never appear in log output (use `client._redact_url()` helper or similar)
- [ ] `--debug` flag on all CLI commands enables `DEBUG` logging and full tracebacks
- [ ] `CostEstimate.breakdown` includes `tier1_ms`, `tier2_ms`, `tier3_ms` when available
- [ ] Output table shows tier timing row when `--debug` is set
- [ ] New unit tests: `tests/unit/cli/test_debug_flag.py`
- [ ] All existing tests still pass (logging doesn't break anything)
- [ ] `uv run ruff check src/ tests/` exits 0

---

## Verification

```bash
uv run pytest -m unit -v tests/unit/cli/
uv run pytest -m unit -v 2>&1 | tail -5
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
# Manual smoke test:
uv run burnt estimate "SELECT 1" --debug
```

---

## Handoff

### Result

[Executor: fill in after completion]

### Blocked reason

[If blocked, explain here]
