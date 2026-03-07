# Task: Add metadata caching and connection pooling

---

## Metadata

```yaml
id: p5-02-caching-and-performance
status: todo
phase: 5
priority: medium
agent: ~
blocked_by: [p5-01-error-handling]
created_by: planner
```

---

## Context

### Goal

Add TTL-based caching for `DESCRIBE DETAIL` results and reuse `requests.Session` across calls in `DatabricksClient`. This reduces latency for repeated estimations on the same tables and avoids per-request TCP handshakes.

### Files to read

```
src/dburnrate/tables/connection.py
src/dburnrate/parsers/delta.py
src/dburnrate/estimators/hybrid.py
docs/production-hardening-research.md   (from p5-00)
```

### Background

**Connection pooling:**

`DatabricksClient` currently creates a `requests.Session` in `__init__` but may not configure pool size or keep-alive. Ensure:
- Session is created once in `__init__`, reused across all `execute_sql()` calls
- `session.mount()` with `HTTPAdapter(pool_connections=4, pool_maxsize=10, max_retries=...)`
- Session closed via `__del__` or context manager (`__enter__`/`__exit__`)

**Metadata caching:**

`DESCRIBE DETAIL` output for a table rarely changes within a CLI invocation (or even across invocations in a short window). Add a simple TTL cache:

```python
from dataclasses import dataclass, field
from time import monotonic

@dataclass
class _CacheEntry:
    value: object
    expires_at: float

class TTLCache:
    def __init__(self, ttl_seconds: float = 300.0): ...
    def get(self, key: str) -> object | None: ...
    def set(self, key: str, value: object) -> None: ...
```

Cache key: `f"{workspace_url}:{table_name}"`. TTL: 300 seconds (5 min) default, configurable via `DBURNRATE_CACHE_TTL` env var.

Apply cache in `DatabricksClient` for any `DESCRIBE DETAIL` queries. The `HybridEstimator` should benefit transparently.

**Batch queries:**

When estimating multiple queries in a single session, fingerprint lookups can be batched into a single `system.query.history` query using `IN (fingerprint1, fingerprint2, ...)` instead of N separate queries. Add `find_similar_queries_batch(client, fingerprints, ...)` to `tables/queries.py`.

---

## Acceptance Criteria

- [ ] `DatabricksClient` uses `requests.Session` with `HTTPAdapter` pool config
- [ ] `DatabricksClient` is a context manager (`__enter__`/`__exit__` closes session)
- [ ] `TTLCache` class implemented (simple in-memory dict with expiry, thread-safe with `threading.Lock`)
- [ ] `DESCRIBE DETAIL` results cached with 300s TTL (configurable via env var)
- [ ] `find_similar_queries_batch()` added to `tables/queries.py`
- [ ] Cache TTL configurable via `DBURNRATE_CACHE_TTL` env var (add to `core/config.py`)
- [ ] New unit tests: `tests/unit/tables/test_cache.py`, `tests/unit/tables/test_batch_queries.py`
- [ ] All existing tests still pass
- [ ] `uv run ruff check src/ tests/` exits 0

---

## Verification

```bash
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
