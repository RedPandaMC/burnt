# Task: Add metadata caching and connection pooling

---

## Metadata

```yaml
id: s4-02-caching
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

Add TTL-based caching for `DESCRIBE DETAIL` results and reuse `requests.Session` across calls in `DatabricksClient`. This reduces latency for repeated estimations on the same tables and avoids per-request TCP handshakes.

### Files to read

```
src/burnt/tables/connection.py
src/burnt/parsers/delta.py
src/burnt/estimators/hybrid.py
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

Cache key: `f"{workspace_url}:{table_name}"`. TTL: 300 seconds (5 min) default, configurable via `BURNT_CACHE_TTL` env var.

Apply cache in `DatabricksClient` for any `DESCRIBE DETAIL` queries. The `HybridEstimator` should benefit transparently.

**Event-driven cache invalidation:**

TTL-only caching is wrong for long-running jobs that modify tables between estimation
calls — a 5-minute TTL will serve stale metadata after a large write. Replace TTL as the
primary invalidation mechanism with a change-detection check:

```python
def _is_cache_valid(self, table_name: str) -> bool:
    """Check DESCRIBE HISTORY to detect mutations since last cache fill."""
    try:
        last_modified = self._query_last_modification_time(table_name)
        entry = self._cache.get(table_name)
        if entry is None:
            return False
        return last_modified <= entry.cached_at
    except Exception:
        # Fallback: honour TTL if history check fails
        return self._ttl_valid(table_name)
```

`DESCRIBE HISTORY <table> LIMIT 1` returns the latest operation timestamp cheaply
(single metadata read, no file scanning). Use this as the primary invalidation signal.
TTL remains as a fallback for tables where `DESCRIBE HISTORY` is unavailable (e.g.,
external/non-Delta tables).

Cache write: record `cached_at = time.monotonic()` and store the `last_modified` timestamp
at cache fill time. On cache read, check history first; fall back to TTL comparison only if
the history query throws.

**Batch queries:**

When estimating multiple queries in a single session, fingerprint lookups can be batched into a single `system.query.history` query using `IN (fingerprint1, fingerprint2, ...)` instead of N separate queries. Add `find_similar_queries_batch(client, fingerprints, ...)` to `tables/queries.py`.

---

## Acceptance Criteria

- [ ] `DatabricksClient` uses `requests.Session` with `HTTPAdapter` pool config
- [ ] `DatabricksClient` is a context manager (`__enter__`/`__exit__` closes session)
- [ ] `TTLCache` class implemented (simple in-memory dict with expiry, thread-safe with `threading.Lock`)
- [ ] `DESCRIBE DETAIL` results cached; primary invalidation uses `DESCRIBE HISTORY LIMIT 1` change detection
- [ ] TTL (300s default) used only as fallback when `DESCRIBE HISTORY` unavailable
- [ ] `_CacheEntry` stores `cached_at` timestamp and `last_modified` from history
- [ ] Cache invalidates correctly after a table mutation (simulated in unit test via mock)
- [ ] `find_similar_queries_batch()` added to `tables/queries.py`
- [ ] Cache TTL configurable via `BURNT_CACHE_TTL` env var (add to `core/config.py`)
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
