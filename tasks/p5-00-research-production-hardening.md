# Task: Research production hardening strategies for dburnrate

---

## Metadata

```yaml
id: p5-00-research-production-hardening
status: todo
phase: 5
priority: high
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Research and document production hardening strategies specific to this project. The output is `docs/production-hardening-research.md` — a design document that informs the implementation tasks p5-01, p5-02, p5-03.

### Files to read

```
src/dburnrate/tables/connection.py
src/dburnrate/core/exceptions.py
src/dburnrate/cli/main.py
src/dburnrate/estimators/hybrid.py
```

### Research areas

1. **Connection pooling** — `requests.Session` reuse in `DatabricksClient`. When to create, when to close. Thread safety considerations for CLI vs library use.

2. **Retry/backoff** — Databricks REST API rate limits (429), transient errors (503). Exponential backoff with jitter. Max retries before surfacing error to user.

3. **Metadata caching** — `DESCRIBE DETAIL` results change infrequently. TTL-based caching (in-memory dict with timestamp). What TTL is appropriate? What cache key to use (table name + workspace)? How to invalidate.

4. **Error message quality** — What does a good error message look like when:
   - Databricks token is invalid (401)
   - Warehouse is stopped (503)
   - Table doesn't exist (SQL error from EXPLAIN)
   - Network timeout
   Map each scenario to a user-facing message and a recovery suggestion.

5. **Structured logging** — Python `logging` stdlib vs `structlog`. JSON formatter for machine consumption. Log levels: DEBUG (full request/response), INFO (estimation signal used), WARNING (fallback triggered), ERROR (unrecoverable).

6. **Security** — Token handling: never log tokens. Redact from tracebacks. Validate workspace URL format.

---

## Acceptance Criteria

- [ ] `docs/production-hardening-research.md` created with findings for all 5 areas
- [ ] Each area has a "Recommended approach" section with specific implementation guidance
- [ ] Error message mapping table (scenario → user message → recovery suggestion)
- [ ] Decision on structured logging library (stdlib vs structlog)
- [ ] No code changes — research only

---

## Verification

```bash
# Verify output file exists and is non-empty
ls -la docs/production-hardening-research.md
wc -l docs/production-hardening-research.md
```

---

## Handoff

### Result

[Executor: fill in after completion]

### Blocked reason

[If blocked, explain here]
