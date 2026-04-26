status: done
agent: executor
completed_by: moonshotai/kimi-k2.6

## Implementation
### Changes Made
- `src/burnt/_session.py` - contextlib.suppress(Exception) on listener registration
- `src/burnt/_check/__init__.py` - works without engine, without session, without Databricks
- `src/burnt/runtime/auto.py` - gracefully returns None if no backend detected

### Implementation Notes
- If Spark is unavailable: static analysis only (no crash)
- If Databricks is not installed: `burnt.watch()` raises NotAvailableError with clear message
- If file doesn't exist: empty CheckResult (no crash)

### Verification Results
- Tests: 300 passed
- Lint: pass
