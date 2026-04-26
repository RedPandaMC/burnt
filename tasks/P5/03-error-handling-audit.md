status: todo
agent: executor
completed_by: moonshotai/kimi-k2.6

## Redesign Notes
Error handling audit is still critical:
- Audit every external call (DESCRIBE, system tables, REST APIs)
- Ensure no tracebacks for common failure modes
- Provide clear, actionable error messages

Additional checks needed for new architecture:
- `_session.py` listener registration failures
- `_check.py` engine unavailability
- `display/` IPython import failures
- `watch()` when databricks-sdk is not installed

## Remaining Work
- Systematic review of all `except` blocks
- Ensure `NotAvailableError` is raised with helpful messages
- Verify graceful degradation in all code paths
