status: todo
agent:
completed_by:

## Implementation
### Changes Made
- Audit every external call (DESCRIBE, system tables, etc.).
- Ensure no tracebacks for common failure modes.

### Implementation Notes
- Conduct a thorough review of error handling throughout the application.
- Ensure all possible API failures result in graceful degradation.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
