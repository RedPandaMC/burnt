status: todo
agent:
completed_by:

## Implementation
### Changes Made
- Wrap every external call (DESCRIBE, system tables, etc.).
- Implement fallback behavior for each access level.
- Clear user messaging on degraded features.

### Implementation Notes
- Ensure the tool never crashes due to missing permissions or API failures.
- Provide honest feedback to the user on the confidence and completeness of the results.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
