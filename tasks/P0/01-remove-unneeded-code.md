status: todo
agent:
completed_by:

## Implementation
### Changes Made
- src/burnt/ - remove old architecture (estimators, advisor, forecast, etc.)
- tests/ - remove/clean up tests that are no longer valid or relevant to the new architecture

### Implementation Notes
- Identify which parts of `src/burnt/core` and `src/burnt/_compat.py` are still useful.
- Ensure `pyproject.toml` dependencies are ready for `uv sync`.

### Verification Results
- Tests: N/A
- Lint: pass
