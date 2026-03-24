status: todo
agent:
completed_by:

## Implementation
### Changes Made
- Move/Refactor existing tests into the new structure (`tests/unit/`, `tests/integration/`).
- Update `conftest.py` if needed for the new architecture.
- Ensure existing fixtures in `tests/fixtures/` are preserved if relevant.

### Implementation Notes
- Some unit tests for basic connection/config might be reusable.
- Most engine-related tests will be new in `burnt-engine/tests`.

### Verification Results
- Tests: N/A
- Lint: pass
