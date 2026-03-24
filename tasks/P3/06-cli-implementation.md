status: todo
agent:
completed_by:

## Implementation
### Changes Made
- `burnt check <path>` with all required flags.
- `burnt check --explain [rule]`.
- Exit codes: 0 clean, 1 threshold exceeded, 2 internal error.

### Implementation Notes
- Create a lightweight CLI using `typer`.
- Ensure CLI flags correctly override configuration defaults.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
