status: todo
agent:
completed_by:

## Implementation
### Changes Made
- Invalid `burnt.toml` / `pyproject.toml` → clear error.
- Conflicting env var and file → priority applied.
- `burnt check --init` round-trips.

### Implementation Notes
- Ensure all possible configuration scenarios are handled correctly and provide helpful error messages.
- Verify that configuration from different sources follows the defined priority rules.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
