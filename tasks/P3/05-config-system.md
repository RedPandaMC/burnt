status: todo
agent:
completed_by:

## Implementation
### Changes Made
- Config discovery: `burnt.toml` → `.burnt.toml` → `pyproject.toml` → `~/.config/burnt/burnt.toml`.
- `pydantic-settings` for typed config model.
- `burnt check --init` implementation.

### Implementation Notes
- Implement a hierarchical configuration system following the standard tools like ruff/pytest.
- Support all configuration options via environment variables and files.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
