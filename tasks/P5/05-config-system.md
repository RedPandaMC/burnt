status: done
agent: executor
completed_by: moonshotai/kimi-k2.6

## Implementation
### Changes Made
- `src/burnt/core/config.py` - pydantic-settings based config loader
- `src/burnt/cli/main.py` - `burnt init` generates starter config

### Implementation Notes
- Discovery: `.burnt.toml` → `pyproject.toml` [tool.burnt] → `~/.config/burnt/burnt.toml`
- Environment variables: `BURNT_*` prefix with `__` for nesting
- Config schema needs updating to match new DESIGN.md (check, session, display sections)

### Verification Results
- Tests: 300 passed
- Lint: pass
