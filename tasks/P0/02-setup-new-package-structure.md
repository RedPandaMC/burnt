status: todo
agent:
completed_by:

## Implementation
### Changes Made
- Create `burnt-engine/` directory for the Rust core.
- Scaffolding the new `src/burnt/` structure as per DESIGN.md §14.
- Update `pyproject.toml` to include `burnt-engine` as a dependency and use `maturin`.

### Implementation Notes
- Structure `src/burnt/` with `graph/`, `intelligence/`, `watch/`, `alerts/`, etc.
- Prepare `burnt-engine/` with `Cargo.toml` and basic PyO3 setup.

### Verification Results
- Tests: N/A
- Lint: pass
