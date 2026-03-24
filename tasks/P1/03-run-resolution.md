status: todo
agent:
completed_by:

## Implementation
### Changes Made
- `parse_and_resolve(path, root)` → flat `Vec<Cell>`, Python and Sql only.
- Recursive inline.
- Cycle detection.
- Circular → BN003.
- Missing → BN001.
- `origin_path` tracks source file.

### Implementation Notes
- Resolve `%run` directives within the Rust engine.
- Handle relative and absolute paths if possible.

### Verification Results
- Tests: `cargo test` pass
- Lint: `cargo clippy` pass
