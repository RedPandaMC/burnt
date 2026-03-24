status: todo
agent:
completed_by:

## Implementation
### Changes Made
- ~28 PySpark, ~15 SQL (incl. BSQ001-003), ~5 DLT (DLT002-003).
- TOML + tree-sitter queries.
- Fixture + insta snapshot per rule.

### Implementation Notes
- Externalize rules into TOML files for easier editing and extension.
- Use `insta` for snapshot testing to verify rule findings.

### Verification Results
- Tests: `cargo test` pass
- Lint: `cargo clippy` pass
