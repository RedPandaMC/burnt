status: todo
agent:
completed_by:

## Implementation
### Changes Made
- 5 formats: Databricks `.py`, plain `.py`, plain `.sql`, `.ipynb`, DBSQL `.sql`.
- `classify_magic()` routes cells.
- `byte_offset` per cell.
- Malformed → Finding, no panic.

### Implementation Notes
- tree-sitter will be used for parsing.
- Support both old-style (`# MAGIC`) and new-style notebook formats.

### Verification Results
- Tests: `cargo test` pass
- Lint: `cargo clippy` pass
