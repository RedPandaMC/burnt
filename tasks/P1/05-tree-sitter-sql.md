status: todo
agent:
completed_by:

## Implementation
### Changes Made
- tree-sitter-sql for patterns.
- sqlparser-rs `DatabricksDialect::default()` for typed AST.
- DLT SQL: `CREATE STREAMING TABLE`, `CREATE MATERIALIZED VIEW`, `LIVE.ref`.
- `SqlFragment` with provenance.

### Implementation Notes
- Combine tree-sitter for pattern matching and sqlparser-rs for deep semantic analysis.
- Ensure `DatabricksDialect` handles specific DLT SQL extensions.

### Verification Results
- Tests: `cargo test` pass
- Lint: `cargo clippy` pass
