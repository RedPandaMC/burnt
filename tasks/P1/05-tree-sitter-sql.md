status: done
agent: openrouter/minimax/minimax-m2.7
completed_by: openrouter/minimax/minimax-m2.7

## Implementation
### Changes Made
- src/burnt-engine/src/parse/sql.rs - Complete rewrite using sqlparser-rs with DatabricksDialect + tree-sitter-sequel
- src/burnt-engine/Cargo.toml - Updated to tree-sitter-sequel 0.3
- Added SqlParseResult, SqlFragmentWithAst, DltTableDef, DltTableKind types

### Implementation Notes
- Uses tree-sitter-sequel for pattern matching
- Uses sqlparser-rs with DatabricksDialect for typed SQL AST
- DLT detection via heuristic (name/query contains "streaming")
- extract_table_refs for table reference tracking
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
