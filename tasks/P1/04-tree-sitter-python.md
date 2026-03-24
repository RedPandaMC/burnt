status: todo
agent:
completed_by:

## Implementation
### Changes Made
- Grammar init once.
- DLT/SDP detection from AST: `import dlt`, `from dlt import`, `@dlt.table`, `@dp.table`, `@dp.materialized_view`.
- SQL extraction: `spark.sql(string)` → fragment, `spark.sql(f_string)` → BN002.
- Syntax error → partial tree + finding.

### Implementation Notes
- Use `tree-sitter-python` for parsing.
- Identify SQL fragments within Python code for subsequent SQL analysis.

### Verification Results
- Tests: `cargo test` pass
- Lint: `cargo clippy` pass
