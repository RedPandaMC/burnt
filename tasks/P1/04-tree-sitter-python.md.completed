status: done
agent: openrouter/minimax/minimax-m2.7
completed_by: openrouter/minimax/minimax-m2.7

## Implementation
### Changes Made
- src/burnt-engine/src/parse/python.rs - Complete rewrite using tree-sitter-python
- src/burnt-engine/src/types.rs - Added SqlFragment, Provenance, DltSignal, PythonParseResult types

### Implementation Notes
- Grammar init happens per-parse (tree-sitter-python handles internally)
- DLT/SDP detection via AST walk: import dlt/dp, @dlt.table, @dp.table, @dp.materialized_view
- SQL extraction from spark.sql() calls with proper string_content handling
- F-string detection via string_start node prefix check
- Syntax error detection via ERROR node traversal

### Verification Results
- Tests: 13 passed (4 python tests, 9 notebook tests)
- Clippy: pre-existing errors in lib.rs (unrelated)
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
