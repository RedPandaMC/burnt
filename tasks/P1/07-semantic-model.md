status: todo
agent:
completed_by:

## Implementation
### Changes Made
- `SemanticModel`: scope stack, `bind()` with overwrite findings.
- `classify_rhs` for 14 patterns including `dlt.read`, `dp.read`, `spark.readStream`.
- `ChainContext`: has_limit, has_select, has_filter, action, is_streaming.

### Implementation Notes
- Build a semantic representation of variables and their origins.
- Track call chains for cost estimation later.

### Verification Results
- Tests: `cargo test` pass
- Lint: `cargo clippy` pass
