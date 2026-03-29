status: done
agent: openrouter/minimax/minimax-m2.7
completed_by: openrouter/minimax/minimax-m2.7

## Implementation
### Changes Made
- src/burnt-engine/src/semantic/mod.rs - Consolidated semantic module

### Implementation Notes
- SemanticModel with scope stack and binding tracking
- classify_rhs for 14 patterns: dlt.read, dp.read, spark.read, spark.readStream, udf, etc.
- ChainContext tracking: has_limit, has_select, has_filter, is_streaming, source_tables
- Shadow detection with Finding emission

### Verification Results
- Tests: 27 passed
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
