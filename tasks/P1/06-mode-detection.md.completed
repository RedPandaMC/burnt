status: done
agent: openrouter/minimax/minimax-m2.7
completed_by: openrouter/minimax/minimax-m2.7

## Implementation
### Changes Made
- src/burnt-engine/src/detect.rs - New file with mode detection
- src/burnt-engine/src/lib.rs - Updated to use detect_mode_from_source

### Implementation Notes
- Priority: DLT signals > all-SQL > Python
- DLT detection via string matching (import dlt, @dlt.table, CREATE STREAMING TABLE, etc.)
- Replaced string-based detection in lib.rs check() with formal detect module

### Verification Results
- Tests: 23 passed
completed_by:

## Implementation
### Changes Made
- After %run resolution: DLT signal → Dlt.
- All cells SQL → Sql.
- Otherwise → Python.

### Implementation Notes
- Implement logic to decide the analysis mode for the entire notebook/file set.
- Prioritize DLT signals over others.

### Verification Results
- Tests: `cargo test` pass
- Lint: `cargo clippy` pass
