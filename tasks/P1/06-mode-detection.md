status: todo
agent:
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
