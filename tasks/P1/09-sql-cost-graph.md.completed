status: todo
agent:
completed_by:

## Implementation
### Changes Made
- SQL cells → CostGraph via sqlparser-rs decomposition.
- CREATE TABLE AS SELECT → chain.
- MERGE INTO → chain.
- OPTIMIZE → Maintenance.
- Cross-cell table deps → edges.

### Implementation Notes
- Handle table dependencies correctly even when they are defined in different SQL cells.
- Account for complex SQL statements like MERGE.

### Verification Results
- Tests: `cargo test` pass
- Lint: `cargo clippy` pass
