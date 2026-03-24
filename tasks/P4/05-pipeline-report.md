status: todo
agent:
completed_by:

## Implementation
### Changes Made
- 30 updates from `pipeline_event_log`.
- Per-table cost trend.
- Dominant cost table.

### Implementation Notes
- Analyze historical costs for DLT pipelines at the table level.
- Identify "hotspots" or tables that contribute most to the pipeline cost.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
