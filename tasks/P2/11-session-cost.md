status: todo
agent:
completed_by:

## Implementation
### Changes Made
- Notebook only.
- Total session time, execution time, idle time.
- Utilization percentage.

### Implementation Notes
- Calculate the cost of the current Spark session including idle periods.
- Provide insights into session efficiency.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
