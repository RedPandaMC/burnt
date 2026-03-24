status: todo
agent:
completed_by:

## Implementation
### Changes Made
- DESCRIBE DETAIL → `estimated_input_bytes`.
- Partition pruning.
- DESCRIBE HISTORY → growth rate.
- Cache TTL 5min.
- Missing tables → skip.

### Implementation Notes
- Fetch table statistics from Databricks to enrich the analysis graph with data sizes.
- Handle partition pruning logic to refine input size estimates.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
