status: todo
agent:
completed_by:

## Implementation
### Changes Made
- Pipelines API → PipelineConfig.
- `pipeline_event_log` → per-table metrics.
- Streaming batch sizes.
- Graceful when unavailable.

### Implementation Notes
- Use the Pipelines API to retrieve runtime metrics for DLT pipelines.
- Integrate these metrics into the PipelineGraph.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
