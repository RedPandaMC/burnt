status: todo
agent:
completed_by:

## Implementation
### Changes Made
- `calibrate(job_id, run_id)`: billing → per-node.
- `calibrate(pipeline_id, update_id)`: event_log → per-table.
- EMA 0.3/0.7.
- Store per calibration config (local file or Delta).

### Implementation Notes
- Implement a feedback loop that uses actual billing data to refine cost estimation coefficients.
- Use exponential moving average (EMA) for calibration.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
