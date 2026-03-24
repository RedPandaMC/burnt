status: todo
agent:
completed_by:

## Implementation
### Changes Made
- Real-notebook latency.
- Memory profiling.
- Fix > 3s or > 50 MB.

### Implementation Notes
- Final performance tuning to meet the stated targets in a production-like environment.
- Use profiling tools to ensure memory and CPU usage is within limits.

### Verification Results
- Tests: `pytest` benchmarks pass
- Lint: `ruff check` pass
