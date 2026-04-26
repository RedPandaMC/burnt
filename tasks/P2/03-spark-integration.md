status: done
agent: executor
completed_by: moonshotai/kimi-k2.6

## Implementation
### Changes Made
- `src/burnt/_session.py` - SessionState + SparkListener registration
- `src/burnt/core/models.py` - SessionCost model already existed and works

### Implementation Notes
- Listener captures stage metrics: shuffle_read_bytes, shuffle_write_bytes, input_bytes, output_bytes, executor_cpu_time_ms
- Also captures SQL execution events
- Gracefully degrades if Spark is unavailable
- Session state is stored globally in `burnt.__init__._SESSION`

### Verification Results
- Tests: 300 passed
- Lint: pass (with known Databricks-specific fixture warnings)
