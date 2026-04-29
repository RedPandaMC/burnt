status: done
agent: executor
completed_by: moonshotai/kimi-k2.6

## Implementation
### Changes Made
- `src/burnt/graph/scaling.py` - Already implemented with 5 scaling functions
  - linear, linear_with_cliff, quadratic, step_failure, maintenance

### Implementation Notes
- These functions operate on compute seconds / bytes, not dollars
- Remain generic Spark (no Databricks dependencies)
- Used by the estimation pipeline after graph enrichment

### Verification Results
- Tests: 300 passed
- Lint: pass
