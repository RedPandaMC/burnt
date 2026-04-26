status: done
agent: executor
completed_by: moonshotai/kimi-k2.6

## Implementation
### Changes Made
- `src/burnt/intelligence/session.py` - SessionCost model + analyze_session()

### Implementation Notes
- Calculates execution_cost_usd, idle_cost_usd, total_cost_usd
- Works with any backend that provides timing data
- Already functional and tested

### Verification Results
- Tests: 300 passed
- Lint: pass
