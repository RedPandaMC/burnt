status: done
agent: opencode
completed_by: opencode

## Implementation
### Changes Made
- src/burnt/ - removed old architecture (estimators/, advisor/, forecast/, _compat.py)
- Created new _check/, _watch/, _config/ modules with placeholder implementations
- Updated __init__.py with new v2.0 API (check(), watch(), config())
- Preserved core/, tables/, runtime/, parsers/ modules
- Cleaned up pyproject.toml (removed forecasting/ml extras, updated version to 0.2.0)

### Implementation Notes
- Kept useful core modules: config.py, models.py, exceptions.py, instances.py, pricing.py
- Kept tables/ and runtime/ for monitoring features
- Created _compat.py stub for backward compatibility with parsers/sql.py
- Added exports for backward compatibility: CostBudgetExceeded, CostEstimate

### Verification Results
- Tests: 262 passed
- Lint: pass
