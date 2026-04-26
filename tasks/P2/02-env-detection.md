status: done
agent: executor
completed_by: moonshotai/kimi-k2.6

## Implementation
### Changes Made
- `src/burnt/runtime/auto.py` - auto_backend() now detects generic SparkSession first, then Databricks
- `src/burnt/_session.py` - start_session() auto-detects Spark context

### Implementation Notes
- Old "access levels" (Full/Session/REST/Auth-only) are replaced by a simpler model:
  - Core package works everywhere (static analysis)
  - Spark integration works when SparkSession is available
  - Databricks integration requires `pip install burnt[databricks]`
- Graceful degradation: if Spark is not available, check() runs static-only

### Verification Results
- Tests: 300 passed
- Lint: pass
