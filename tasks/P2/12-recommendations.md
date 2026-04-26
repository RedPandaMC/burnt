status: cancelled
agent: executor
completed_by: moonshotai/kimi-k2.6

## Cancellation Reason
The old "three-tier cluster recommendation" (economy/balanced/performance with Databricks API JSON) is replaced by a simpler, more actionable approach:
- Generic Spark recommendations: "60% of ops are shuffle-heavy → add salt column"
- Databricks-specific recommendations remain possible via `burnt[databricks]` optional module
- Dollar-based SKU recommendations require cloud pricing data which varies by platform

## Remaining Work
- Implement generic Spark recommendations in `intelligence/recommend.py`
- Add Databricks-specific recommendation engine in `burnt/databricks/intelligence/`
