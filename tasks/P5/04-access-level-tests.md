status: cancelled
agent: executor
completed_by: moonshotai/kimi-k2.6

## Cancellation Reason
The old "access levels" (Full / Session / REST / Auth-only) have been removed. The new model is:
- Core package: static analysis works everywhere
- Spark integration: works when SparkSession is available
- Databricks integration: requires `pip install burnt[databricks]`

Tests should instead verify:
1. Static analysis without any backend
2. Static + runtime listener with SparkBackend
3. Databricks enrichment with DatabricksBackend (optional extra)

See P5/01-e2e-tests.md for the redesigned test plan.
