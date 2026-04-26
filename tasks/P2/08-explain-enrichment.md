status: todo
agent: executor
completed_by: moonshotai/kimi-k2.6

## Redesign Notes
This task is being redesigned for the new architecture:
- EXPLAIN parsing in `src/burnt/parsers/explain.py` already exists and is mostly generic Spark
- It should be verified to work with standard Spark EXPLAIN output (not just Databricks EXPLAIN COST)
- Runtime listener provides actual stage metrics, reducing dependence on EXPLAIN estimates

## Remaining Work
- Verify explain.py works with standard Spark `EXPLAIN EXTENDED` output
- Integrate explain.py into the hybrid check() flow when runtime data is unavailable
