status: todo
agent: executor
completed_by: moonshotai/kimi-k2.6

## Redesign Notes
This task is being redesigned for the new architecture:
- Delta enrichment is now a Databricks-specific feature
- It should be implemented in `burnt/databricks/` namespace, not core
- Generic Spark users can use runtime listener data instead

## Remaining Work
- Move delta enrichment to `burnt/databricks/enrich.py`
- Implement DESCRIBE DETAIL enrichment via DatabricksBackend
