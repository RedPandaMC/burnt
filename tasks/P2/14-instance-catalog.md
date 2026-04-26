status: cancelled
agent: executor
completed_by: moonshotai/kimi-k2.6

## Cancellation Reason
The Azure instance catalog (23 VMs with DBU rates) is Databricks-specific. The new architecture uses compute seconds as the core unit:
- Core package: no instance catalog needed
- Generic Spark: users see executor-hours
- Dollar estimates: delegated to optional backends (Databricks, AWS, GCP)

The existing `core/instances.py` and `core/pricing.py` should be moved to `burnt/databricks/pricing/`.
