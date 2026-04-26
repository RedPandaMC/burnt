status: cancelled
agent: executor
completed_by: moonshotai/kimi-k2.6

## Cancellation Reason
DLT (Delta Live Tables) is a Databricks-only feature. PipelineGraph support in the Rust engine should remain for Databricks users, but DLT enrichment via Pipelines API is out of scope for the generic Spark-first architecture.

The Rust engine already builds PipelineGraph for DLT code; Databricks-specific enrichment should live in `burnt[databricks]`.
