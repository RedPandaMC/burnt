status: todo
agent:
completed_by:

## Implementation
### Changes Made
- `databricks-sdk` WorkspaceClient.
- Statement Execution API.
- Pipelines API.
- SQL warehouse auto-discovery.
- TTL caching (Delta 5min, system tables 1hr, configs 10min).
- ThreadPoolExecutor for concurrent queries.

### Implementation Notes
- Implement a backend that uses the Databricks REST API for connectivity outside of a SparkSession.
- Use the `databricks-sdk` for reliable communication.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
