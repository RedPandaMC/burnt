status: todo
agent:
completed_by:

## Implementation
### Changes Made
- `DATABRICKS_RUNTIME_VERSION` → notebook.
- `DATABRICKS_HOST` → connected.
- Neither → `ConnectionRequired`.
- Probe system tables → Full.
- Probe DESCRIBE → Session.
- REST → REST.
- Nothing → Auth-only.

### Implementation Notes
- Implement logic to detect the runtime environment and appropriate access level.
- Ensure graceful handling of missing environment variables or permissions.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
