status: done
agent: executor
completed_by: moonshotai/kimi-k2.6

## Implementation
### Changes Made
- `src/burnt/runtime/rest_backend.py` - RestBackend exists but is now lazily imported
- `src/burnt/runtime/__init__.py` - Only exports generic backends by default
- `pyproject.toml` - `databricks-sdk` moved to `[project.optional-dependencies] databricks`

### Implementation Notes
- RestBackend is only used when:
  1. `DATABRICKS_HOST` environment variable is set
  2. `databricks-sdk` is installed (`pip install burnt[databricks]`)
- This prevents mandatory Databricks dependencies in the core package

### Verification Results
- Tests: 300 passed
- Lint: pass
