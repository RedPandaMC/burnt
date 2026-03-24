status: todo
agent:
completed_by:

## Implementation
### Changes Made
- `templates/burnt_monitor.py` notebook.
- Widgets for configuration.
- Calls `burnt.watch()` + `.alert()`.

### Implementation Notes
- Provide a ready-to-use Databricks notebook for automated cost monitoring.
- Ensure the monitoring job itself is cost-efficient.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
