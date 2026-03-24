status: todo
agent:
completed_by:

## Implementation
### Changes Made
- Detect env → access level → read notebook (or file) → `burnt_engine.analyze_*()` → check mode → deserialize → enrich → estimate → session cost → recommend → CheckResult.
- Handle different path inputs (none, file, directory).

### Implementation Notes
- Orchestrate the entire analysis flow in the `burnt.check()` entry point.
- Implement efficient per-file analysis for directory inputs.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
