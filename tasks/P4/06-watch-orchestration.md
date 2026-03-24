status: todo
agent:
completed_by:

## Implementation
### Changes Made
- Parameters control: `tag_key`, `drift_threshold`, `idle_threshold`, `budget`, `days`, `job_id`, `pipeline_id`.
- Reads defaults from `[watch]` in `burnt.toml`.
- Returns `WatchResult`.

### Implementation Notes
- Orchestrate the `burnt.watch()` entry point to provide a comprehensive monitoring experience.
- Combine various monitoring metrics into a single report.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
