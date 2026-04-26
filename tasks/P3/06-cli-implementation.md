status: todo
agent: executor
completed_by: moonshotai/kimi-k2.6

## Redesign Notes
CLI `burnt check` already partially works via `src/burnt/cli/main.py`. Remaining work:
- Update `check` command to call new `_check.run()` instead of old lint-only flow
- Add `--event-log` flag for post-hoc analysis
- Remove or move `burnt advise` and `burnt doctor` to Databricks-specific CLI
- Add `burnt rules` and `burnt cache` commands (already exist)

## Verification Results
- Tests: 300 passed
- Lint: pass
