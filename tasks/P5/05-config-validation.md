status: todo
agent: executor
completed_by: moonshotai/kimi-k2.6

## Redesign Notes
Config validation remains important but schema has changed:
- New sections: `[check]`, `[session]`, `[display]`
- Removed/cleaned: old `[watch]`, `[alert]` still exist for Databricks extra
- Priority: CLI args > `burnt.config()` > burnt.toml > env vars

## Remaining Work
- Ensure config loader handles new `[session]` and `[display]` sections
- Validate that unknown sections/keys are handled gracefully
- Test `burnt.toml` round-trip (init → edit → load)
