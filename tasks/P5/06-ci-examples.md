status: todo
agent: executor
completed_by: moonshotai/kimi-k2.6

## Redesign Notes
CI integration is under consideration. The tool is primarily designed for interactive notebook/CLI use, but `burnt check` could run in CI for static analysis.

Changes needed:
- CI examples should use `burnt check --json` for machine-readable output
- Databricks credentials are NOT required for static analysis (core package works without them)
- Focus on GitHub Actions for pure static analysis

## Remaining Work
- Create `docs/ci-examples/github-actions.yml` for `burnt check` with JSON output
- Document that CI usage requires `pip install burnt` only (no databricks extra needed)
