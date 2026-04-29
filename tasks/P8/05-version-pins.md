status: todo
agent: executor
completed_by: moonshotai/kimi-k2.6

## Redesign Notes
Version pinning remains important:
- `Cargo.lock` committed in repo
- Python upper bounds in `pyproject.toml`
- Test on multiple Python versions (3.12+)

No longer DBR-specific since core package is generic Spark.

## Remaining Work
- Verify `Cargo.lock` is committed
- Pin Python dependency upper bounds
- Test on clean Python 3.12 environment
