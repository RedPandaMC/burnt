status: todo
agent: executor
completed_by: moonshotai/kimi-k2.6

## Redesign Notes
Packaging changes already made:
- `pyproject.toml`: `databricks-sdk` moved to optional dependencies
- Rust engine compiles successfully via Maturin
- `pip install burnt` installs the lightweight core
- `pip install burnt[databricks]` adds Databricks support

## Remaining Work
- Verify wheel builds correctly for both Linux and macOS
- Test `pip install burnt` in a clean environment
- Ensure GitHub Actions CI/CD pipeline works with the new package structure
