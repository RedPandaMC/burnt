status: todo
agent:
completed_by:

## Implementation
### Changes Made
- `pyproject.toml` final.
- `maturin build --release` 4 platforms.
- `pip install burnt` on 3.10/3.11/3.12.

### Implementation Notes
- Prepare the project for distribution as a Python wheel with a Rust backend.
- Ensure compatibility with standard Python package managers.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
