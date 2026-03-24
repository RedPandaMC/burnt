status: todo
agent:
completed_by:

## Implementation
### Changes Made
- `Cargo.lock` committed.
- Python upper bounds.
- Test on DBR 14.3 LTS + 15.x.

### Implementation Notes
- Finalize all dependency pins and ensure compatibility with current Databricks Runtime (DBR) versions.
- Prepare for a stable release.

### Verification Results
- Tests: `pytest` on multiple DBR versions pass
- Lint: `ruff check` pass
