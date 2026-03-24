status: todo
agent:
completed_by:

## Implementation
### Changes Made
- Notebook only.
- EXPLAIN → `estimated_output_bytes`.
- Skip in REST mode.

### Implementation Notes
- Use Spark's EXPLAIN command to get internal row/byte estimates when available.
- Handle cases where EXPLAIN fails or provides limited information.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
