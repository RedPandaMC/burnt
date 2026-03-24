status: todo
agent:
completed_by:

## Implementation
### Changes Made
- Empty notebook.
- 100+ cells.
- Nested `%run`.
- Circular `%run`.
- Every cell syntax error.
- `.ipynb` markdown only.
- Mixed DLT + non-DLT.

### Implementation Notes
- Thoroughly test the tool against all identified edge cases.
- Verify that the tool handles each case gracefully and as expected.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
