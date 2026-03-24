status: todo
agent:
completed_by:

## Implementation
### Changes Made
- `.json()`: mode, summary, per-file details.
- `.markdown()`: formatted for PR comments.

### Implementation Notes
- Support exporting the analysis result into machine-readable JSON and human-readable Markdown.
- Markdown output should be optimized for integration with GitHub PRs.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
