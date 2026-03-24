status: todo
agent:
completed_by:

## Implementation
### Changes Made
- Full → complete.
- Session → graph + DESCRIBE.
- REST → graph + REST enrichment.
- Auth-only → findings + message.

### Implementation Notes
- Verify that the tool correctly handles different access levels and permissions.
- Ensure appropriate features are enabled or disabled based on the level.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
