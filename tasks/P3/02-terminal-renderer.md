status: done
agent: executor
completed_by: moonshotai/kimi-k2.6

## Implementation
### Changes Made
- `src/burnt/display/terminal.py` - Rich table output for CLI

### Implementation Notes
- Uses rich.Console and rich.Table
- Severity color coding: error (red), warning (yellow), info (blue)
- Shows rule ID, location, message, and suggestion

### Verification Results
- Tests: 300 passed
- Lint: pass
