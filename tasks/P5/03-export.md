status: done
agent: executor
completed_by: moonshotai/kimi-k2.6

## Implementation
### Changes Made
- `src/burnt/display/export.py` - `to_json()` and `to_markdown()`
- `src/burnt/_check/__init__.py` - `CheckResult.to_json()` and `CheckResult.to_markdown()`

### Implementation Notes
- JSON includes file_path, mode, compute_seconds, findings array
- Markdown is formatted for PR descriptions with emoji severity indicators

### Verification Results
- Tests: 300 passed
- Lint: pass
