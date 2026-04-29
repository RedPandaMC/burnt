status: done
agent: executor
completed_by: moonshotai/kimi-k2.6

## Implementation
### Changes Made
- `src/burnt/display/notebook.py` - HTML output for Jupyter/Databricks notebooks
- `src/burnt/display/__init__.py` - `auto_render()` detects notebook vs terminal

### Implementation Notes
- Uses IPython.display.HTML when in ZMQInteractiveShell
- Severity-based color coding: error (red), warning (yellow), info (blue)
- Clean HTML table rendering with collapsible sections

### Verification Results
- Tests: 300 passed
- Lint: pass
