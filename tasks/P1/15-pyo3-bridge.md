status: todo
agent:
completed_by:

## Implementation
### Changes Made
- `analyze_file`, `analyze_source`, `analyze_directory`.
- serde_json → PyDict.
- Returns: `mode`, `graph`/`pipeline`, `findings`, `cells`.
- `analyze_directory` releases GIL, rayon parallel.

### Implementation Notes
- Expose the Rust engine's functionality to Python via PyO3.
- Ensure thread safety when analyzing multiple files in parallel.

### Verification Results
- Tests: `cargo test` pass
- Lint: `cargo clippy` pass
