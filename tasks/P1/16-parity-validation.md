status: todo
agent:
completed_by:

## Implementation
### Changes Made
- Backward shim: `detect_antipatterns()` calls Rust.
- Rust ≥ Python on all fixtures.
- proptest: no panics 10k inputs.
- ≥5× throughput.
- v1 tests pass.

### Implementation Notes
- Ensure parity with the previous version's functionality.
- Benchmark and verify the performance improvements.

### Verification Results
- Tests: `cargo test` pass, `pytest` pass
- Lint: `cargo clippy` pass, `ruff check` pass
