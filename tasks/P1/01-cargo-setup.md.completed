status: done
agent: openrouter/minimax/minimax-m2.7
completed_by: openrouter/minimax/minimax-m2.7

## Implementation
### Changes Made
- `Cell`, `CellKind` (Python/Sql/RunRef), `AnalysisMode` (Python/Sql/Dlt), `Finding`, `Severity`, `Confidence`, `RuleEntry`, `RuleTable` (128-bit bitset).
- `build.rs` reads `rules/registry.toml` → static `REGISTRY` (84 entries).
- `maturin develop` → importable module.

### Implementation Notes
- Use `pyo3` for the bridge.
- Follow the structure defined in DESIGN.md §14.

### Verification Results
- Tests: `cargo test` pass
- Lint: `cargo clippy` pass
