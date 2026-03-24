status: todo
agent:
completed_by:

## Implementation
### Changes Made
- TOML → CompiledRule.
- Phase execution (0–6).
- Suppression: codes and names.
- DLT escalation.
- Sorted output.
- Graph node linking.

### Implementation Notes
- Orchestrate the rule execution pipeline from simple patterns to semantic rules.
- Support rule suppression via comments in the source code.

### Verification Results
- Tests: `cargo test` pass
- Lint: `cargo clippy` pass
