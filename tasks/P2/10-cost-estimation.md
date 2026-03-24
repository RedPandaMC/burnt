status: todo
agent:
completed_by:

## Implementation
### Changes Made
- Python/SQL: topological walk, per-node cost, infrastructure, TCO.
- DLT: per-table walk, batch/full models, 1.12× overhead, tier rates.

### Implementation Notes
- Orchestrate the cost estimation by traversing the graph and applying scaling functions.
- Combine operation-level costs with infrastructure (VM) costs.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
