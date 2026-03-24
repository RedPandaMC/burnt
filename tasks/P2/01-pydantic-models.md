status: todo
agent:
completed_by:

## Implementation
### Changes Made
- All types: `CostGraph`, `PipelineGraph`, `CostNode`, `CostEdge`, `PipelineTable`, `TableDependency`, `PipelineConfig`, `Expectation`, `Finding`, `CostEstimate`, `SessionCost`, `CheckResult`, `ClusterConfig`.
- Deserialization from Rust JSON via `model_validate()`.

### Implementation Notes
- Use Pydantic v2 for data validation and modeling.
- Ensure mapping between Rust and Python types is consistent.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
