status: todo
agent:
completed_by:

## Implementation
### Changes Made
- `@dlt.table`/`@dp.table` functions and `CREATE STREAMING TABLE` SQL → PipelineGraph.
- Streaming/MV/temp classification.
- `dlt.read()`/`dp.read()`/`LIVE.ref` → edges.
- Inner CostNodes from function bodies.
- Expectations from decorators/CONSTRAINT.

### Implementation Notes
- Model DLT pipelines specifically as graphs of tables rather than operations.
- Capture table-level metadata for DLT.

### Verification Results
- Tests: `cargo test` pass
- Lint: `cargo clippy` pass
