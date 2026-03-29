status: todo
agent:
completed_by:

## Implementation
### Changes Made
- SemanticModel → CostGraph.
- Operations → CostNodes.
- Bindings → CostEdges.
- OperationKind, ScalingBehavior type, photon_eligible, shuffle_required, driver_bound.

### Implementation Notes
- Map logical operations in Python to physical-like nodes for cost estimation.
- Ensure all relevant Spark operations are captured.

### Verification Results
- Tests: `cargo test` pass
- Lint: `cargo clippy` pass
