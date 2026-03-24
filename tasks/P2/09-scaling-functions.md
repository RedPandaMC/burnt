status: todo
agent:
completed_by:

## Implementation
### Changes Made
- 7 functions: Linear, LinearWithCliff, Quadratic, StepFailure, Maintenance, StreamBatch, FullRecompute.
- Calibration dict by `(operation_kind, instance_family)`.
- Cluster enrichment fills thresholds.

### Implementation Notes
- Implement a set of scaling functions to translate data volume and operation type into cost.
- Support instance-specific calibration factors.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
