status: todo
agent:
completed_by:

## Implementation
### Changes Made
- Cluster config from SparkContext: instance type, workers, Photon, memory, SKU.
- Notebook source retrieval.
- `%run` path resolution.

### Implementation Notes
- Integrate with the active SparkSession to extract environmental metadata.
- Support both interactive and job contexts.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
