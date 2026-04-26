status: done
agent: executor
completed_by: moonshotai/kimi-k2.6

## Implementation
### Changes Made
- `src/burnt/core/models.py` - CostEstimate, ClusterConfig, etc. already exist
- `src/burnt/graph/model.py` - CostGraph, CostNode, CostEdge
- `src/burnt/_check/__init__.py` - CheckResult, Finding models

### Implementation Notes
- Graph models are constructed by the Rust engine and exposed via PyO3 (no JSON deserialization needed)
- CostEstimate uses compute seconds as core unit, not dollars
- Remaining work: refactor ClusterConfig to remove Databricks SKU validation and Azure instance assumptions

### Verification Results
- Tests: 300 passed
- Lint: pass
