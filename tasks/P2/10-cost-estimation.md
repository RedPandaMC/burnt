status: todo
agent: executor
completed_by: moonshotai/kimi-k2.6

## Redesign Notes
This task is being redesigned for the new architecture:
- Cost estimation now focuses on **compute seconds** (not dollars)
- Topological graph walk is already implemented in Rust engine
- Python layer should:
  1. Merge runtime listener metrics with graph nodes
  2. Apply scaling functions to produce compute-time estimates for unobserved nodes
  3. Let backends (optional) map compute seconds to dollar amounts

## Remaining Work
- Implement `graph/estimate.py` to merge runtime data with graph nodes
- Map observed stage metrics to CostNode IDs
- Fall back to scaling functions for nodes without runtime data
