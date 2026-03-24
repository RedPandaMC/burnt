status: todo
agent:
completed_by:

## Implementation
### Changes Made
- Python/SQL: SKU, instance, Photon, spot, API JSON.
- DLT: MV → streaming, tier.
- Session: serverless, auto-termination.

### Implementation Notes
- Generate actionable recommendations to reduce costs.
- Provide the exact JSON needed to update Databricks cluster configurations.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
