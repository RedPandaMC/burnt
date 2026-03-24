status: todo
agent:
completed_by:

## Implementation
### Changes Made
- Azure catalog (23 VMs).
- DLT tier rates.
- `get_dbu_rate(sku, dlt_tier)`.

### Implementation Notes
- Maintain an internal catalog of Databricks instance types and their respective DBU/VM pricing.
- Support multiple cloud platforms if necessary (Azure as initial focus).

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
