```yaml
id: P6-05-azure-databricks-backend
status: todo
phase: 6
priority: medium
agent: ~
blocked_by: [P6-00, P4-01]
created_by: planner
```

## Context

### Goal

Implement the `[azure-databricks]` `PricingBackend` — converts compute-seconds to USD
using Azure Databricks DBU rates combined with Azure VM pricing.

### Background

`[azure-databricks]` auto-pulls `[databricks]` (workspace API + system tables). The
pricing backend needs two components:

1. **DBU rate**: from the Databricks workspace API (`/api/2.0/clusters/get`) — cluster
   node type → DBU count; DBU price from Azure retail pricing API or a bundled JSON
2. **VM rate**: from Azure Billing API (`azure-mgmt-billing`) — VM SKU → hourly price
   in the configured Azure region

**Cost formula:**
```
dbu_cost = executor_run_time_hours * dbu_per_node * dbu_price_per_hour
vm_cost  = executor_run_time_hours * vm_price_per_hour * num_nodes
total    = dbu_cost + vm_cost
```

**Config schema:**
```toml
[burnt.pricing]
backend = "azure-databricks"

[burnt.azure_databricks]
region = "eastus"           # required
subscription_id = "..."     # required for VM pricing API
```

### Files to modify

```
src/burnt/cloud/azure_databricks/backend.py   (new)
src/burnt/cloud/azure_databricks/__init__.py  (new)
src/burnt/_config/__init__.py                 (add azure_databricks fields)
tests/unit/test_azure_backend.py              (new, uses mock azure client)
```

---

## Acceptance Criteria

- [ ] `AzureDatabricksBackend` implements the `PricingBackend` protocol from P3-02
- [ ] `AzureDatabricksBackend.estimate(session_state, config)` returns `CostEstimate`
  with `usd`, `backend = "azure-databricks"`, `confidence = "high"`
- [ ] Without `[azure-databricks]` installed: `import burnt.cloud.azure_databricks`
  raises `NotAvailableError` with install hint
- [ ] `pip install burnt[azure-databricks]` installs `databricks-sdk`, `azure-mgmt-compute`,
  `azure-mgmt-billing` and nothing else beyond core
- [ ] Unit tests mock both the Databricks SDK and Azure mgmt clients
- [ ] `uv run pytest tests/unit/test_azure_backend.py -v` passes

## Verification

```bash
uv run pytest tests/unit/test_azure_backend.py -v

# Graceful absence (without extra installed):
python -c "from burnt.cloud.azure_databricks import backend" 2>&1 | grep NotAvailableError
```
