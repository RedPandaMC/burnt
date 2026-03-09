# burnt — Usage Guide

> The Data Engineer's best friend. Optimize, advise, and estimate costs for Databricks workflows before you deploy them to production.

---

## Installation

```bash
# Clone and install
git clone https://github.com/your-org/burnt
cd burnt
uv sync

# Verify
uv run burnt --help
```

---

## Programmatic API (Python)

`burnt` is built to be used natively inside your Databricks Notebooks or local data pipelines. 

```python
import burnt

# 1. Cost-Aware Linting
# Catch expensive anti-patterns before they cause OOMs or explode your bill.
issues = burnt.lint("SELECT * FROM orders CROSS JOIN customers")
for issue in issues:
    print(f"[{issue.severity}] {issue.pattern}: {issue.description}")

# 2. The Interactive Advisor (Coming Soon)
# Pass an interactive test run ID, get production cluster recommendations.
advice = burnt.advise(run_id="1234567890")
print(f"Recommended Cluster: {advice.recommended_cluster}")
print(f"Estimated Savings: {advice.estimated_savings_usd}/run")

# 3. Cost Estimation
# Estimate ad-hoc SQL without executing it
cost = burnt.estimate("SELECT customer_id, SUM(amount) FROM orders GROUP BY 1")
print(f"Estimated Cost: ${cost.estimated_cost_usd:.4f} ({cost.confidence})")
```

---

## Command Line Interface (CLI)

`burnt` provides a powerful CLI for CI/CD gates and local development.

### 1. Cost-Aware Linting
Detect single-partition bottlenecks, driver memory risks, and unoptimized joins.

```bash
uv run burnt lint ./notebooks/
```

### 2. The Interactive Advisor (Coming Soon)
Translate a slow, expensive interactive notebook test run into a highly optimized Job cluster configuration.

```bash
uv run burnt advise --run-id 1234567890
```

### 3. Pre-Execution Cost Estimation
Estimate the DBU cost of a query or file offline.

```bash
uv run burnt estimate "SELECT customer_id, SUM(amount) FROM orders GROUP BY 1"
```

Output:
```
        Cost Estimate
┌─────────────────┬───────────┐
│ Metric          │ Value     │
├─────────────────┼───────────┤
│ Estimated DBU   │ 0.0312    │
│ Estimated Cost  │ $0.0062   │
│ Confidence      │ medium    │
│ Signal          │ static    │
└─────────────────┴───────────┘
```

---

## Hybrid Estimation (with Databricks connection)

When you connect to your workspace, `burnt` uses **EXPLAIN COST**, **Delta log metadata**, and **query history** for significantly more accurate estimates and advice.

### Setup

```bash
export BURNT_WORKSPACE_URL=https://adb-1234567890.12.azuredatabricks.net
export BURNT_TOKEN=dapi...
```

Or create a `.env` file:
```ini
BURNT_WORKSPACE_URL=https://adb-1234567890.12.azuredatabricks.net
BURNT_TOKEN=dapi...
```

### Run with EXPLAIN COST

```bash
uv run burnt estimate "SELECT ..." --warehouse-id sql-warehouse-abc123
```

This will:
1. Fingerprint your SQL and look up matching historical executions.
2. Run `EXPLAIN COST` on the query (no data scanned).
3. Blend historical p50, EXPLAIN statistics, and static analysis.
4. Return a confidence=**high** estimate.

---

## What-If Scenarios & Orchestration

Model the cost impact of infrastructure changes:

```bash
# How much cheaper would Photon make this?
uv run burnt whatif "SELECT ..." --scenario photon

# What if we migrate to serverless SQL?
uv run burnt whatif "SELECT ..." --scenario serverless --utilization 60

# Resize cluster
uv run burnt whatif "SELECT ..." --scenario resize
```

---

## Enterprise Governance 

If your organization hides `system.*` tables behind curated views with row-level security, `burnt` supports customizable table registries.

**Programmatic Override:**
```python
import burnt
from burnt.core.table_registry import TableRegistry

registry = TableRegistry(
    billing_usage="governance.cost_management.v_billing_usage",
    query_history="governance.observability.v_query_history"
)

cost = burnt.estimate("SELECT ...", registry=registry)
```

**Environment Variables:**
```bash
export BURNT_TABLE_BILLING_USAGE="governance.cost_management.v_billing_usage"
```
