<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="public/logo_text_dark.svg">
  <img src="public/logo_text.svg" alt="burnt" width="400">
</picture>

**Pre-Orchestration FinOps & Cost Estimation for Databricks**

Project job and query costs _before_ you run them.

[![Tests](https://img.shields.io/badge/tests-263%20passing-brightgreen)](https://github.com/anomalyco/burnt/actions)
[![Python](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/)
[![Ruff](https://img.shields.io/badge/lint-ruff-purple)](https://github.com/astral-sh/ruff)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![PyPI](https://img.shields.io/badge/pypi-coming%20soon-orange)](https://pypi.org)

</div>

---

## What is this?

`burnt` is an open-source tool designed to shift Databricks cost management left. Instead of waiting for the monthly bill, `burnt` predicts the cost of **Jobs, DABs (Databricks Asset Bundles), and SQL Queries** before they run.

Industry benchmarks show that **cluster configuration drives 70% of Databricks spend**. Rather than solely relying on query-level SQL analysis, `burnt` focuses on the **Qubika Cost Multiplier Model**: analyzing cluster configuration, compute types, and historical workload data to project costs at scale.

### Core Capabilities

*   **Pre-Orchestration Estimates:** Parse `databricks.yml` and Job JSON to project minimum and maximum workload costs before deployment.
*   **Historical Baselines:** Cross-reference `system.lakeflow.jobs` and `system.billing.usage` to detect silent cost drift.
*   **Query-Level `EXPLAIN` Analytics:** For ad-hoc analytics, estimate SQL query costs utilizing Spark's `EXPLAIN COST` without touching the actual data.

---

## Installation

```bash
git clone https://github.com/your-org/burnt
cd burnt
uv sync
```

---

## Quick Start

### 1. Pre-Orchestration Job Estimation (Coming Soon)
Estimate the cost of a Databricks Asset Bundle (DAB) before deploying:
```bash
uv run burnt estimate-job ./databricks.yml
```

### 2. SQL / PySpark Cost Estimation
Estimate an individual query or file offline using static heuristics:
```bash
uv run burnt estimate "SELECT customer_id, SUM(amount) FROM orders GROUP BY 1"
uv run burnt estimate ./notebooks/daily_revenue.sql
```

### 3. Native Databricks Runtime Integration
If you are running `burnt` *inside* a Databricks notebook, it natively uses your active `SparkSession`—avoiding slow and expensive REST API roundtrips:
```python
import burnt
# Automatically detects current notebook and estimates the cost to run it
estimate = burnt.estimate_current_notebook()
burnt.display()
```

---

## Anti-Pattern Detection (burnt lint)

`burnt` automatically warns you about expensive patterns in your SQL/PySpark code via AST parsing:

```
⚠  cross_join             CROSS JOIN creates O(n×m) rows — use INNER JOIN with ON clause
⚠  order_by_no_limit      ORDER BY without LIMIT forces a global sort
✗  collect_without_limit  collect() without limit() can OOM the driver
```

---

## Architecture & Enterprise Readiness

`burnt` is built for enterprise Databricks environments:
*   **Dual-Mode Runtime:** Automatically switches between external REST execution (via PAT) and internal SparkSession execution based on `DATABRICKS_RUNTIME_VERSION`.
*   **Table Registry:** Supports customizable mapping for governance-restricted system table views (e.g., `governance.cost_management.v_billing_usage`).
*   **Hybrid Estimation Pipeline:** Blends static analysis (Offline), Delta Metadata, `EXPLAIN COST` plans, and Historical Fingerprinting.
*   **Total Cost of Ownership:** Calculates both Databricks DBU rates *and* underlying Cloud VM infrastructure costs (AWS/Azure/GCP).

---

## Roadmap

| Phase | Status | Focus |
|-------|--------|-------|
| 1-3 | ✅ Done | Foundation, System Tables, EXPLAIN parser |
| 4 | 🔄 Active | Databricks-Native Runtime & Core Math Fixes |
| 5 | ⏳ Planned | Pre-Orchestration Job Cost Projection (DABs) |
| 6 | ⏳ Planned | Query-Level Estimation Wiring |

For a complete look at our architecture and research findings, see [`DESIGN.md`](DESIGN.md).

---

## Contributing & Development

We use `uv` for fast package management.

```bash
uv run pytest -m unit -v          # 263 unit tests
uv run ruff check src/ tests/     # lint
uv run ruff format src/ tests/    # format
uv run bandit -c pyproject.toml -r src/  # security audit
```