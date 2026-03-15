<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="public/logo_text_dark.svg">
  <img src="public/logo_text.svg" alt="burnt" width="400">
</picture>

**Pre-Orchestration FinOps & Cost Estimation for Databricks**

Project job and query costs _before_ you run them.

[![Tests](https://img.shields.io/badge/tests-406%20passing-brightgreen)](https://github.com/anomalyco/burnt/actions)
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

*   **End-of-Notebook Advisor:** Run `burnt.advise_current_session()` after developing in a notebook to get a recommended cluster config, cost comparison (All-Purpose → Jobs Compute → Serverless → Spot), and Databricks API JSON to paste into your job definition.
*   **What-If Simulation:** Model cost scenarios fluently — `estimate.simulate().cluster().enable_photon().compare()` — with calibrated multipliers and verified/estimated flags.
*   **4-Tier Estimation Pipeline:** Static analysis → Delta metadata (`DESCRIBE DETAIL`) → `EXPLAIN COST` → Historical fingerprints. Each tier adds accuracy with graceful fallback.
*   **Anti-Pattern Detection:** AST-based SQL/PySpark linting for expensive patterns (CROSS JOIN, collect without limit, Python UDFs, etc.).
*   **System Table Analytics:** Tag-based cost attribution, anomaly detection, idle cluster alerting, warehouse scaling analysis — all via `system.*` tables.

---

## Installation

```bash
git clone https://github.com/your-org/burnt
cd burnt
uv sync
```

---

## Quick Start

### 1. End-of-Notebook Advisor (Flagship)
After developing in a Databricks notebook, get a cluster recommendation with cost breakdown:
```python
import burnt
advice = burnt.advise_current_session()
advice.display()
# → "Switch to Standard_DS3_v2 Jobs Compute with 3 workers.
#    Cost drops from $45/run to $12/run. Peak memory was 14% — over-provisioned."

# Get Databricks API JSON to paste directly into your job definition
print(advice.recommended.to_api_json())

# Explore simulation scenarios from the advisory
advice.simulate().cluster().enable_photon().compare().display()
```

### 2. SQL / PySpark Cost Estimation
Check for anti-patterns and estimate query cost offline:
```bash
uv run burnt check "SELECT * FROM a CROSS JOIN b"
uv run burnt check ./notebooks/
```

Or estimate programmatically with simulation:
```python
estimate = burnt.estimate("SELECT customer_id, SUM(amount) FROM orders GROUP BY 1")
result = (
    estimate.simulate()
    .cluster().enable_photon()
    .data_source().enable_liquid_clustering(["customer_id"])
    .compare()
)
result.display()
```

### 3. Cost Attribution & FinOps
```python
# Tag-based cost breakdown
report = burnt.cost_by_tag("team", days=30)
report.display()

# Detect idle all-purpose clusters
idle = burnt.detect_idle_clusters()
for cluster in idle:
    print(f"{cluster.cluster_name}: {cluster.idle_cost_usd:.2f} USD wasted")

# Check environment health
# (from CLI)
# uv run burnt doctor
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

## Dual-Mode Runtime

`burnt` automatically detects its execution context:

| Context | Backend | Auth |
|---------|---------|------|
| Inside Databricks notebook | **SparkBackend** | Auto (SparkSession) |
| External with DATABRICKS_HOST | **RestBackend** | OAuth/PAT via SDK |
| Offline / CLI | Static only | None |

```python
import burnt
# Auto-detects: in-cluster → SparkBackend, external → RestBackend, offline → static
backend = burnt.runtime.auto_backend()
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

| Sprint | Status | Focus |
|--------|--------|-------|
| 1 | ✅ Done | RuntimeBackend, instance catalog, `advise_current_session()` |
| 2 | 🔄 In progress | Display mixin ✓, cost guard (partial) ✓; CLI redesign, `burnt doctor`, offline mode fix, `ClusterProfile` pending |
| 3 | 📋 Planned | Estimation accuracy — partition pruning, spill risk, photon eligibility, `EstimationTrace` |
| 4 | 📋 Planned | Production hardening — error handling, caching, cost anomaly detection, tag attribution |
| 4.5 | 📋 Planned | ML research spike — model selection, forecast target, training data requirements |
| 5 | 📋 Planned | ML models — transfer function, cost regressor, calibration loop, streaming projection |
| 6 | 📋 Planned | Analytics — schema impact, commitment advisor, DLT decomposition, query regression, storage tiering |

### What's Implemented Today

| Feature | API |
|---------|-----|
| End-of-notebook advisor | `burnt.advise_current_session()` |
| Historical run analysis | `burnt.advise(run_id=…)` |
| SQL/PySpark anti-pattern detection | `burnt.lint()`, `burnt check` (CLI) |
| Static + hybrid cost estimation | `burnt.estimate(sql)` |
| What-if simulation builder | `estimate.simulate().cluster().enable_photon().compare()` |
| Cluster right-sizing | `burnt.right_size(profile)` |
| Databricks API JSON output | `cluster_config.to_api_json()` |
| Cost guard (circuit breaker) | `estimate.raise_if_exceeds(50.0)` |
| Display mixin / markdown export | `estimate.to_markdown()`, `result.to_markdown()` |
| Dual-mode runtime | Auto-detects SparkBackend / RestBackend / offline |
| 4-tier estimation pipeline | Static → Delta → EXPLAIN → Historical fingerprints |
| System table clients | billing, query history, compute, attribution |
| Azure instance catalog | 23 VM types, DBU rates, right-sizer |
| What-if cost multipliers | Photon, spot, serverless, Delta, Liquid Clustering, AQE |

For architecture details, research findings, and full sprint specs, see [`DESIGN.md`](DESIGN.md).

---

## Contributing & Development

We use `uv` for fast package management.

```bash
uv run pytest -m unit -v          # 388 unit tests
uv run ruff check src/ tests/     # lint
uv run ruff format src/ tests/    # format
uv run bandit -c pyproject.toml -r src/  # security audit
```