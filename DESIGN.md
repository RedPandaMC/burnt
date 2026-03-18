# burnt Design Document

> The data engineer's tool for bridging the gap between interactive development and production orchestration.

---

## Table of Contents

1. [Vision](#vision)
2. [Current Status](#current-status)
3. [Sprint Roadmap](#sprint-roadmap)
4. [Architecture](#architecture)
5. [Top-Level Python API](#top-level-python-api)
6. [CLI](#cli)
7. [Azure Instance Catalog](#azure-instance-catalog)
8. [Pricing Model](#pricing-model)
9. [Estimation Architecture](#estimation-architecture)
10. [Runtime Backend](#runtime-backend)
11. [Key Research Findings](#key-research-findings)
12. [Cost Optimization Scenarios](#cost-optimization-scenarios)
13. [Fluent Builder Design](#fluent-builder-design)
14. [System Tables Reference](#system-tables-reference)
15. [Enterprise Support](#enterprise-support)
16. [Verification \& Quality](#verification--quality)
17. [Design Principles](#design-principles)

---

## Vision

A data scientist finishes testing a notebook on an All-Purpose cluster. Before scheduling it as a Job, they run one cell:

```python
import burnt
advice = burnt.advise_current_session()
advice.display()
```

They see: "Switch to `Standard_DS3_v2` Jobs Compute with 3 workers. Estimated cost drops from $45/run to $12/run. Peak memory was 14% — you're over-provisioned." They copy the Databricks API JSON, paste it into their Job definition, and deploy. That's the product.

**Three capabilities, one workflow:**

1. **Optimize code** — lint SQL/PySpark for anti-patterns that spike DBUs or OOM the driver
2. **Predict costs** — estimate what a query or notebook will cost before it runs
3. **Suggest cluster configurations** — recommend instance type, worker count, Photon, spot policy — output Databricks API-compatible JSON

**Design principle:** "Cuts like butter." Fluent interfaces, smart defaults, zero configuration for the common case, progressive depth when you need it.

---

## Current Status

| Component | State | Notes |
|-----------|-------|-------|
| Anti-pattern detection (`check`) | **Working** | AST-based via sqlglot, CLI + API |
| Static cost estimation | **Working** | Linear throughput model |
| EstimationPipeline (4-tier) | **Scaffolded** | Tiers wired but no backend to feed them |
| EXPLAIN COST parser | **Working** | Parses plan text into structured `ExplainPlan` |
| Delta metadata parser | **Working** | `DESCRIBE DETAIL` → `DeltaTableInfo` |
| Query fingerprinting | **Working** | SHA-256 normalization pipeline |
| System table clients | **Working** | billing, queries, compute modules |
| Simulation scenarios | **Working** | Fluent `Simulation` builder with cluster/data_source/spark_config contexts |
| RuntimeBackend | **Working** | SparkBackend + RestBackend with auto_backend() detection |
| Instance catalog | **Working** | 23 Azure VM types, DBU rates, `get_cluster_json()` |
| `advise_current_session()` | **Working** | Analyzes current SparkSession and recommends cluster config |
| Data source scenarios | **Working** | Delta format, Liquid Clustering, caching, partitioning |
| Spark config scenarios | **Working** | Shuffle partitions, AQE, broadcast thresholds |
| Comparison groups | **Working** | Side-by-side multi-scenario comparison |
| Cost transparency | **Working** | Verified vs estimated multiplier flags |
| Cluster right-sizer | **Working** | `get_cluster_json()` outputs Databricks API JSON |
| Benchmark dataset | **Working** | 5 SQL queries, expected_costs.json, monotonicity + Hypothesis tests |
| CLI redesign | **Pending** | `check`/`init`/`tutorial`/`cache`/`rules`/`doctor`/`generate-policy` — see s2-05a |
| Simulation API | **Working** | `Simulation` builder, `.simulate()` entry point — see s2-05a |
| `burnt doctor` | **Pending** | Permission audit for 7 system tables — see s2-08 |
| `ClusterConfig.from_databricks_json()` | **Pending** | Jobs API JSON → ClusterConfig — see s2-11 |
| `ClusterProfile` | **Pending** | Extended cluster context (spark_version, tags, pool) — see s2-12 |
| Display mixin + `.to_markdown()` | **Working** | `_DisplayMixin` base class; `.display()`, `.to_markdown()`, `.comparison_table()` — see s2-06 |
| Cost guard — `raise_if_exceeds()` | **Partial** | `raise_if_exceeds()` implemented on `CostEstimate`; `warn_if_exceeds()` pending — see s2-07 |
| Partition pruning | **Pending** | WHERE clause × partition selectivity — see s3-04 |
| Spill risk scoring | **Pending** | Cluster memory vs shuffle bytes — see s3-05 |
| Photon eligibility scoring | **Pending** | AST-based 0–100 score — see s3-06 |
| EstimationTrace | **Pending** | Tier-by-tier transparency on CostEstimate — see s3-09 |
| Cost anomaly detection | **Pending** | MAD-based billing anomalies — see s4-05 |
| Tag cost attribution | **Pending** | `cost_by_tag()` — see s4-06 |
| Idle cluster alerting | **Pending** | CPU threshold + auto-termination advice — see s4-07 |
| ML architecture (3-layer) | **Pending** | Transfer function + regressor + calibration — see s5-00 to s5-04 |
| Sprint 6 analytics features | **Pending** | 10 new capabilities — see s6-01 to s6-10 |

**Lint:** clean | **Security:** bandit clean

### What Works Today

```python
# The flagship workflow
import burnt
advice = burnt.advise_current_session()
advice.display()

# Simulation (what-if) scenarios — fluent builder
estimate = burnt.estimate("SELECT customer_id, SUM(amount) FROM orders GROUP BY 1")
result = estimate.simulate().cluster().enable_photon().compare()

# Data source optimization
result = (
    estimate.simulate()
    .data_source().to_delta_format().enable_liquid_clustering(["date"]).compare()
)

# Multiple scenarios
result = (
    estimate.simulate()
    .cluster().enable_photon()
    .scenarios([
        ("Downsize", lambda b: b.cluster().to_instance("DS3_v2")),
        ("Full", lambda b: b.cluster().to_instance("DS3_v2").data_source().to_delta_format()),
    ])
    .compare()
)
result.display()

# Cluster right-sizing
profile = burnt.WorkloadProfile(peak_memory_pct=50.0)
print(burnt.get_cluster_json(profile))  # → Databricks API JSON
```

### Still Pending

```bash
# Environment health check — s2-08 (stub only)
burnt doctor

# Not yet implemented
burnt.ClusterConfig.from_databricks_json(...)  # s2-11
burnt.cost_by_tag("team", days=30)             # s4-06
burnt.detect_idle_clusters()                    # s4-07
```

---

## Sprint Roadmap

The old task graph had 5 serial dependencies before the flagship feature could start. This roadmap short-circuits that chain: build the user-facing features first, then wire the estimation accuracy improvements behind them.

### Sprint 1: The Core Loop

> **Goal:** Get `advise_current_session()` working end-to-end.

| Task | File | What |
|------|------|------|
| `s1-01` | RuntimeBackend | `SparkBackend` + `RestBackend` + `auto_backend()` detection |
| `s1-02` | Azure Instance Catalog | 22+ VM types, DBU rates, categories, `right_size()` |
| `s1-03` | advise + CLI | `advise_current_session()`, `advise(run_id=)`, `burnt advise` CLI |

**Acceptance:** A data engineer can run `burnt.advise_current_session()` in a Databricks notebook and see the Compute Migration Analysis table with cost comparisons and a cluster recommendation with API JSON.

### Sprint 2: The Developer Experience

> **Goal:** Lint rules, benchmark infrastructure, CLI/API redesign, and UX foundations.

| Task | What |
|------|------|
| `s2-03` | ✅ Benchmark dataset — 5 queries, expected costs, monotonicity + Hypothesis tests |
| `s2-04` | AST lint rules — 7 of 12 rules done, 5 remaining |
| ✅ `s2-05a` | CLI/API redesign — `check`/`init`/`tutorial`/`cache`/`rules`; `Simulation` builder |
| ✅ `s2-06` | Display mixin — `_DisplayMixin` base class, `.to_markdown()`, progress feedback |
| 🔄 `s2-07` | Cost guard — `raise_if_exceeds()` implemented; `warn_if_exceeds()` pending |
| `s2-08` | `burnt doctor` — environment health check, system table permission audit |
| `s2-09` | `--output json` flag on `check` command |
| `s2-10` | Offline mode fix — suppress dollar amounts when no backend; show complexity only |
| `s2-11` | `ClusterConfig.from_databricks_json()` factory method |
| `s2-12` | `ClusterProfile` — extended cluster config with `spark_version`, `custom_spark_conf`, tags |
| `s2-13` | Advisory → Simulation integration — `AdvisoryReport.simulate()` passes real metrics |

**Acceptance:** `burnt check ./src/` lints for anti-patterns. Offline mode shows complexity + warnings without dollar amounts. `ClusterConfig.from_databricks_json()` round-trips correctly.

### Sprint 3: Estimation Accuracy

> **Goal:** Wire all 4 estimation tiers; add accuracy-critical estimators.

| Task | What |
|------|------|
| `s3-01` | Delta scan integration — `DESCRIBE DETAIL` feeds scan size into estimator |
| `s3-02` | Fingerprint lookup + fuzzy match — AST edit distance ≤ 3; lower confidence weight |
| `s3-03` | Pipeline hardening — total cost (DBU + VM), confidence calibration, signal logging |
| `s3-04` | Partition pruning estimation — WHERE clause selectivity × scan bytes |
| `s3-05` | Spill risk scoring — `spill_ratio = shuffle_bytes / executor_memory`; pool-aware warnings |
| `s3-06` | Photon eligibility scoring — AST-based 0–100 score calibrates `enable_photon()` multiplier |
| `s3-07` | OPTIMIZE/VACUUM cost estimation — file rewrite cost + storage savings |
| `s3-08` | Notebook cell-level cost breakdown — hotspot detection, PySpark action attribution |
| `s3-09` | `EstimationTrace` — tier-by-tier transparency attached to `CostEstimate` |

**Acceptance:** Connected-mode estimates use Delta metadata + EXPLAIN + history. Partition-filtered queries show accurate scan fractions. Spill risk appears as a warning on undersized clusters.

### Sprint 4: Production Hardening

> **Goal:** Make it reliable for daily use in enterprise notebooks and CI/CD.

| Task | What |
|------|------|
| `s4-01` | Error handling — graceful failures, typed exceptions, helpful messages |
| `s4-02` | Caching + perf — event-driven cache invalidation via `DESCRIBE HISTORY`; TTL fallback |
| `s4-03` | Observability — structured logging, `--debug` flag, timing metrics |
| `s4-04` | Table registry — enterprise governance view mapping |
| `s4-05` | Cost anomaly detection — MAD-based z-score on billing data; dual-input API |
| `s4-06` | Tag cost attribution — `cost_by_tag(tag_key, days)`; untagged bucket reporting |
| `s4-07` | Idle cluster alerting — CPU threshold check; auto-termination recommendations |

**Acceptance:** No unhandled exceptions. Event-driven cache invalidation prevents stale estimates after table mutations. `cost_by_tag()` and `detect_anomalies()` work with direct or pre-queried billing data.

### Sprint 4.5: ML Research Phase

> **Goal:** Answer open questions before committing to an ML architecture.

| Task | What |
|------|------|
| `s4p5-01` | ML research — fingerprint recurrence rate, forecast target, model selection, training data requirements, structural break handling |

**Output:** Written research report in `docs/ml-research-report.md`; DESIGN.md ML section updated with decisions.

### Sprint 5: ML Models & Cost Projection

> **Goal:** Three-layer ML architecture; push accuracy from 3× to 2×.

| Task | What |
|------|------|
| `s5-00` | Listener infrastructure — SparkMeasure integration for session metrics |
| `s5-01` | Feature extraction — ExplainPlan + Delta + cluster → normalised feature vector |
| `s5-02` | Transfer function — physics-based source→target cluster scaling (Amdahl's Law) |
| `s5-03` | Cost regressor — `HistGradientBoostingRegressor` on `log1p(dbu)` |
| `s5-04` | Cost projection + calibration — per-run DBU → daily/monthly/yearly; EMA bias correction |
| `s5-05` | Streaming cost projection — `cost_per_hour` + `cost_per_gb`; growth scenarios |
| `s5-06` | UC lineage cost propagation — downstream impact of table growth via Lineage API |

**Architecture:** Layer 1 (base model) + Layer 2 (transfer function) + Layer 3 (calibration loop). See `tasks/r7-ml-architecture-v4.md`.

**Acceptance:** `advise_current_session()` returns `JobCostProjection` with daily/monthly/yearly breakdown. Streaming jobs get `StreamingCostProjection`.

### Sprint 6: Net-New Analytics Capabilities

> **Goal:** New analytical features beyond estimation — governance, FinOps, and proactive cost intelligence.

| Task | What |
|------|------|
| `s6-01` | Schema change cost impact — downstream query cost delta from column adds/drops/renames |
| `s6-02` | Cluster policy generator — cost-ceiling → Databricks Cluster Policy JSON (offline) |
| `s6-03` | Freshness vs cost tradeoff — wasted runs when consumers read less often than job runs |
| `s6-04` | Cross-workspace benchmarking — per-workspace efficiency score (0–100) with 5 metrics |
| `s6-05` | Commitment plan advisor — DCU breakeven analysis; 1yr vs 3yr with under-utilisation risk |
| `s6-06` | DLT pipeline cost decomposition — per-table Bronze/Silver/Gold cost attribution |
| `s6-07` | Warehouse auto-scaling analysis — idle cluster-hours, queue time, min/max recommendations |
| `s6-08` | Spot interruption cost model — historical preemption rate → realistic spot TCO |
| `s6-09` | Query regression detector — fingerprint-based regression with probable cause heuristics |
| `s6-10` | Storage tiering recommendations — cold table detection; cool/archive savings estimate |

### Accuracy Targets by Sprint

| Sprint | Target | Method |
|--------|--------|--------|
| 1-2 | Within **10×** of actual | Static + what-if heuristics |
| 3 | Within **3×** of actual | Full pipeline with Delta + EXPLAIN + history |
| 5 | Within **2×** of actual | Three-layer ML model trained on historical data |

---

## Architecture

### The Core Loop: Interactive to Production

```mermaid
flowchart LR
    subgraph IDEV[Interactive Development]
        NB[Notebook Cell]
        AD[Advisory Report]
        COST[Cost Table]
        TIP[Downsize Tip]
        JSON[API JSON]
    end
    
    subgraph PORD[Production]
        PROD[Optimized Job]
    end
    
    NB --> AD
    AD --> COST
    COST --> TIP
    TIP --> JSON
    JSON --> PROD
    
    style IDEV fill:#333333,stroke:#333333,color:#fff
    style PORD fill:#333333,stroke:#333333,color:#fff
    style AD fill:#ffffff,stroke:#333333
    style JSON fill:#ffffff,stroke:#333333
```

### Package Structure

```
src/burnt/
├── __init__.py              # Top-level API: estimate, lint, advise, right_size, cost_by_tag, …
├── _compat.py               # Optional import helpers
├── core/
│   ├── models.py            # Pydantic models (CostEstimate, ClusterConfig, ClusterProfile, …)
│   ├── config.py            # pydantic-settings with env vars
│   ├── pricing.py           # DBU rate lookups by SKU
│   ├── exchange.py          # USD/EUR/GBP/JPY/CNY via frankfurter.app
│   ├── protocols.py         # Protocol classes for extensibility
│   ├── table_registry.py    # Enterprise governance view mapping
│   ├── instances.py         # Azure instance catalog + right-sizer
│   ├── policy.py            # Cluster policy generator (s6-02)
│   ├── _display.py          # _DisplayMixin base class (s2-06)
│   └── _progress.py         # Progress feedback during estimation (s2-06)
├── runtime/
│   ├── __init__.py
│   ├── backend.py           # Backend protocol
│   ├── spark_backend.py     # In-cluster via SparkSession
│   ├── rest_backend.py      # External via REST API + PAT
│   └── auto.py              # auto_backend() detection
├── parsers/
│   ├── sql.py               # SQLGlot analysis
│   ├── pyspark.py           # Python AST analysis
│   ├── notebooks.py         # .ipynb, .dbc, .py (COMMAND-delimited) parsing
│   ├── antipatterns.py      # Anti-pattern detection
│   ├── explain.py           # EXPLAIN COST parser
│   └── delta.py             # DESCRIBE DETAIL parser
├── tables/
│   ├── billing.py           # system.billing.*
│   ├── queries.py           # system.query.history + fingerprinting + fuzzy match
│   ├── compute.py           # system.compute.*
│   ├── attribution.py       # Cost attribution joins
│   ├── connection.py        # DatabricksClient (connection pooling + event-driven cache)
│   ├── anomaly.py           # detect_anomalies() — MAD-based (s4-05)
│   ├── tags.py              # cost_by_tag() — tag attribution (s4-06)
│   ├── idle.py              # detect_idle_clusters() (s4-07)
│   ├── lineage.py           # estimate_lineage_impact() via UC Lineage API (s5-06)
│   ├── schema.py            # schema_impact() (s6-01)
│   ├── benchmark.py         # workspace_benchmark() (s6-04)
│   ├── commitment.py        # commitment_advisor() (s6-05)
│   ├── dlt.py               # dlt_decomposition() (s6-06)
│   ├── warehouse.py         # warehouse_scaling_analysis() (s6-07)
│   ├── spot.py              # spot_analysis() (s6-08)
│   ├── regression.py        # detect_query_regressions() (s6-09)
│   ├── storage.py           # storage_analysis() (s6-10)
│   └── freshness.py         # freshness_analysis() (s6-03)
├── estimators/
│   ├── static.py            # Complexity-based offline estimation
│   ├── hybrid.py            # Blended EXPLAIN + Delta + history
│   ├── pipeline.py          # 4-tier orchestrator + EstimationTrace
│   ├── simulation.py        # Simulation + cluster/data_source/spark_config builders
│   ├── partition.py         # Partition pruning selectivity (s3-04)
│   ├── spill.py             # Spill risk scoring (s3-05)
│   ├── photon.py            # Photon eligibility scorer (s3-06)
│   ├── streaming.py         # Streaming cost projection (s5-05)
│   ├── projection.py        # Job cost projection daily/monthly/yearly (s5-04)
│   ├── transfer.py          # Source→target cluster transfer function (s5-02)
│   ├── ml.py                # HistGBR cost regressor (s5-03)
│   └── calibration.py       # EMA bias correction (s5-04)
├── advisor/
│   ├── __init__.py
│   ├── session.py           # advise_current_session() implementation
│   └── report.py            # AdvisoryReport model + display
├── forecast/
│   └── prophet.py           # Time-series forecasting (model TBD via s4p5-01 research)
└── cli/
    └── main.py              # Typer CLI: check, init, tutorial, cache, rules, doctor, generate-policy
```

### Interaction Modes

burnt operates in 5 modes. The backend is the only variable — all features are available everywhere.

| # | Mode | Where | Backend | Auth |
|---|------|-------|---------|------|
| 1 | Local CLI, offline | Laptop / CI | Static only | None |
| 2 | Local CLI + Databricks | Laptop | `RestBackend` | PAT |
| 3 | Databricks CLI / job | DB terminal | `SparkBackend` | Auto |
| 4 | Notebook, external | Notebook → other files | `SparkBackend` | Auto |
| 5 | Notebook, self | Notebook → itself | `SparkBackend` + path detection | Auto |

### Feature Parity Matrix

| Feature | Mode 1 | Mode 2 | Mode 3 | Mode 4 | Mode 5 |
|---------|:------:|:------:|:------:|:------:|:------:|
| `estimate("SELECT ...")` | Static | Hybrid | Hybrid | Hybrid | Hybrid |
| `estimate_file("file.sql")` | Static | Hybrid | Hybrid | Hybrid | Hybrid |
| `lint()` / `lint_file()` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `advise(run_id=)` | — | ✓ | ✓ | ✓ | ✓ |
| `advise_current_session()` | — | — | ✓ | — | ✓ |
| `right_size(profile)` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `estimate.simulate()` builder | ✓ | ✓ | ✓ | ✓ | ✓ |
| `.to_api_json()` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `.display()` rich output | — | — | — | — | ✓ |

---

## Top-Level Python API

The public API is designed around three workflows that data engineers actually use.

### Workflow 1: End-of-Notebook Advisor (Flagship)

```python
import burnt

# Analyzes queries you just ran in this SparkSession
advice = burnt.advise_current_session()
advice.display()  # Rich HTML table in notebook UI

# Get the Databricks API JSON for the recommended cluster
print(advice.recommended.to_api_json())

# Explore simulation scenarios from the advice
advice.simulate().enable_photon().use_spot().compare().display()
```

### Workflow 2: Circuit Breaker

```python
import burnt

sql = generate_dynamic_query(start_date, end_date)
estimate = burnt.estimate(sql)

if estimate.estimated_cost_usd > 50.00:
    notify_slack(f"Query blocked! Est: ${estimate.estimated_cost_usd}")
    raise CostLimitExceeded(estimate)

spark.sql(sql)
```

### Workflow 3: Cost-Aware Testing

```python
# test_pipeline_costs.py
import burnt

def test_daily_job_under_budget():
    cost = burnt.estimate_file("src/jobs/daily_agg.py")
    assert cost.estimated_cost_usd < 5.00

def test_no_oom_risks():
    issues = burnt.lint_file("src/jobs/daily_agg.py")
    errors = [i for i in issues if i.severity == "error"]
    assert len(errors) == 0
```

### Fluent Simulation Builder

The simulation builder chains naturally with explicit context methods for cluster, data source, and spark config scenarios:

```python
estimate = burnt.estimate("SELECT ...")

# Single scenario - method chaining
result = estimate.simulate().cluster().enable_photon().compare()
result.display()  # "Photon: $12.50 → $8.30 (-34%), requires 2.7× speedup to break even"

# Data source optimization (Delta, Liquid Clustering, caching)
result = (
    estimate.simulate()
    .data_source()
    .to_delta_format()
    .enable_liquid_clustering(keys=["date", "customer_id"])
    .enable_disk_cache()
    .compare()
)
result.display()  # Shows cost transparency with verified/estimated flags

# Spark config optimization
result = (
    estimate.simulate()
    .spark_config()
    .with_shuffle_partitions(200)
    .with_aqe_enabled()
    .compare()
)
result.display()

# Multiple scenarios with top-level modifications applied to all
result = (
    estimate.simulate()
    .cluster().enable_photon()  # Applied to ALL scenarios including Baseline
    .scenarios([
        ("Downsize", lambda b: b.cluster().to_instance("DS3_v2")),
        ("Full", lambda b: (
            b.cluster().to_instance("DS3_v2")
            .data_source().to_delta_format()
        )),
    ])
    .compare()
)
result.display()
# Result: 3 scenarios (Baseline with photon, Downsize+photon, Full)

# Multiple scenarios with no top-level mods (baseline auto-added)
result = (
    estimate.simulate()
    .scenarios([
        ("Photon Only", lambda b: b.cluster().enable_photon()),
        ("Full", lambda b: b.cluster().enable_photon().to_instance("DS3_v2").data_source().to_delta_format()),
    ])
    .compare()
)
result.display()

# Using 3-letter aliases
from burnt.simulate.aliases import wrap
result = wrap(estimate.simulate()).clsr.enable_photon
```

**Key design points:**

- **Entry point**: `estimate.simulate()` → returns `Simulation` builder
- **Explicit contexts**: `.cluster()`, `.data_source()`, `.spark_config()` for clear category separation
- **Cost transparency**: Each modification shows `is_verified` flag with source reference
- **Discovery**: `.options()` prints available options directly to console
- **Multi-scenario comparison**: `.scenarios([...])` or `.add_scenario(name)` with top-level mods applied to all
- **Baseline auto-added**: First scenario is always "Baseline"
- **3-letter aliases**: `clsr`, `data`, `conf` via `wrap()` from `burnt.simulate.aliases`

### Cluster Right-Sizing with API JSON

```python
import burnt

# Right-size from a workload profile
profile = burnt.WorkloadProfile(peak_memory_pct=14.0, peak_cpu_pct=30.0)
cluster = burnt.right_size(profile)
print(cluster.to_api_json())  # ← Paste directly into Databricks Job definition
```

**Databricks API JSON output:**
```json
{
  "new_cluster": {
    "spark_version": "15.4.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 3,
    "spark_conf": {},
    "azure_attributes": {
      "availability": "SPOT_WITH_ON_DEMAND_FALLBACK"
    },
    "autoscale": {
      "min_workers": 2,
      "max_workers": 4
    }
  }
}
```

---

## CLI

The CLI is for **static analysis and tooling only**. Cost estimation, simulation, and advising are Python API features.

```bash
# Check for anti-patterns (renamed from lint)
burnt check ./notebooks/
burnt check ./src/jobs/daily_agg.py --fail-on warning

# Project setup
burnt init                 # creates .burnt.yaml, updates .gitignore
burnt tutorial             # generates examples/ notebooks

# Cache management
burnt cache show
burnt cache clear

# Lint rules management
burnt rules                # interactive TUI to enable/disable rules

# Version
burnt --version
```

**Commands removed from CLI** (use Python API instead):
- `burnt estimate` → `burnt.estimate("SELECT ...")`
- `burnt advise` → `burnt.advise_current_session()` / `burnt.advise(run_id=...)`
- `burnt simulate` → `estimate.simulate().cluster().enable_photon().compare()`

---

## Azure Instance Catalog

The instance catalog is the backbone of cluster right-sizing. Azure-only for v0.1 (user-specified constraint).

### Representative Instances

| Instance | vCPU | Memory GB | DBU/hr | Photon DBU/hr | Category | VM $/hr |
|----------|------|-----------|--------|---------------|----------|---------|
| Standard_DS3_v2 | 4 | 14 | 0.75 | 1.875 | General | $0.293 |
| Standard_DS4_v2 | 8 | 28 | 1.50 | 3.75 | General | $0.585 |
| Standard_DS5_v2 | 16 | 56 | 3.00 | 7.50 | General | $1.170 |
| Standard_D16s_v3 | 16 | 64 | 3.00 | 7.50 | General | $0.768 |
| Standard_E8s_v3 | 8 | 64 | 1.50 | 3.75 | Memory | $0.504 |
| Standard_E16s_v3 | 16 | 128 | 3.00 | 7.50 | Memory | $1.008 |
| Standard_F8s_v2 | 8 | 16 | 1.50 | 3.75 | Compute | $0.338 |
| Standard_F16s_v2 | 16 | 32 | 3.00 | 7.50 | Compute | $0.677 |
| Standard_L8s_v3 | 8 | 64 | 1.50 | 3.75 | Storage | $0.624 |
| Standard_L16s_v3 | 16 | 128 | 3.00 | 7.50 | Storage | $1.248 |

Full catalog: 22+ types across 4 categories (general, memory, compute, storage). Source: `system.compute.node_types` at runtime with hardcoded fallback.

### Right-Sizing Logic

```
IF peak_memory_pct < 30% AND peak_cpu_pct < 40%:
    recommend DOWNSIZE (smaller instance, fewer workers)
IF spill_to_disk_bytes > 0:
    recommend UPSIZE memory (E-series)
IF total_task_duration_ms / execution_duration_ms > 4:
    recommend SCALE OUT (more workers, shuffle-bound)
IF compute_intensity > 0.7:
    recommend C-series (F-series on Azure)
```

Three recommendation tiers: **economy** (smallest viable), **balanced** (default), **performance** (headroom for growth).

---

## Pricing Model

### DBU Rates (Azure Premium, US East, Pay-As-You-Go)

| Compute Type | $/DBU | Includes VM? |
|-------------|-------|-------------|
| Jobs Compute (Classic) | **$0.30** | No |
| All-Purpose (Classic) | **$0.55** | No |
| Serverless Jobs | **$0.45** | Yes |
| Serverless Notebooks | **$0.95** | Yes |
| SQL Serverless | **$0.70** | Yes |
| DLT Core / Pro / Advanced | $0.30 / $0.38 / $0.54 | No |

### Total Cost Formula

For classic compute: `total = (dbu_per_hour × dbu_rate × hours) + (vm_rate × node_count × hours)`

For serverless: `total = dbu_per_hour × serverless_rate × hours` (VM bundled)

This dual-bill model is critical — DBU-only estimates miss 40-60% of classic compute cost.

### Photon

2.5× DBU multiplier on Azure classic clusters. Breakeven requires 2× runtime speedup. Benchmarks show 2.7× average for complex SQL (joins, aggregations, windows), making it cost-effective for transform-heavy workloads but cost-negative for simple appends.

### Data Source Layer Cost Factors

Data source optimizations can significantly impact DBU consumption through improved I/O efficiency:

| Optimization | Cost Multiplier | Notes |
|--------------|-----------------|-------|
| **Delta vs Parquet** | 0.6-0.8x (estimated) | 30-40% savings from improved scan efficiency |
| **Liquid Clustering** | 0.3-0.5x (estimated) | 50-70% savings for queries filtering on clustering keys |
| **Disk Cache (high hit)** | 0.1-0.2x (estimated) | 80-90% savings for repeated queries |
| **Column Pruning** | 0.5-0.7x (estimated) | Reduced I/O from reading fewer columns |
| **File Skipping** | 0.5-0.8x (estimated) | Delta stats-based file skipping |

**Key insight:** Data source optimizations are **workload-dependent**. The simulation builder applies estimated multipliers with clear disclaimers about when they apply.

### Cost Transparency

The `Simulation` builder displays cost transparency inline, showing:

- **Verified multipliers**: From official Databricks benchmarks
- **Estimated multipliers**: From research and industry data
- **Break-even requirements**: E.g., "requires 2.7× speedup to break even"
- **Source citations**: Links to official documentation

---

## Estimation Architecture

### 4-Tier Pipeline

```
Tier 1: Static Analysis (always runs, < 10ms)
  └── Parse SQL/PySpark → complexity score → heuristic DBU estimate

Tier 2: Delta Metadata (if backend available, < 100ms)
  └── DESCRIBE DETAIL → exact table sizes → refine scan estimate

Tier 3: EXPLAIN COST (if warehouse available, < 2s)
  └── Optimized logical plan → sizeInBytes, rowCount, join types

Tier 4: Historical Fingerprints (if history available, < 500ms)
  └── Normalize → SHA-256 → match in system.query.history → p50/p95
```

Each tier adds accuracy but requires more runtime context. The pipeline tries all available tiers and blends signals with confidence weighting.

### Static Estimation Model

```
estimated_seconds = (scan_bytes / throughput_bps) + (shuffle_count × shuffle_overhead_s)
estimated_dbu = (estimated_seconds / 3600) × cluster_dbu_per_hour
estimated_usd = (estimated_dbu × dbu_rate) + (vm_rate × node_count × estimated_seconds / 3600)
```

### Complexity Scoring

| Operation | Weight | Rationale |
|-----------|--------|-----------|
| `MERGE INTO` | 20 | Join + scan + file rewrite |
| Cross join | 50 | O(n×m) output |
| Shuffle join | 10 | Full data redistribution |
| `GROUP BY` | 8 | Shuffle for aggregation |
| Window function | 8 | Shuffle + sort within partitions |
| `collect()` / `toPandas()` | 25 | Driver memory anti-pattern |
| Python UDF | 15 | Row-at-a-time JVM↔Python serde |
| Pandas UDF | 5 | Vectorized via Arrow |

---

## Runtime Backend

The single most important piece. Without it, the tool can't run inside Databricks notebooks — the primary use case.

### Detection Logic

```python
def auto_backend() -> Backend | None:
    """Auto-detect execution context and return appropriate backend."""
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        # Running inside Databricks — use SparkSession directly
        return SparkBackend(SparkSession.getActiveSession())
    elif os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"):
        # External with credentials — use REST API
        return RestBackend(
            host=os.environ["DATABRICKS_HOST"],
            token=os.environ["DATABRICKS_TOKEN"],
        )
    else:
        # No credentials — offline mode (static estimation only)
        return None
```

### Backend Protocol

```python
class Backend(Protocol):
    def execute_sql(self, sql: str, warehouse_id: str | None = None) -> list[dict]: ...
    def get_cluster_config(self, cluster_id: str) -> ClusterConfig: ...
    def get_recent_queries(self, limit: int = 100) -> list[QueryRecord]: ...
    def describe_table(self, table_name: str) -> DeltaTableInfo: ...
```

`SparkBackend` calls `spark.sql()` directly — no REST latency, no PAT required. `RestBackend` uses the Statement Execution API as the existing `DatabricksClient` does today.

### Current Notebook Path Detection

```python
def current_notebook_path() -> str | None:
    # 1. SparkConf — most reliable inside DBR
    spark.conf.get("spark.databricks.notebook.path", None)
    # 2. dbutils context — interactive notebooks
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    # 3. ipynbname — local Jupyter
    # 4. inspect.stack() — script context (.py job)
```

---

## Key Research Findings

### EXPLAIN COST

Spark's `EXPLAIN COST <query>` generates the optimized logical plan with per-operator `sizeInBytes` and `rowCount` without reading data. Accuracy: ~1.5× error with `ANALYZE TABLE` stats, 1000×+ without. Predictive Optimization (GA 2025) auto-runs ANALYZE on UC managed tables.

### Delta Transaction Logs

`DESCRIBE DETAIL` gives instant table-level `sizeInBytes` and `numFiles`. Delta `_delta_log` has per-file stats: `numRecords`, `minValues`/`maxValues`, `nullCount`, `size`. Cheapest scan-size signal available.

### Query Fingerprinting

Normalize → replace literals with `?` → collapse IN-lists → SHA-256 → `template_id`. Match against `system.query.history` for p50/p95 execution metrics. Three tiers: exact match, AST edit distance, embedding similarity.

### system.query.history Coverage Gap

Captures: SQL Warehouses, serverless notebooks/jobs. Does NOT capture: all-purpose clusters, classic Jobs, DLT, PySpark DataFrames. The `advise` workflow works around this by analyzing the SparkSession directly in-cluster.

### ML Cost Models

Microsoft Cleo (SIGMOD 2020): 14% median error with learned models. Twitter (IC2E 2021): 97.9% CPU prediction from raw SQL features. Classification into cost buckets > exact regression for developer UX.

### Competitive Gap

No existing tool provides pre-execution cost estimation from code + what-if modeling + reusable Python library for notebooks. Unravel/Sedai/Sync optimize retroactively — burnt operates proactively.

---

## Cost Optimization Scenarios

Based on comprehensive research into Databricks and Spark optimization patterns, the following scenarios are available in the `Simulation` builder:

### Join Strategy Optimizations

| Scenario | Cost Impact | Complexity | Category |
|----------|-------------|------------|----------|
| **Broadcast Hash Join** | High | Low | Spark Config |
| **Sort-Merge Join** | Medium | Medium | Spark Config |
| **Shuffle Hash Join** | Medium | Medium | Spark Config |
| **Join Reordering** | High | High | Spark Config |

**Modeling approach:** Check table size vs `spark.sql.autoBroadcastJoinThreshold`. Broadcast avoids shuffle (network I/O) and sorting, drastically reducing DBU consumption for join-heavy queries.

### Query Plan Hints

| Hint | Cost Impact | Category |
|------|-------------|----------|
| `/*+ BROADCAST(table) */` | High | Spark Config |
| `/*+ MERGE(table) */` | Medium | Spark Config |
| `/*+ SHUFFLE_HASH(table) */` | Medium | Spark Config |
| `/*+ COALESCE(n) */` | Low/Med | Spark Config |
| `/*+ REPARTITION(n, col) */` | Low/Med | Spark Config |

### File Format & I/O Optimizations

| Optimization | Cost Impact | Category |
|--------------|-------------|----------|
| **Parquet Row Group Size** | Medium | Data Source |
| **Delta Merge-on-Read vs Copy-on-Write** | High | Data Source |
| **Z-Ordering** | High | Data Source |
| **Compression (ZSTD)** | Low | Data Source |

### Memory & Execution Optimizations

| Optimization | Cost Impact | Category |
|--------------|-------------|----------|
| **Driver Memory** | Low | Cluster |
| **Executor Memory Overhead** | Low | Cluster |
| **Task Parallelism** | Medium | Spark Config |
| **Shuffle Partitions** | Medium | Spark Config |

### Adaptive Query Execution (AQE)

AQE is enabled by default (DBR 10.4+) and provides dynamic optimizations:

| Feature | Condition | Cost Reduction |
|---------|-----------|----------------|
| Join Conversion | Small table < threshold after shuffle | 20-40% |
| Partition Coalescing | Many small tasks (< 64MB each) | 15-30% |
| Skew Handling | Partition > 5× median size AND > 256MB | 30-50% |
| Empty Propagation | Early filter eliminates all rows | 40-80% |

**Configuration:**
- `spark.sql.shuffle.partitions = auto` (recommended)
- `spark.databricks.adaptive.autoBroadcastJoinThreshold = 30 MB` (Databricks default)
- `spark.sql.adaptive.coalescePartitions.enabled = true`

---

## Fluent Builder Design

The `Simulation` builder follows established Pythonic patterns from popular libraries.

### Why Method Chaining?

The fluent builder pattern is Pythonic and aligns with Python's Zen principles:

| Principle | Fluent Builder Alignment |
|-----------|--------------------------|
| **"Flat is better than nested"** | Chain depth linear vs nested config objects |
| **"Explicit is better than implicit"** | Method names are self-documenting |
| **"Readability counts"** | Natural language flow |
| **"Namespaces are great"** | Method names form clear namespace |

**Evidence from popular Python libraries:**
- **SQLAlchemy**: `query.filter().order_by().limit()`
- **Pandas/Polars**: `df.query().groupby().agg()`
- **Boto3**: Resource interface fluent chaining

### Method-Based vs Property-Based

burnt uses **method-based chaining** rather than property-based for:

1. **Semantic correctness** — Methods are verbs (actions), properties should be nouns
2. **Parameter support** — Methods can accept arguments like `enable_liquid_clustering(keys=[...])`
3. **IDE support** — Type hints work better with methods
4. **Clear intent** — `enable_photon()` is unambiguous vs. `.photon` which could be a getter

### Design Recommendations Applied

```python
class Simulation:
    def enable_photon(self, query_type: str = "complex_join") -> "Simulation":
        """Enable Photon optimization for the query."""
        ...

    def downsize_to(self, instance_type: str, num_workers: int | None = None) -> "Simulation":
        """Downsize cluster to specified instance type."""
        ...

    def use_spot(self, fallback: bool = True) -> "Simulation":
        """Use spot instances with on-demand fallback."""
        ...
```

---

## System Tables Reference

| Table | What | Retention | Used by |
|-------|------|-----------|---------|
| `system.billing.usage` | DBU consumption per SKU | 365 days | Cost attribution, anomaly detection, tag attribution, commitment advisor |
| `system.billing.list_prices` | Historical pricing | Indefinite | Dollar amount calculation |
| `system.query.history` | Query execution metrics | 365 days | Fingerprint lookup, regression detection, freshness analysis, storage analysis |
| `system.compute.node_types` | Instance → hardware specs | Current | Instance catalog refresh |
| `system.compute.node_timeline` | Minute-by-minute utilization | 90 days | Idle cluster detection, spot interruption analysis |
| `system.compute.warehouse_events` | SQL warehouse scale events | 90 days | Warehouse scaling analysis (s6-07) |
| `system.lakeflow.jobs` | Job metadata (SCD2) | Indefinite | Job analysis, freshness tradeoff |
| `system.lakeflow.job_run_timeline` | Per-run duration breakdown | 365 days | Job run cost attribution |
| `system.lakeflow.pipeline_event_log` | DLT update events per dataset | 365 days | DLT cost decomposition (s6-06) |
| `system.access.audit` | Workspace audit events | 365 days | Schema change detection (s6-01) |
| `{catalog}.INFORMATION_SCHEMA.TABLES` | Table sizes and types | Current | Storage tiering analysis (s6-10) |
| `{catalog}.INFORMATION_SCHEMA.COLUMNS` | Column definitions | Current | Schema change impact (s6-01) |

### Coverage Gaps

`system.query.history` captures: SQL Warehouses, serverless notebooks/jobs.
Does NOT capture: all-purpose clusters, classic Jobs, DLT pipelines, PySpark DataFrames on classic compute.

UC Lineage API tracks SQL-based lineage only. PySpark `df.read()` / `df.write()` are untracked.
Features that depend on lineage (s5-06, s6-01, s6-03) emit coverage warnings when this gap may affect results.

---

## Enterprise Support

Enterprise environments hide system tables behind governance views:

```python
# Via environment variables
burnt_TABLE_BILLING_USAGE=governance.cost_management.v_billing_usage

# Via programmatic API
registry = TableRegistry(billing_usage="governance.cost_management.v_billing_usage")
estimate = burnt.estimate("SELECT ...", registry=registry)
```

---

## Verification & Quality

```bash
uv run pytest -m unit -v              # Unit tests
uv run pytest --cov --cov-report=term  # Coverage
uv run ruff check src/ tests/          # Lint
uv run ruff format --check src/ tests/ # Format
uv run bandit -c pyproject.toml -r src/ # Security
uv run interrogate src/ -v             # Docstrings
```

---

## Design Principles

1. **Developer workflow first** — every feature serves the interactive → production transition
2. **Cuts like butter** — fluent interfaces, smart defaults, zero config for the common case
3. **Layered fidelity** — static → Delta → EXPLAIN → history → ML
4. **Total cost, not DBU-only** — include VM infrastructure for classic compute
5. **Dual-mode runtime** — SparkBackend (in-cluster) + RestBackend (external), auto-detected
6. **API JSON output** — cluster recommendations paste directly into Databricks Job definitions
7. **Empirically grounded** — all constants cite source or benchmark
8. **Graceful degradation** — always produce an estimate, even if low-confidence
9. **Explicit categorization** — cluster, data source, and spark config scenarios are clearly separated
10. **Cost transparency** — verified vs estimated multipliers shown inline

*Document version: 3.0 | March 2026 | Sprints 2–6 fully specced; Sprint 5 updated to three-layer ML architecture; Sprint 6 adds 10 net-new analytics capabilities*
