# dburnrate Design Document

> Comprehensive design document consolidating research, architecture, and roadmap

---

## Table of Contents

1. [Research Findings](#research-findings)
2. [Architecture & Concepts](#architecture--concepts)
3. [Implementation Roadmap](#implementation-roadmap)
4. [Verification & Quality](#verification--quality)

---

## Current Status

| Phase | Status | What | Tasks |
|-------|--------|------|-------|
| 1 | ✅ Done | Tests, lint, docstrings | `tasks/p1-*.md.completed` |
| 2 | ✅ Done | Databricks system table client (billing, query history, compute) | `tasks/p2-*.md.completed` |
| 3 | ✅ Done | EXPLAIN COST parser, Delta log reader, hybrid estimator | `tasks/p3-*.md.completed` |
| 4A | 🔄 Active | Fix critical bugs (quadratic formula, phantom price, SQL injection, SKU inference) | `tasks/p4a-*.md` |
| 4B | 🔄 Active | Wire hybrid pipeline into CLI + calibration research | `tasks/p4-*.md` |
| 4C | ⏳ Planned | Enterprise support: TableRegistry + RuntimeBackend | — |
| 4D | 🟢 Ready | Ship `dburnrate lint` as standalone feature | — |
| 5 | ⏳ Planned | Production hardening (error handling, caching, observability) + multi-cloud | `tasks/p5-*.md` |
| 6 | ⏳ Planned | ML cost bucket classification (requires calibration data) | `tasks/p6-*.md` |
| v0.2 | ⏳ Deferred | Notebook aggregation, compute advisor, cost regression detection | — |
| v0.3 | ⏳ Deferred | CI/CD gates, DABs integration, forecasting | — |

**Test count**: 263 passing | **Lint**: 0 errors | **Security**: bandit clean

### Roadmap changes (March 2026 Audit)
- **Phase 11** (`estimate_current_notebook()`): Reinstated — not a gimmick but Mode 5 parity. `estimate_self()` renamed to `estimate_current_notebook()` with proper path detection chain.
- **Phase 12** (batch glob): Deferred to v0.2 after core accuracy is validated
- **Phase 13** (CI/CD workflows): Deferred to v0.3
- **Phases 7–10**: Merged into Phase 5 (eliminated duplicate scope)

---

# March 2026 Audit: Critical Findings

> Independent audit of all source files, 263 tests, and DESIGN.md. See `files/` for full reports.

## Critical Bugs (Must Fix Before Any Estimate Is Trusted)

### Bug 1: Static Estimator Formula Is Quadratic (estimators/static.py)
Current: `complexity² × cluster_factor / 100` → 960× overestimate for simple GROUP BY.
**Fix:** Replace with linear throughput model: `(scan_bytes / throughput_bps + shuffle_count × shuffle_overhead) / 3600 × cluster_dbu_per_hour`

### Bug 2: Hybrid Estimator Uses Phantom Price (estimators/hybrid.py)
`_NOMINAL_USD_PER_DBU = 0.20` matches no real SKU (ALL_PURPOSE=$0.55, JOBS=$0.30, SQL_SERVERLESS=$0.70).
**Fix:** Remove constant. Call `get_dbu_rate(sku)` from `core/pricing.py`.

### Bug 3: EXPLAIN DBU Constants Are Ungrounded (estimators/hybrid.py)
`_SCAN_DBU_PER_GB = 0.5` is ~7,900× too high. DS3_v2 scans Parquet at ~3.2 GB/s → 1 GB takes ~0.3 s = 0.000063 DBU.
**Fix:** Derive from throughput benchmarks (R1 in research backlog). Use `0.000063 DBU/GB` as interim constant until empirically calibrated.

### Bug 4: Historical Estimation Ignores Data Volume Scaling (estimators/hybrid.py)
p50 duration from history is not scaled when current table is larger than historical runs.
**Fix:** `adjusted_ms = p50_ms × (current_read_bytes / median_historical_read_bytes)`

### Bug 5: SQL Injection in System Table Queries (tables/billing.py, queries.py, compute.py)
All queries use f-string interpolation. Sanitize or parameterize all cluster_id/warehouse_id inputs.

### Bug 6: Anti-Pattern Detector Uses String Matching (parsers/antipatterns.py)
`"CROSS JOIN" in sql.upper()` matches inside comments, string literals. Use sqlglot AST (already in `sql.py`).

### Bug 7: protocols.py Shadows Core Models
`CostEstimate` and `ParseResult` in `protocols.py` shadow the real `CostEstimate` in `models.py`. The `Estimator` protocol references the wrong class.

### Bug 8: SKU Inference Misclassifies Compute (estimators/static.py)
String-matching on instance type misclassifies SQL Warehouses, serverless, DLT. SKU must be explicit.

## Architecture Gaps

### Gap 1: No Estimation Pipeline Orchestrator
`HybridEstimator`, `DatabricksClient`, fingerprinting, Delta metadata, EXPLAIN all exist but nothing connects them. CLI uses only static estimator.
**Required:** `src/dburnrate/estimators/pipeline.py` — `EstimationPipeline` class orchestrating all tiers.

### Gap 2: No Databricks Runtime Support (Critical for 70% In-Cluster Use)
Package communicates exclusively via REST. Running inside a Databricks notebook → wasteful REST round-trip when `spark.sql()` is already available. Zero awareness of `DATABRICKS_RUNTIME_VERSION`.
**Required:** `src/dburnrate/runtime/` — `RuntimeBackend` protocol with `SparkBackend` (in-cluster) and `RestBackend` (external) implementations + `auto_backend()` detection.

### Gap 3: Missing `tables/attribution.py`
Referenced in DESIGN.md but doesn't exist. Required for calibration, "last time this cost $X" signal, and ML training data.
**Core pattern:** `system.billing.usage` × `system.billing.list_prices` join on cloud/sku/time; query attribution via `warehouse_id` + time overlap.

### Gap 4: DBU-Only Estimates Miss 40-60% of Classic Compute Cost
For classic clusters (Jobs, All-Purpose, DLT) the Azure VM bill is separate. DS4_v2 cluster = $0.585/hr in VM fees on top of DBU.
**Required:** Total cost = `(dbu × dbu_rate) + (vm_hours × vm_rate × node_count)` for classic; `dbu × serverless_rate` for serverless.

### Gap 5: No Top-Level Python API
`import dburnrate; dburnrate.estimate("SELECT ...")` fails. Only entry point is CLI.

## Enterprise Support: TableRegistry

Enterprise environments hide system tables behind curated views with row-level security. dburnrate has 8 hardcoded `system.*` paths across 3 files.

**Required:** `src/dburnrate/core/table_registry.py` — `TableRegistry` dataclass mapping logical → physical table names. Configurable via env vars (`DBURNRATE_TABLE_BILLING_USAGE=...`), TOML config, or programmatic API. All `tables/*.py` modules accept a `registry` parameter.

Config channels:
- Env: `DBURNRATE_TABLE_BILLING_USAGE`, `DBURNRATE_TABLE_QUERY_HISTORY`, etc.
- TOML: `[dburnrate.tables]` section in `.dburnrate.toml` or `pyproject.toml`
- Programmatic: `dburnrate.estimate("...", registry=TableRegistry(billing_usage="..."))`

## Research Backlog

| ID | Question | Status | Priority |
|----|----------|--------|----------|
| R1 | Actual Parquet scan / shuffle / join throughput per instance type | 🔬 Needs benchmark | P0 — blocks all constants |
| R2 | EXPLAIN COST accuracy vs actual execution (with/without PO stats) | 🔬 Needs benchmark | P1 |
| R3 | AQE plan divergence rate (EXPLAIN vs actual physical plan) | 🔬 Needs benchmark | P2 |
| R4 | Fingerprint recurrence rates (enterprise jobs vs ad-hoc) | 🔬 Needs benchmark | P1 |
| R5 | Billing attribution accuracy at concurrent queries | 🔬 Needs benchmark | P0 |
| R6 | Predictive Optimization coverage and freshness | ✅ Done | P2 |
| R7 | system.query.history coverage map | ✅ Done | P1 |
| R8 | Serverless billing granularity | ✅ Done | P2 |
| R9 | Cloud VM pricing APIs (Azure, AWS, GCP) | ✅ Done | P0 |
| R10 | Liquid Clustering vs partitioning scan cost impact | 🔬 Needs benchmark | P3 |
| R11 | ML feature importance (SHAP values) | 🔬 Needs benchmark | P3 |

**Key R6 finding:** Predictive Optimization runs exclusively on Unity Catalog managed tables, uses stats-on-write via Photon, billed under `billing_origin_product = 'PREDICTIVE_OPTIMIZATION'`. Freshness queryable from `system.storage.predictive_optimization_operations_history`.

**Key R7 finding:** `system.query.history` captures SQL Warehouses + serverless notebooks/jobs only. All-purpose clusters, classic Jobs clusters, DLT, and PySpark DataFrames are NOT captured.

**Key R8 finding:** Serverless SQL Warehouses have no per-query DBU attribution. `usage_metadata` is mostly null except `warehouse_id`. Serverless jobs have clean per-run attribution via `job_id`/`job_run_id`.

**Key R9 finding:** Azure VM pricing via `prices.azure.com/api/retail/prices` (zero auth). DS4_v2=$0.585/hr, DS3_v2=$0.293/hr, E8s_v3=$0.504/hr. Infracost GraphQL API covers all clouds with one interface.

## SQL Warehouse DBU Rates (Cross-Referenced March 2026)

| Size | DBU/hr | Confidence |
|------|--------|-----------|
| 2X-Small | 4 | Confirmed |
| X-Small | ~8 | Inferred |
| Small | 12 | Confirmed |
| Medium | 24 | Confirmed |
| Large | ~48 | Inferred |
| X-Large | ~96 | Inferred |
| 2X-Large | ~192 | Inferred |
| 4X-Large | 528 | Confirmed |

## Lakeflow Job Cost Attribution (Canonical SQL, January 2026)

```sql
-- Per-job-run cost with clock-hour-aligned timeline join
SELECT t1.workspace_id, t2.name, t1.job_id, t1.run_id, SUM(list_cost) as list_cost
FROM (
  SELECT workspace_id, usage_metadata.job_id,
         usage_metadata.job_run_id as run_id,
         SUM(usage_quantity * list_prices.pricing.default) as list_cost
  FROM system.billing.usage t1
  INNER JOIN system.billing.list_prices list_prices
    ON t1.cloud = list_prices.cloud AND t1.sku_name = list_prices.sku_name
    AND t1.usage_start_time >= list_prices.price_start_time
    AND (t1.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
  WHERE billing_origin_product = 'JOBS'
    AND usage_date >= CURRENT_DATE() - INTERVAL 30 DAY
  GROUP BY ALL
) t1
LEFT JOIN (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY workspace_id, job_id ORDER BY change_time DESC) as rn
  FROM system.lakeflow.jobs QUALIFY rn=1
) t2 USING (workspace_id, job_id)
GROUP BY ALL ORDER BY list_cost DESC
```

**All-purpose cluster limitation:** `usage_metadata.job_id` is NULL for jobs on all-purpose clusters. Per Databricks: "Precise cost calculation for jobs on all-purpose compute is not possible with 100% accuracy."

## Compute Throughput Benchmarks (R1 Research Findings)

**Parquet scan:** CERN measurements on 20-core Xeon: 0.05–0.10 GB/s per core. Databricks Cache ~4× boost.

**Azure instance I/O limits:**
| Instance | vCPUs | Uncached Disk MB/s | Network Mbps |
|----------|-------|-------------------|-------------|
| DS3_v2 | 4 | 192 | 3,000 |
| DS4_v2 | 8 | 384 | 6,000 |
| D16s_v3 | 16 | 384 | 8,000 |
| E8s_v3 | 8 | 192 | 5,000 |

**Photon speedups (SIGMOD '22, TPC-H SF=3000):**
| Operation | Speedup |
|-----------|---------|
| Parquet scan | 1.2–2× |
| Hash join | 3–3.5× |
| Aggregation | 3.5–5.7× |
| Sort | ~2× |
| TPC-H overall | 4× avg, 23× max |

**Critical:** Photon DBU rates are ~1.5–2× higher. Breakeven requires sufficient speedup — Zipher benchmark shows join query 1.8× faster but 4× more expensive ($0.07→$0.30).

## Testing Strategy Gaps

1. **Zero accuracy tests** — 263 tests verify types/edges, not estimate correctness
2. **No benchmark dataset** — required: `tests/benchmarks/` with TPC-DS queries + known costs
3. **No property-based tests** — Hypothesis is in dev deps but unused
4. **No integration test infrastructure** — no fixtures for live Databricks connection

**Accuracy targets by phase:**
- Phase 4: All estimates within **10×** of actual
- Phase 5: Within **3×** of actual
- Phase 6 (ML): Within **2×** of actual

**Required benchmark structure:**
```
tests/benchmarks/
├── README.md
├── queries/          # TPC-DS reference SQL
├── explain_outputs/  # Known EXPLAIN COST text
├── delta_metadata/   # Known DESCRIBE DETAIL outputs
├── expected_costs.json
└── conftest.py
```

## Performance Notes

For `dburnrate estimate` in connected mode: 85-95% of wall time is network I/O. Rust acceleration is premature until batch mode ships and profiling confirms parsing is the bottleneck. Prefer:
1. Server-side fingerprinting via `SHA2(UPPER(REGEXP_REPLACE(statement_text, '\\d+', '?')), 256)`
2. `sqlglotrs` optional dep for 2-5× faster tokenization (zero custom Rust)
3. Aggressive TTL caching on `DESCRIBE DETAIL` and `normalize_sql`

---

# Research Findings

## Overview

**Databricks has no BigQuery-style dry-run API, but a powerful hybrid architecture is achievable** by combining Spark's `EXPLAIN COST` plans, Delta Lake transaction log metadata, query fingerprinting against `system.query.history`, and ML models trained on execution plan features. The critical insight from researching how Netflix, Uber, and Airbnb handle this at scale: no organization does pure pre-execution cost estimation from scratch — every production system relies on a blend of static plan analysis for cold-start queries and historical execution data for recurring workloads.

---

## EXPLAIN COST provides rich statistics without execution

Spark SQL's `EXPLAIN COST <query>` generates the full optimized logical plan with **per-operator `sizeInBytes` and `rowCount` estimates** without reading a single byte of data. This is the strongest pre-execution signal available on the platform.

**Five EXPLAIN modes:**
- `SIMPLE` - Basic plan structure
- `EXTENDED` - Logical and physical plans
- `COST` - **Key mode for estimation** with statistics
- `CODEGEN` - Generated code
- `FORMATTED` - Machine-readable JSON

**Typical output:**
```
Aggregate [s_store_sk], Statistics(sizeInBytes=20.0 B, rowCount=1)
+- Join Inner, Statistics(sizeInBytes=30.8 MB, rowCount=1.62E+6)
   +- Relation parquet, Statistics(sizeInBytes=134.6 GB, rowCount=2.88E+9)
```

**Physical plan reveals:**
- Join strategies (BroadcastHashJoin vs SortMergeJoin vs ShuffledHashJoin)
- Shuffle operations
- Pushed-down filters
- AQE markers

**Programmatic access:**
- `spark.sql("EXPLAIN COST ...")` returns DataFrame
- `df._jdf.queryExecution().optimizedPlan().stats()` exposes structured stats

**Accuracy caveats:**
- With `ANALYZE TABLE` statistics: ~1.5× error factor
- Without statistics: **1000× or more error**
- Databricks Runtime 16.0+ reports statistics completeness: `missing`/`partial`/`full`
- Predictive Optimization (GA 2025) auto-runs ANALYZE on UC tables

**Comparison to competitors:**
- **BigQuery**: `dryRun: true` → exact `totalBytesProcessed` → direct dollar cost at $5/TB
- **Snowflake**: EXPLAIN shows partition pruning but no dollar cost; must retroactively compute from QUERY_HISTORY
- **Databricks**: Richest plan detail but **no direct DBU-to-dollar mapping** from EXPLAIN

---

## Delta transaction logs enable exact scan-size estimation

Delta Lake's `_delta_log` stores per-file metadata providing **exact** table-level statistics without data scanning.

**Per-file statistics in `add` actions:**
- `numRecords` - exact row count
- `minValues`/`maxValues` - per-column (first 32 columns)
- `nullCount` - per-column
- `size` - exact file size in bytes

**Access methods:**
- `DESCRIBE DETAIL tablename` → `sizeInBytes`, `numFiles`, `partitionColumns`, `lastModified`
- `delta-rs` library: `get_add_actions(flatten=True)` returns DataFrame with per-file stats

**Two statistics systems:**
1. **Delta data-skipping** - automatic, always current (file-level min/max/null/record counts)
2. **Query optimizer statistics** - requires `ANALYZE TABLE`, stored in metastore (distinct_count, histograms, avg_col_len)

**Practical implications:**
- Use `DESCRIBE DETAIL` for instant table-level size
- Parse `add` actions for partition-level sizes
- File-level min/max for predicate filter estimation
- Reserve ANALYZE statistics for join cardinality

---

## Query fingerprinting turns history into prediction

**Normalization pipeline** (Percona `pt-fingerprint` pattern):
1. Strip comments
2. Normalize whitespace
3. Replace literals with `?`
4. Collapse IN-lists to single placeholders
5. Abstract database names
6. SHA-256 hash → `template_id`

**`system.query.history` metrics:**
- `total_duration_ms`
- `read_bytes`, `read_rows`, `read_files`
- `produced_rows`
- `spill_to_disk_bytes`
- `total_task_duration_ms`
- Cache hit indicators

**Cost attribution:** `(query_duration / total_warehouse_duration_in_hour) × hourly_DBU_cost × list_price`

**Three similarity-matching tiers:**
1. **Exact fingerprint match** (highest confidence) → historical p50/p95
2. **AST edit distance** (high confidence) → SQLGlot structural distance
3. **Embedding-based similarity** (medium confidence) → CodeBERT/`all-mpnet-base-v2` vectors

**Uber's production system:** 100K+ Spark applications/day using Spark Event Listeners → Kafka → Flink analyzers with 180-day windows.

---

## ML models achieve 14–98% accuracy

**Microsoft's Cleo (SIGMOD 2020):**
- Traditional models with perfect cardinalities: **258% median error**, 0.04 Pearson correlation
- Learned approach: **14% median error** (operator-subgraph), **42%** (operator-level)
- Architecture: large collection of smaller specialized models with meta-model ensemble

**Twitter (IC2E 2021):**
- Raw SQL text features, classification into resource buckets
- **97.9% accuracy** CPU prediction, **97%** memory
- ~200ms inference time

**RAAL/DRAL (ICDE 2022-2024):**
- First Spark-specific learned cost models
- Key innovation: resource-awareness (executors, memory, cores as features)
- DRAL adds data-aware features via unsupervised learning

**Cold-start solutions:**
- Zero-shot models (Hilprecht & Binnig, VLDB 2022): graph neural networks with transferable features
- Few-shot fine-tuning: 10–100 queries dramatically improves accuracy
- Bao (SIGMOD 2021): Thompson sampling for exploration-exploitation

**Counterpoint (Heinrich et al., SIGMOD 2025):** Traditional PostgreSQL cost models often outperform learned models on actual plan selection despite higher estimation errors.

---

## Unity Catalog metadata gaps

**What's missing:**
- `information_schema.tables`: no size, row count, or statistics
- `information_schema.columns`: no cardinality estimates, distribution info, or null counts
- Unity Catalog REST API: no size or statistics

**No `pg_class`-equivalent** for passive table size recording across all tables.

**Metadata access hierarchy:**
1. `DESCRIBE DETAIL` → best lightweight source
2. `DESCRIBE TABLE EXTENDED ... AS JSON` → statistics if ANALYZE/PO ran
3. `information_schema.columns` + type-based width estimation
4. `system.storage.predictive_optimization_operations_history`
5. Lakehouse Monitoring profile tables (richest, requires setup)

---

## Serverless billing challenges

**Billing model:** DBU/hour × uptime (per-second granularity), **not per-query**

**Fixed burn rates:**
- X-Small = 6 DBU/hr
- Small = 12 DBU/hr
- etc.

**Per-DBU rate:** ~$0.70 (AWS US) - bundles infrastructure

**No pre-execution cost estimation API exists** in Databricks platform. IWM system predicts internally for autoscaling but doesn't expose predictions.

**Viable approximation:** duration-proportional attribution via `system.query.history` + `system.billing.usage` join on `warehouse_id` + time overlap.

---

## Hybrid architecture for pre-execution estimation

**Tier 1 — Instant metadata lookup (< 100ms):**
- Cached `DESCRIBE DETAIL` results
- Compute total input data volume
- Partition pruning estimates

**Tier 2 — EXPLAIN plan analysis (< 2 seconds):**
- Submit `EXPLAIN COST <query>` via Statement Execution API
- Parse for `sizeInBytes`/`rowCount`, join strategies, shuffle count
- Cross-validate with Tier 1 Delta metadata

**Tier 3 — Historical fingerprint matching (< 500ms):**
- Normalize and hash → `template_id`
- Look up in `system.query.history` index
- Exact matches: p50/p95 with confidence intervals
- Near-matches: AST edit distance or embedding similarity

**Tier 4 — ML model prediction (< 300ms):**
- Features: operator types, cardinalities from EXPLAIN, table sizes, cluster config
- Classify into cost buckets (low/medium/high/very-high) per Twitter approach
- Cleo's hierarchical strategy: specialized → general → zero-shot

**Compute type differentiation:**
- **SQL Warehouses:** cost ∝ query duration × fixed DBU rate
- **Jobs Compute:** cost = cluster_uptime × instance_cost + DBU_cost
- **Interactive clusters:** resource share via `total_task_duration_ms` / cluster capacity

---

# Architecture & Concepts

## Static analysis turns SQL and PySpark into cost signals

**SQLGlot (v28.10.1)** — SQL parsing backbone:
- Databricks officially supported dialect
- First-class support: `MERGE INTO`, `COPY INTO`, `CREATE STREAMING TABLE`, JSON colon extraction (`col:path`), `CLUSTER BY`, Delta DML
- Typed expression nodes: `exp.Join`, `exp.AggFunc`, `exp.Window`, `exp.Merge`
- Zero dependencies, Python 3.9+, Rust-accelerated tokenizer available
- Limitations: `OPTIMIZE`/`ZORDER BY` parse as generic `Command`

**Python `ast` module** — PySpark analysis:
- Custom `ast.NodeVisitor` detects: `.groupBy()`, `.join()`, `.collect()`, `.toPandas()`, `.repartition()`
- Decorator patterns: `@udf` vs `@pandas_udf`
- Embedded SQL via `spark.sql()` strings → route to SQLGlot
- No off-the-shelf PySpark anti-pattern detector exists

**Anti-patterns to flag:**
- `collect()` without prior `limit()`
- Python UDFs instead of Pandas UDFs (10–100× overhead)
- `.crossJoin()`
- `.repartition(1)`
- `toPandas()` on unbounded datasets

**Notebook parsing:**
- `.ipynb`: JSON with cell schema (via `nbformat` or raw `json`)
- `.dbc`: ZIP with JSON notebooks (UTF-8, Base64, or gzip-compressed)
- Magic commands: `%sql`, `%python`, `%scala`
- Source exports: `# COMMAND ----------` delimiters

---

## Complexity scoring model

| Operation | Weight | Rationale |
|-----------|--------|-----------|
| `MERGE INTO` | 20 | Join + scan + file rewrite |
| Cross join | 50 | O(n×m) output |
| Shuffle join | 10 | Full data redistribution |
| `GROUP BY` | 8 | Shuffle for aggregation |
| Window function | 8 | Shuffle + sort within partitions |
| `collect()` / `toPandas()` | 25 | Driver memory anti-pattern |
| Python UDF | 15 | Row-at-a-time JVM↔Python serialization |
| Pandas UDF | 5 | Vectorized via Arrow |
| `ORDER BY` | 7 | Global sort requires shuffle |
| `DISTINCT` | 6 | Dedup shuffle |

**DBU cost formula:** `complexity_score × data_volume_factor × cluster_config_factor × dbu_rate`

---

## System tables reference

**`system.billing.usage`** (GA, 365-day retention):
- `usage_quantity` in DBUs
- `sku_name` for compute type
- `usage_metadata`: `cluster_id`, `warehouse_id`, `job_id`, `job_run_id`, `notebook_id`, `dlt_pipeline_id`, `endpoint_name`
- `billing_origin_product`: `JOBS`, `SQL`, `INTERACTIVE`, `MODEL_SERVING`, `PREDICTIVE_OPTIMIZATION`
- `identity_metadata.run_as` for per-user attribution
- `custom_tags` for team/department attribution

**`system.billing.list_prices`** (GA, indefinite retention):
- Historical pricing by SKU, cloud, time period
- Join: `usage_quantity × pricing.effective_list.default`

**`system.query.history`** (Public Preview, 365-day retention):
- `statement_text`, `execution_duration_ms`, `total_task_duration_ms`
- `read_bytes`, `read_rows`, `spilled_local_bytes`, `network_sent_bytes`, `written_bytes`
- `query_source` links to notebooks, jobs, dashboards
- No direct join to billing.usage — correlate via `warehouse_id`/`notebook_id`/`job_id` + time window

**Additional tables (2024-2025):**
- `system.lakeflow.*` - job and pipeline monitoring (June 2024)
- `system.compute.node_timeline` - minute-by-minute utilization (90-day retention)
- `system.serving.endpoint_usage` - token-level model serving
- `system.storage.predictive_optimization_operations_history` - COMPACTION/VACUUM/CLUSTERING with DBU
- `system.mlflow.*` - experiment tracking
- `system.compute.node_types` - instance type → hardware specs mapping

---

## Azure pricing model

**Dual-bill:** DBUs for platform + Azure VMs for infrastructure (except serverless)

**Premium tier, US East, pay-as-you-go:**

| Compute type | $/DBU | Includes VM? |
|-------------|-------|-------------|
| Jobs Compute (Classic) | **$0.30** | No |
| Jobs Light | $0.22 | No |
| All-Purpose (Classic) | **$0.55** | No |
| Serverless Jobs | **$0.45** | Yes (50% promo) |
| Serverless Notebooks | **$0.95** | Yes (30% promo) |
| SQL Classic | $0.22 | No |
| SQL Pro | $0.55 | No |
| SQL Serverless | **$0.70** | Yes |
| DLT Core/Pro/Advanced | $0.30 / $0.38 / $0.54 | No |
| Model Serving (CPU/GPU) | $0.07 | Yes |

**Instance DBU rates:**
- Standard_DS3_v2 (4 vCPU, 14 GiB) = 0.75 DBU/hr
- Standard_DS4_v2 (8 vCPU, 28 GiB) = 1.50 DBU/hr
- Standard_D64s_v3 (64 vCPU, 256 GiB) = 12.0 DBU/hr

**Photon:**
- Classic clusters: **2.5× DBU multiplier** (Azure), 2.9× (AWS)
- SQL Warehouses/serverless: enabled by default, no extra charge
- Breakeven: **2× runtime speedup** - benchmarks show 2.7× average for complex SQL

**Serverless:**
- Eliminates VM management
- SQL Warehouse sizes: Small = 12 DBU/hr = $8.40/hr all-in
- Steady-state: 2–5× more than optimized classic, but eliminates idle costs

**Committed-use discounts:**
- 1-year: up to **33% savings**
- 3-year: up to **37% savings**

---

## Forecasting architecture

**Prophet** (default):
- Daily/weekly/yearly seasonality
- Holiday calendars
- Trend changepoints
- Bayesian uncertainty intervals (`yhat_lower`, `yhat_upper`)

**NeuralProphet** (optional upgrade):
- 55–92% accuracy improvement over Prophet
- Conformalized Quantile Regression (CQR) for guaranteed coverage

**Implementation decisions:**
- Train per SKU × workspace
- Normalize for business events as explicit regressors
- Handle regime changes (cluster resize, serverless migration) as changepoints
- Target <15% MAPE; best-in-class: <10% overrun frequency, <20% deviation

**Optional dependency:** `pip install dbcost[forecasting]` (~200 MB for Prophet's `cmdstanpy`)

---

## What-if scenarios

**Photon toggle:**
```
cost = (current_DBU × 2.5) / speedup_factor
```
- Complex joins: 2–4× speedup (20–57% net cost reduction)
- Aggregations: 4× speedup
- Simple inserts: zero speedup, **72% cost increase**

**Liquid Clustering migration:**
- **2–12× read performance improvement**
- **7× faster writes** vs partitioning + Z-ORDER
- Incremental rewrites vs full partition rewrites
- Migrate when partition cardinality >5,000 or patterns evolve

**Cluster right-sizing:**
- **CPU-bound** (>90% util, heavy UDFs) → scale cores, C-series
- **Memory-bound** (spill, OOM) → scale memory, E/R-series
- **I/O-bound** (slow reads, small files) → fix data layout, then L-series
- **Shuffle-bound** (high network) → scale out

**Savings:**
- Fixed → autoscaling: **40–60%**
- Spot instances: additional **60–80%** on VM costs

**Serverless migration:**
- Compare: `classic_cost = DBU_rate × DBU_count + VM_cost`
- vs: `serverless_cost = serverless_DBU_rate × estimated_DBU_count`
- Breakeven: <30% active compute time typically benefits from serverless

---

## Competitive landscape

**Databricks native:**
- System tables, AI/BI dashboards, Budgets API
- Cloud Infra Cost Field Solution (open-source)
- Overwatch (deprecated — system tables subsumed)

**Open-source:**
- SparkMeasure (v0.25): collects task/stage metrics, no dollar translation
- OpenCost: Databricks plugin (early-stage, KubeCon 2024)
- Onehouse Spark Analyzer (2025): Excel reports from History Server

**Commercial:**
- **Unravel Data**: AI-powered optimization, CI/CD integration, 25–35% savings
- **Sedai** (GA June 2025): autonomous right-sizing via RL
- **Sync Computing/Slingshot**: per-job ML models, 37–55% savings
- **Acceldata**: cost + quality observability, 96–98% forecast accuracy
- **CloudZero**: multi-vendor aggregation, lacks DB-specific optimization

**Gap:** No tool provides **pre-execution cost estimation from code**, **what-if modeling with quantified tradeoffs**, or a **reusable Python library** for notebooks/CI/CD.

---

## Package architecture

```
dburnrate/
├── src/
│   └── dburnrate/
│       ├── __init__.py
│       ├── _compat.py              # Optional import helpers
│       ├── core/
│       │   ├── models.py           # Pydantic models
│       │   ├── config.py           # Pydantic-settings
│       │   ├── pricing.py          # DBU rate lookups
│       │   └── protocols.py        # Protocol classes
│       ├── parsers/
│       │   ├── sql.py              # SQLGlot analysis [sql]
│       │   ├── pyspark.py          # AST analysis
│       │   ├── notebooks.py        # .ipynb + .dbc
│       │   └── antipatterns.py     # Detection rules
│       ├── tables/
│       │   ├── billing.py          # system.billing.*
│       │   ├── queries.py          # system.query.history
│       │   ├── compute.py          # system.compute.*
│       │   └── attribution.py     # Cost joins
│       ├── estimators/
│       │   ├── static.py           # Complexity-based
│       │   ├── historical.py       # History-based
│       │   ├── hybrid.py           # Combined approach
│       │   ├── self.py             # Self-referential [NEW]
│       │   └── whatif.py           # Scenarios
│       ├── batch.py                # Batch file analysis [NEW]
│       ├── cli/
│       │   └── main.py             # Typer entry point
│       └── forecast/
│           └── prophet.py          # Time-series [forecasting]
├── tests/
├── pyproject.toml
└── README.md
```

**pyproject.toml extras:**
```toml
[project.optional-dependencies]
sql = ["sqlglot>=20.0"]
forecasting = ["prophet>=1.1"]
ml = ["scikit-learn>=1.3"]
all = ["dburnrate[sql,forecasting,ml]"]
```

**Type safety:**
- Python 3.12+: PEP 695 generics, Protocol classes, TypedDict
- pydantic-settings with env vars: `DBCOST_WORKSPACE_URL`, `DBCOST_TOKEN`

**Graceful imports:**
```python
from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import sqlglot

def _require(module: str, extra: str):
    try:
        return __import__(module)
    except ImportError:
        raise ImportError(f"Install with: pip install dburnrate[{extra}]") from None
```

---

## Design principles

**Layered estimation fidelity:**
1. Static analysis (code only) — cheapest, useful for dev/CI
2. Historical data enrichment — medium accuracy
3. Live pricing + forecasting — highest accuracy, requires runtime context

**Single most impactful capability:** Pre-execution cost estimation answering "what will this notebook cost?" before cluster starts.

---

# Implementation Roadmap

## Phase 1: Foundation & Validation ✅ COMPLETE

### 1.1 Run and Fix Tests ✅
- [x] Run existing tests: `uv run pytest -m unit -v`
- [x] Run linting: `uv run ruff check src/ tests/`
- [x] Run formatting check: `uv run ruff format --check src/ tests/`
- [x] Fix all linting/type errors
- [x] Add return type hints to all functions
- [x] Add missing docstrings
- [x] Run security audit: `uv run bandit -c pyproject.toml -r src/`

### 1.2 Code Quality ✅
- [x] Type hints on all public functions
- [x] Proper exception handling
- [x] CLI argument validation

---

## Phase 2: System Tables Integration ✅ COMPLETE

### 2.1 Billing Integration ✅
- [x] Implement `src/dburnrate/tables/billing.py`
- [x] Query `system.billing.usage`
- [x] Query `system.billing.list_prices`
- [x] Cost attribution: `query_duration / total_duration * hourly_cost`

### 2.2 Query History Integration ✅
- [x] Implement `src/dburnrate/tables/queries.py`
- [x] Extract: `execution_duration_ms`, `read_bytes`, `read_rows`, `total_task_duration_ms`
- [x] SQL fingerprinting: `normalize_sql()` + SHA-256 → `fingerprint_sql()`

### 2.3 Compute Integration ✅
- [x] Implement `src/dburnrate/tables/compute.py`
- [x] Query `system.compute.node_types`
- [x] Query `system.compute.clusters`
- [x] Query `system.compute.node_timeline`

### 2.4 Databricks Connection ✅
- [x] REST API client (`src/dburnrate/tables/connection.py`)
- [x] Workspace URL + token auth (Bearer)
- [x] Inline result short-circuit + polling for async statements
- [x] Basic retry logic

---

## Phase 3: EXPLAIN COST Integration ✅ COMPLETE

### 3.1 EXPLAIN Parsing ✅
- [x] Implement `src/dburnrate/parsers/explain.py`
- [x] Parse `sizeInBytes` and `rowCount`
- [x] Extract join strategies
- [x] Count shuffle operations
- [x] Research spec: `docs/explain-cost-schema.md`

### 3.2 Statistics Handling ✅
- [x] Detect completeness (8-byte placeholder = no stats)
- [x] Handle tables without ANALYZE (zero-fill)
- [x] Delta metadata integration (`src/dburnrate/parsers/delta.py`)

### 3.3 Hybrid Estimation ✅
- [x] Combine static + EXPLAIN (`src/dburnrate/estimators/hybrid.py`)
- [x] Weight EXPLAIN higher when stats available
- [x] Confidence boost on agreement

---

## Phase 4A: Critical Bug Fixes (🔴 Must Do First)

> These bugs make every existing estimate wrong. Fix before wiring CLI.

### 4A.1 Fix Quadratic Formula → Linear Throughput Model (estimators/static.py)
- [ ] Replace `complexity² × cluster_factor / 100` with linear model
- [ ] Formula: `(scan_bytes / throughput_bps + shuffle_count × shuffle_overhead_sec) / 3600 × cluster_dbu_per_hour`
- [ ] Use interim constant: `throughput_bps = 3.2e9` (DS4_v2 Parquet scan from R1 research)
- [ ] Verify: GROUP BY on 2-worker DS3_v2 produces ~0.001 DBU (not 0.96)

### 4A.2 Fix Phantom Price (estimators/hybrid.py)
- [ ] Remove `_NOMINAL_USD_PER_DBU = 0.20`
- [ ] Use `get_dbu_rate(sku)` from `core/pricing.py` throughout
- [ ] Return DBU estimates from hybrid; let CLI/API layer convert to dollars

### 4A.3 Fix EXPLAIN DBU Constants (estimators/hybrid.py)
- [ ] Replace `_SCAN_DBU_PER_GB = 0.5` with `_SCAN_DBU_PER_GB = 0.000063` (derived from 0.3s/GB × cluster_dbu_per_hour / 3600)
- [ ] Document derivation with citation (R1 benchmarks)
- [ ] Add TODO comment noting constants need empirical calibration (R1)

### 4A.4 Add Data Volume Scaling to Historical Estimates (estimators/hybrid.py)
- [ ] Implement: `adjusted_ms = p50_ms × (current_read_bytes / median_historical_read_bytes)`
- [ ] Guard: if `median_historical_read_bytes == 0`, skip scaling

### 4A.5 Fix SQL Injection in Table Queries (tables/*.py)
- [ ] Sanitize `cluster_id`, `warehouse_id`, `statement_id` inputs (alphanumeric + hyphens only)
- [ ] Add `_sanitize_id(value: str) -> str` helper in `connection.py`

### 4A.6 Fix Anti-Pattern Detector (parsers/antipatterns.py)
- [ ] Replace string-matching with sqlglot AST traversal
- [ ] Reuse `detect_operations()` from `sql.py`

### 4A.7 Fix protocols.py Shadow Classes
- [ ] Remove `CostEstimate` and `ParseResult` placeholder classes from `protocols.py`
- [ ] Update `Estimator` protocol to import from `models.py`

---

## Phase 4B: CLI Wiring & Attribution (🔄 Active)

> Task files: `tasks/p4-01-wire-explain-into-cli.md`, `tasks/p4-02-delta-scan-size.md`,
> `tasks/p4-03-fingerprint-lookup.md`, `tasks/p4-04-aws-gcp-pricing.md`

### 4B.1 EstimationPipeline Orchestrator (NEW — required first)
- [ ] Create `src/dburnrate/estimators/pipeline.py`
- [ ] `EstimationPipeline.estimate(query, cluster)` orchestrates all tiers
- [ ] Tier 1: static (always runs)
- [ ] Tier 2: Delta metadata (if connected)
- [ ] Tier 3: EXPLAIN COST (if connected + warehouse_id)
- [ ] Tier 4: fingerprint + history lookup (if connected)
- [ ] Graceful fallback at each tier

### 4B.2 Wire Pipeline into CLI
- [ ] `estimate` command uses `EstimationPipeline`, not static estimator
- [ ] `--warehouse-id`, `--workspace-url` flags
- [ ] `--explain` flag shows per-tier breakdown
- [ ] Fallback to static on connection error

### 4B.3 Missing `tables/attribution.py`
- [ ] Implement billing × list_prices join (canonical SQL from `files/02-ARCHITECTURE-GAPS.md`)
- [ ] Per-query attribution via warehouse_id + time overlap
- [ ] Lakeflow job-run cost attribution
- [ ] `get_historical_cost(fingerprint)` → used by pipeline Tier 4

### 4B.4 AWS/GCP Pricing
- [ ] `get_dbu_rate(sku_name, cloud="AZURE", tier="PREMIUM") -> Decimal`
- [ ] AWS and GCP DBU rates in `pricing.py`
- [ ] Cloud auto-detection from workspace URL
- [ ] `--cloud` CLI flag

---

## Phase 4C: Enterprise Support (⏳ Planned)

### 4C.1 TableRegistry
- [ ] `src/dburnrate/core/table_registry.py` — frozen dataclass with default `system.*` paths
- [ ] Thread registry through `billing.py`, `queries.py`, `compute.py` (all 8 hardcoded refs)
- [ ] Env var support: `DBURNRATE_TABLE_BILLING_USAGE`, etc.
- [ ] TOML config: `[dburnrate.tables]` in `.dburnrate.toml`

### 4C.2 RuntimeBackend (Dual-Mode)
- [ ] `src/dburnrate/runtime/` package
- [ ] `RuntimeBackend` Protocol: `execute_sql()`, `explain_cost()`, `describe_detail()`, `is_connected`
- [ ] `SparkBackend`: uses `SparkSession.getActiveSession()` (in-cluster)
- [ ] `RestBackend`: uses `DatabricksClient` (external)
- [ ] `auto_backend()`: checks `DATABRICKS_RUNTIME_VERSION` env var

### 4C.3 Top-Level Python API
- [ ] `dburnrate.estimate(query, cluster=None, registry=None) -> CostEstimate`
- [ ] `dburnrate.TableRegistry` exported from `__init__.py`

---

## Phase 4D: `dburnrate lint` (🟢 Ready Today)

Anti-pattern detection works now with zero calibration. Ship as standalone feature.

### 4D.1 CLI Command
- [ ] `dburnrate lint <path|glob>` — recursive file discovery
- [ ] Output: file:line severity message
- [ ] Exit code 1 if any errors found (CI-compatible)
- [ ] Severity levels: ERROR / WARNING / INFO
- [ ] `--format json` for CI integration

---

## Phase 5: Production Hardening (⏳ Planned)

> Consolidates former Phases 5, 7, 8, 9, 10. Task files: `tasks/p5-*.md`

### 5.1 Error Handling
- [ ] Extended exception hierarchy (`AuthenticationError`, `RateLimitError`, `WarehouseError`)
- [ ] User-friendly messages with recovery suggestions
- [ ] Token redaction from all error output
- [ ] Graceful degradation with `--extra sql` hint

### 5.2 Caching & Performance
- [ ] TTL cache for `DESCRIBE DETAIL` results (5 min default)
- [ ] `requests.Session` + `HTTPAdapter` pool in `DatabricksClient`
- [ ] Server-side fingerprinting: push `SHA2(REGEXP_REPLACE(...))` to SQL instead of client-side
- [ ] `sqlglotrs` optional dep for batch mode

### 5.3 Observability
- [ ] `logging.NullHandler()` on `dburnrate` root logger
- [ ] Structured log calls (DEBUG/INFO/WARNING/ERROR)
- [ ] `--debug` flag for verbose output + full tracebacks
- [ ] Per-tier timing in `CostEstimate.breakdown`

### 5.4 Multi-Cloud Pricing
- [ ] AWS DBU rates + instance types (Photon 2.9× multiplier)
- [ ] GCP DBU rates + instance types
- [ ] VM pricing via Azure Retail Prices API / AWS Pricing API / GCP Billing Catalog (or Infracost)
- [ ] Total cost of ownership: DBU + VM for classic; DBU-bundled for serverless

### 5.5 Enhanced Operations
- [ ] COPY INTO, OPTIMIZE/ZORDER detection (sqlglot AST)
- [ ] Unity Catalog 3-level naming awareness
- [ ] Anti-pattern severity levels + suggestions

### 5.6 CLI Enhancements
- [ ] `--export json/csv` flag
- [ ] `dburnrate audit --days N` — hidden cost audit (PO, mat views, monitoring)
- [ ] `dburnrate waste --days N` — idle cluster detection
- [ ] `dburnrate advise` — compute type advisor

---

## Phase 6: ML Cost Models (⏳ Planned — Requires Calibration Data from 4B)

> Task files: `tasks/p6-*.md`

### 6.1 Feature Extraction
- [ ] `src/dburnrate/estimators/features.py` — `QueryFeatures` dataclass
- [ ] Operator types, cardinalities from EXPLAIN
- [ ] Table sizes from Delta, cluster config
- [ ] `FEATURE_NAMES` constant

### 6.2 Classification Model
- [ ] `src/dburnrate/estimators/ml.py` — `CostBucketClassifier`
- [ ] Buckets: low (<0.1 DBU) / medium / high / very-high (>10 DBU)
- [ ] `HistGradientBoostingClassifier` (sklearn `[ml]` extra)
- [ ] `train-model` CLI command

### 6.3 HybridEstimator Integration
- [ ] ML bucket as optional fourth signal in `EstimationPipeline`
- [ ] Confidence adjustment when ML contradicts other signals

---

## v0.2: New Features (⏳ Deferred)

Requires calibrated estimation (Phase 4A) first.

| Feature | Description | Effort |
|---------|-------------|--------|
| Notebook-level aggregation | Per-cell cost breakdown for `.ipynb` files | 3 days |
| Cost regression detection | Alert when p50 cost for fingerprint jumps >2× | 1 week |
| Committed-use discount modeling | Model DBCU savings from 90-day usage | 3 days |
| Lakeflow job DAG estimation | Estimate multi-task job from job definition JSON | 2 weeks |
| Batch file analysis | `dburnrate estimate-batch "queries/*.sql"` | 1 week |
| Spot instance modeling | Model spot VM savings vs interruption risk | 2 days |

---

## v0.3: CI/CD & Integrations (⏳ Deferred)

| Feature | Description |
|---------|-------------|
| CI/CD cost gate | `dburnrate compare baseline.json current.json --threshold 200%` |
| DABs integration | `dburnrate estimate-bundle ./databricks.yml` |
| Notebook widget | `dburnrate.install_widget()` for Databricks UI |
| GitHub Actions template | Post estimate as PR comment |
| Data layout advisor | Liquid Clustering vs partitioning scan cost impact |

---

## Post-MVP: Advanced Features

### Forecasting
- [ ] Prophet-based cost forecasting
- [ ] Per-SKU × workspace models
- [ ] Business event awareness
- [ ] Confidence intervals

### DLT/SDP
- [ ] DLT pipeline cost estimation
- [ ] Tier detection (Core/Pro/Advanced)
- [ ] Pipeline dependency analysis

### Cluster Optimization
- [ ] Right-sizing recommendations
- [ ] Bottleneck classification
- [ ] Instance family recommendations

---

# Verification & Quality

## Required Commands

Run after each phase:

```bash
# Unit tests
uv run pytest -m unit -v

# Coverage
uv run pytest --cov --cov-report=term-missing

# Lint
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
uv run bandit -c pyproject.toml -r src/

# Docstring coverage
uv run interrogate src/ -v

# Security audit
uv run pip-audit
```

## Key Documents

- **DESIGN.md** (this file) - Research, architecture, roadmap, and current status
- **AGENTS.md** - LLM working rules and workflow
- **README.md** - User-facing documentation
- **tasks/*.md** - Authoritative execution task files (active tasks: `status: todo`)
- **tasks/*.md.completed** - Completed task history
- **docs/explain-cost-schema.md** - EXPLAIN COST parsing specification (583 lines)
- **docs/usage.md** - Programmatic API usage guide

> **Note**: `PLAN.md` has been archived (removed from working tree; preserved in git history at commit prior to removal). It contained scaffolding code for phases 0–4 that is now superseded by the working implementation in `src/` and the task files in `tasks/`.

---

## Interaction Modes

dburnrate targets five distinct usage contexts. All features must be available in all modes — the backend is the only variable.

| # | Mode | Where code runs | Backend | Auth |
|---|------|----------------|---------|------|
| 1 | **Local CLI, offline** | Laptop / CI | Static only | None |
| 2 | **Local CLI + Databricks** | Laptop | `RestBackend` | PAT token |
| 3 | **Databricks CLI / job** | Databricks terminal or job task | `SparkBackend` | Auto |
| 4 | **Databricks notebook, external** | Notebook analyzing *other* files | `SparkBackend` | Auto |
| 5 | **Databricks notebook, self** | Notebook analyzing *itself* | `SparkBackend` + path detection | Auto |

### Feature Parity Matrix

| Feature | Mode 1 | Mode 2 | Mode 3 | Mode 4 | Mode 5 |
|---------|:------:|:------:|:------:|:------:|:------:|
| `estimate("SELECT ...")` | Static | Hybrid | Hybrid | Hybrid | Hybrid |
| `estimate_file("file.sql")` | Static | Hybrid | Hybrid | Hybrid | Hybrid |
| `estimate_notebook("path")` | Static | Hybrid | Hybrid | Hybrid | ✓ |
| `estimate_current_notebook()` | — | — | ✓ | — | ✓ |
| `estimate_cells()` — per-cell breakdown | — | — | — | — | ✓ |
| `display()` — rich table in notebook output | — | — | — | — | ✓ |
| `lint` — anti-pattern detection | ✓ | ✓ | ✓ | ✓ | ✓ |
| `audit` — hidden cost audit | — | ✓ | ✓ | ✓ | ✓ |
| `waste` — idle cluster detection | — | ✓ | ✓ | ✓ | ✓ |
| `advise` — compute type advisor | ✓ | ✓ | ✓ | ✓ | ✓ |

### Python API (all modes)

```python
import dburnrate

# Single SQL string or code snippet
estimate = dburnrate.estimate("SELECT * FROM sales JOIN customers ON sales.cust_id = customers.id")

# File on disk (.sql, .py, .ipynb, .dbc)
estimate = dburnrate.estimate_file("./queries/daily_etl.sql")

# Notebook by explicit path (Modes 1–4)
estimate = dburnrate.estimate_notebook("/Workspace/Users/me/etl.ipynb")

# Current notebook — path auto-detected (Modes 3 and 5)
estimate = dburnrate.estimate_current_notebook()

# Per-cell breakdown of current notebook (Mode 5)
cells = dburnrate.estimate_cells()      # list[CellEstimate]
for cell in cells:
    print(f"Cell {cell.index} ({cell.language}): {cell.estimated_dbu:.4f} DBU — {cell.summary}")

# Rich table in notebook output (Mode 5)
dburnrate.display()          # full notebook cost breakdown
dburnrate.display(cell=5)    # single cell detail
```

### CLI (all modes, same commands)

```bash
dburnrate estimate "SELECT ..."
dburnrate estimate ./notebook.ipynb
dburnrate estimate ./notebook.ipynb --breakdown    # per-cell table
dburnrate estimate "queries/*.sql"                 # glob (batch)
dburnrate estimate --self                          # current notebook/script (Modes 3, 5)
dburnrate lint ./queries/
dburnrate audit --days 30
dburnrate waste --days 7
dburnrate advise "SELECT ..." --current-sku ALL_PURPOSE
```

### Mode 5: Current Notebook Path Detection

```python
def current_notebook_path() -> str | None:
    """Detect path of currently-running notebook. Returns None if not in a notebook."""

    # 1. Databricks SparkConf — most reliable inside DBR
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark:
            path = spark.conf.get("spark.databricks.notebook.path", None)
            if path:
                return path
    except Exception:
        pass

    # 2. dbutils context — interactive Databricks notebooks
    try:
        import IPython
        ip = IPython.get_ipython()
        if ip:
            dbutils = ip.user_ns.get("dbutils")
            if dbutils:
                return dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    except Exception:
        pass

    # 3. ipynbname — local Jupyter (optional dep)
    try:
        import ipynbname
        return str(ipynbname.path())
    except Exception:
        pass

    # 4. __file__ — script context (Mode 3 in a .py job)
    import inspect
    for frame_info in inspect.stack():
        f = frame_info.filename
        if f and not f.startswith("<") and f != __file__:
            return f

    return None
```

`estimate_current_notebook()` calls `current_notebook_path()` then delegates to `estimate_notebook()` — no separate implementation needed. The `--self` CLI flag does the same.

### `CellEstimate` model

```python
@dataclass
class CellEstimate:
    index: int                  # cell number (1-based)
    language: str               # "sql" | "python" | "scala" | "markdown"
    source: str                 # cell source code
    estimated_dbu: float
    cost_usd: float
    confidence: str             # "high" | "medium" | "low"
    summary: str                # one-line description of most expensive operation
    anti_patterns: list[AntiPattern]
```

`dburnrate.estimate_cells()` returns `list[CellEstimate]` with a `.total` property summing the notebook. `dburnrate.display()` renders it as a rich table using `rich` (already a dep) or `displayHTML()` when inside Databricks.

---

## Design Principles Summary

1. **Accuracy first** - Fix bugs before adding features; calibrate before shipping estimates
2. **Layered fidelity** - Static → Delta → EXPLAIN → Historical → ML
3. **Hybrid architecture** - EXPLAIN + Delta + fingerprinting + ML via `EstimationPipeline`
4. **Dual-mode runtime** - `SparkBackend` (in-cluster) + `RestBackend` (external), auto-detected
5. **Enterprise-ready** - Configurable `TableRegistry` for governance view environments
6. **Total cost, not DBU-only** - Include VM infrastructure for classic compute
7. **Empirically grounded** - All constants must cite source or benchmark; no fabricated values
8. **Full parity across 5 modes** - Every feature available in every context; backend is the only variable

*Document version: 1.3 | Last updated: March 2026 | Audit: files/00-EXECUTIVE-SUMMARY.md*
