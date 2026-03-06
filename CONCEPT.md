# Designing a Databricks cost estimation package for Azure

**No open-source tool currently estimates Databricks costs before execution.** Every existing solution — SparkMeasure, Unravel, Acceldata, even Databricks' own system tables — operates retrospectively. This gap defines the core opportunity for a custom Python package that combines static code analysis with live billing data to predict, attribute, and forecast Azure Databricks spend. The technical foundations are mature: SQLGlot natively parses Databricks SQL into rich ASTs, system tables expose granular billing and query metrics, and the pricing model is fully programmable via `system.billing.list_prices`. What follows is a complete architecture grounded in the latest tooling, pricing, and API surfaces as of early 2026.

---

## Static analysis turns SQL and PySpark into cost signals

The package's highest-value capability is estimating DBU consumption from code structure alone, before any cluster spins up. Two complementary parsers handle this.

**SQLGlot (v28.10.1)** is the SQL parsing backbone. Databricks is an officially supported dialect — not a community plugin — with first-class support for `MERGE INTO`, `COPY INTO`, `CREATE STREAMING TABLE`, JSON colon extraction (`col:path`), `CLUSTER BY` (Liquid Clustering), and Delta DML. The parser produces a fully traversable AST where every join, aggregation, window function, and subquery is a typed expression node (`exp.Join`, `exp.AggFunc`, `exp.Window`, `exp.Merge`, etc.). SQLGlot has zero dependencies, runs on Python 3.9+, and offers a Rust-accelerated tokenizer (`sqlglotrs` v0.7.3) for batch workloads. Key limitations: `OPTIMIZE` and `ZORDER BY` parse as generic `Command` objects rather than first-class nodes, and some Delta table properties pass through without validation. SQLGlot performs no cost estimation or plan simulation — that logic must be built on top.

**Python's `ast` module** handles PySpark cost signals. A custom `ast.NodeVisitor` detects method calls (`.groupBy()`, `.join()`, `.collect()`, `.toPandas()`, `.repartition()`), decorator patterns (`@udf` vs `@pandas_udf`), and embedded SQL via `spark.sql()` strings that can be routed to SQLGlot. The existing library `pyspark-ast-lineage` demonstrates table lineage extraction via AST, but **no off-the-shelf PySpark anti-pattern detector exists** — this must be purpose-built. Anti-patterns to flag include `collect()` without prior `limit()`, Python UDFs instead of Pandas UDFs (10–100× overhead), `.crossJoin()`, `.repartition(1)`, and `toPandas()` on unbounded datasets.

**Notebook parsing** requires handling two formats. Jupyter `.ipynb` files are JSON with a well-defined cell schema, parseable via `nbformat` or raw `json`. Databricks `.dbc` archives are ZIP files containing JSON notebooks that may be plain UTF-8, Base64-encoded, or gzip-compressed. Community tools `dbcexplode`, `dbc-to-py`, and `dbc-to-ipynb` handle unpacking, but the logic is straightforward enough to inline. Magic commands (`%sql`, `%python`, `%scala`) identify cell languages; in source-format exports, cells are delimited by `# COMMAND ----------` markers.

A static complexity scoring model maps parsed patterns to relative cost weights. Research from Baldacci & Golfarelli (2018), Le et al. (2022, ~7.7% average error on TPC-H), and Databricks' own CBO documentation establishes that **shuffle-inducing operations dominate cost**: `MERGE INTO` (join + full rewrite), SortMergeJoin, global `ORDER BY`, and high-cardinality `GROUP BY` are the most expensive. A practical weight table:

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

The estimated DBU cost is then `complexity_score × data_volume_factor × cluster_config_factor × dbu_rate`, where the latter three come from system tables and pricing data.

---

## System tables provide the live data backbone

Databricks now exposes **30+ system tables** across billing, compute, query history, lakeflow, serving, MLflow, and storage schemas. The three essential tables for cost estimation are fully documented with stable schemas.

**`system.billing.usage`** (GA, 365-day retention, global) records every billable event with `usage_quantity` in DBUs, `sku_name` identifying the compute type, and a rich `usage_metadata` struct containing `cluster_id`, `warehouse_id`, `job_id`, `job_run_id`, `notebook_id`, `dlt_pipeline_id`, and `endpoint_name`. The `billing_origin_product` column (values like `JOBS`, `SQL`, `INTERACTIVE`, `MODEL_SERVING`, `PREDICTIVE_OPTIMIZATION`) disambiguates products sharing the same SKU. The `identity_metadata.run_as` field enables per-user cost attribution. Custom tags on clusters and serverless budget policies propagate to the `custom_tags` map for team/department attribution.

**`system.billing.list_prices`** (GA, indefinite retention) contains historical pricing by SKU, cloud, and time period. The critical join pattern multiplies `usage_quantity × pricing.effective_list.default` to convert DBUs to dollars:

```sql
SELECT u.sku_name, SUM(u.usage_quantity * lp.pricing.effective_list.default) as cost_usd
FROM system.billing.usage u
JOIN system.billing.list_prices lp
  ON lp.sku_name = u.sku_name
  AND u.usage_end_time >= lp.price_start_time
  AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
WHERE u.usage_date >= current_date - INTERVAL 30 DAYS
GROUP BY u.sku_name
```

**`system.query.history`** (Public Preview, 365-day retention) captures SQL warehouse and serverless query execution with `statement_text`, `execution_duration_ms`, `total_task_duration_ms`, `read_bytes`, `read_rows`, `spilled_local_bytes`, `network_sent_bytes`, `written_bytes`, and cache hit rates. The `query_source` struct links queries to notebooks, jobs, dashboards, and alerts. There is **no direct join key** to billing.usage — correlation works through shared identifiers like `warehouse_id`, `notebook_id`, or `job_id` with time-window alignment.

Key tables added in 2024–2025 include the **`system.lakeflow.*`** schema (June 2024) with six tables for job and pipeline monitoring, **`system.compute.node_timeline`** for minute-by-minute utilization metrics (90-day retention), **`system.serving.endpoint_usage`** for token-level model serving tracking, **`system.storage.predictive_optimization_operations_history`** tracking automatic COMPACTION/VACUUM/CLUSTERING with estimated DBU consumption, and **`system.mlflow.*`** for experiment tracking. The compute tables include `system.compute.node_types` which maps instance types to hardware specs — essential for the DBU-per-instance lookup.

---

## Azure pricing is a dual-bill model with serverless convergence

Azure Databricks bills two components: **DBUs for the platform** and **Azure VMs for infrastructure** (except serverless, which bundles both). All rates below are Premium tier, US East, pay-as-you-go. Standard tier is being retired October 2026.

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

Each Azure VM instance type has a fixed DBU-per-hour rate that scales with compute power. Representative mappings: **Standard_DS3_v2** (4 vCPU, 14 GiB) = 0.75 DBU/hr; **Standard_DS4_v2** (8 vCPU, 28 GiB) = 1.50 DBU/hr; **Standard_D64s_v3** (64 vCPU, 256 GiB) = 12.0 DBU/hr. The official mapping is available at `system.compute.node_types` and the Databricks pricing calculator.

**Photon** on classic clusters applies a **2.5× DBU multiplier** on Azure (2.9× on AWS) with no change to the per-DBU dollar rate. On SQL Warehouses and serverless compute, Photon is enabled by default with no extra charge. The breakeven point for Photon on classic clusters is a **2× runtime speedup** — independent benchmarks show **2.7× average speedup** for complex SQL (joins, aggregations, window functions), making it cost-effective for transform-heavy workloads but **cost-negative for simple appends and maintenance operations**.

**Serverless compute** (GA 2024–2025) eliminates VM management. Serverless SQL Warehouses consume fixed DBUs per hour by size (Small = 12 DBU/hr = $8.40/hr all-in). For steady-state workloads, community reports indicate serverless can cost **2–5× more** than optimized classic clusters, but it eliminates idle cluster costs for bursty workloads. Committed-use discounts (DBCU) offer up to **33% savings** on 1-year terms and **37% on 3-year terms**, applicable across all workload types.

The package should store these rates in a versioned configuration file, with `system.billing.list_prices` as the authoritative runtime source for any workspace-specific pricing (including negotiated discounts reflected in the `effective_list` field).

---

## Forecasting requires per-SKU models with business-event awareness

Cost projection from billing data is a time-series problem with strong seasonality and regime changes. Databricks itself provides a Prophet-based demo notebook that trains separate models per SKU and workspace — the right architecture since each has distinct patterns.

**Prophet** remains the pragmatic default for daily/weekly/yearly seasonality, holiday calendars, and trend changepoints. It outputs Bayesian uncertainty intervals (`yhat_lower`, `yhat_upper`) at configurable widths. **NeuralProphet** (PyTorch-based) offers **55–92% accuracy improvement** over Prophet for short-medium forecasts and supports Conformalized Quantile Regression (CQR) for mathematically guaranteed coverage intervals regardless of distribution. For a minimal package, Prophet with optional NeuralProphet upgrade is the right layering.

Key implementation decisions:

- **Train per SKU × workspace** to capture distinct patterns (batch ETL vs interactive SQL vs model serving)
- **Normalize for business events** as explicit regressors (fiscal close, marketing campaigns, data refreshes)
- **Handle regime changes** when clusters are resized or workloads migrate to serverless — document infra changes as changepoints
- **Target <15% MAPE** for mature forecasting; FinOps Foundation benchmarks show best-in-class organizations achieve <10% overrun frequency with <20% deviation
- **Confidence intervals** via Prophet's Bayesian uncertainty (default) or NeuralProphet's CQR for harder guarantees

The forecasting module should be an optional dependency group (`pip install dbcost[forecasting]`) since Prophet pulls in `cmdstanpy` and ~200 MB of compiled Stan models.

---

## What-if modeling quantifies optimization decisions

The package should support four primary what-if scenarios, each grounded in empirical data.

**Photon toggle**: Model cost as `(current_DBU × 2.5) / speedup_factor`. Independent benchmarks (Miles Cole, April 2024) show complex joins achieve 2–4× speedup (net cost reduction of 20–57%), aggregations achieve 4× speedup, but simple inserts show zero speedup and **72% cost increase**. The package should classify queries by type and apply per-category speedup factors rather than a single multiplier.

**Liquid Clustering migration**: Databricks reports **2–12× read performance improvement** and **7× faster writes** versus partitioning + Z-ORDER. Liquid Clustering only rewrites unclustered ZCubes (incremental), while Z-ORDER rewrites entire partitions (full). The write-cost reduction alone can justify migration for tables with frequent OPTIMIZE runs. Decision heuristic: use Liquid Clustering for all new tables, migrate existing tables when partition cardinality exceeds ~5,000 or query patterns evolve. Tables >500 TB warrant Databricks consultation.

**Cluster right-sizing**: Four bottleneck types drive different scaling strategies. **CPU-bound** (>90% utilization, heavy UDFs) → scale cores, use compute-optimized C-series. **Memory-bound** (spill to disk, OOM) → scale memory, use E/R-series. **I/O-bound** (slow reads, small files) → fix data layout first, then use storage-optimized L-series. **Shuffle-bound** (high network transfer) → scale out to more nodes. Switching from fixed-size to autoscaling clusters alone can cut costs **40–60%**; adding spot instances saves an additional 60–80% on VM costs.

**Serverless migration**: Compare `current_classic_cost = DBU_rate × DBU_count + VM_cost` against `serverless_cost = serverless_DBU_rate × estimated_DBU_count`. Serverless eliminates idle cluster costs but has higher per-DBU rates. The breakeven depends on cluster utilization — workloads with <30% active compute time typically benefit from serverless.

---

## The competitive landscape leaves pre-execution estimation wide open

The existing tool ecosystem has clear layers, and a custom package should target the gap between them.

**Databricks native features** now handle retrospective cost visibility well: system tables, AI/BI dashboards (importable from account console), Budgets API (Public Preview since mid-2024), compute policies, serverless budget policies, and predictive optimization. The open-source **Cloud Infra Cost Field Solution** (`databricks-solutions/cloud-infra-costs`) unifies DBU + Azure VM costs into a single dashboard. **Overwatch** (formerly the go-to Labs project) is officially deprecated — system tables have subsumed its role.

**Open-source tools** don't address cost estimation. SparkMeasure (v0.25) collects Spark task/stage metrics via the Listener interface but doesn't translate to dollars. OpenCost (CNCF Incubating) announced a Databricks plugin at KubeCon 2024 but it remains early-stage. Onehouse's Spark Analyzer (2025) generates Excel reports from History Server data but is primarily a sales funnel. No significant open-source Databricks cost estimation package was released in 2024–2025.

**Commercial tools** fill the optimization layer. **Unravel Data** (Databricks Partner) offers AI-powered query optimization, code-level insights across SQL/Python/Scala, and CI/CD integration for pre-merge cost impact. **Sedai** (GA June 2025) provides autonomous cluster right-sizing via patented reinforcement learning. **Sync Computing / Capital One Slingshot** builds per-job ML models for cluster configuration optimization, claiming up to 50% savings. **Acceldata** combines cost intelligence with data quality observability, claiming 96–98% forecast accuracy. **CloudZero** aggregates multi-vendor costs with business-dimension allocation (cost per customer/feature) but lacks Databricks-specific optimization.

What none of these tools provide — and what the custom package should own — is **pre-execution cost estimation from code**, **what-if scenario modeling with quantified tradeoffs**, and a **reusable Python library** (not a SaaS dashboard) that developers can embed in notebooks, CI/CD pipelines, and governance workflows.

---

## Package architecture: thin core, optional heavy modules

The package targets Python ≥3.11 (supporting DBR 15.4 LTS and 16.4 LTS), uses the `src` layout, and builds with `hatchling`. Core dependencies are minimal — only `pydantic>=2.0` for configuration and data models. All analysis capabilities live behind optional dependency groups.

```
dbcost/
├── src/
│   └── dbcost/
│       ├── __init__.py
│       ├── _compat.py              # Optional import helpers
│       ├── core/
│       │   ├── models.py           # Pydantic models: CostEstimate, QueryProfile, ClusterConfig
│       │   ├── config.py           # Pydantic-settings for workspace connection
│       │   ├── pricing.py          # DBU rate lookups, list_prices integration
│       │   └── protocols.py        # Protocol classes: Estimator, Parser, DataSource
│       ├── parsers/
│       │   ├── sql.py              # SQLGlot-based SQL analysis [requires: sql]
│       │   ├── pyspark.py          # AST-based PySpark analysis (stdlib only)
│       │   ├── notebooks.py        # .ipynb + .dbc parsing (stdlib only)
│       │   └── antipatterns.py     # Anti-pattern detection rules
│       ├── tables/
│       │   ├── billing.py          # system.billing.usage queries
│       │   ├── queries.py          # system.query.history queries
│       │   ├── compute.py          # system.compute.* queries
│       │   └── attribution.py      # Cost attribution joins
│       ├── estimators/
│       │   ├── static.py           # Complexity-score-based estimation
│       │   ├── historical.py       # History-based estimation from system tables
│       │   └── whatif.py           # Scenario modeling (Photon, LC, serverless)
│       ├── forecast/
│       │   └── prophet.py          # Time-series forecasting [requires: forecasting]
│       └── cli/
│           └── main.py             # Typer CLI entry point
├── tests/
├── pyproject.toml
└── README.md
```

The `pyproject.toml` defines four extras:

```toml
[project]
name = "dbcost"
requires-python = ">=3.11"
dependencies = ["pydantic>=2.0,<3", "pydantic-settings>=2.0"]

[project.optional-dependencies]
sql = ["sqlglot>=20.0"]
forecasting = ["prophet>=1.1"]
ml = ["scikit-learn>=1.3"]
all = ["dbcost[sql,forecasting,ml]"]
```

Type safety uses Python 3.12+ patterns where the runtime supports them: PEP 695 generic syntax (`class Pipeline[T]:`), `Protocol` classes for extensibility (any object implementing `estimate(features: dict[str, float]) -> float` satisfies the `CostModel` protocol), and `TypedDict` for structured query results. Configuration uses `pydantic-settings` with environment variable loading (`DBCOST_WORKSPACE_URL`, `DBCOST_TOKEN`) for seamless Databricks integration.

The graceful import pattern ensures the core works without any optional dependency:

```python
from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import sqlglot

def _require(module: str, extra: str):
    try:
        return __import__(module)
    except ImportError:
        raise ImportError(f"Install with: pip install dbcost[{extra}]") from None
```

Installation in Databricks is a single cell: `%pip install dbcost[sql]` for code analysis, or `%pip install dbcost[all]` for the full suite. The wheel should be under 50 KB for the base install, with heavy dependencies pulled only when the corresponding extra is requested.

---

## Conclusion

The strongest design principle for this package is **layered estimation fidelity**. At the cheapest layer, static analysis scores query complexity from code alone — useful during development and code review. The next layer enriches estimates with historical data from system tables, correlating similar queries' actual DBU consumption via `query.history` metrics. The richest layer combines live pricing from `list_prices`, cluster configuration from `compute.clusters`, and time-series forecasting to project costs forward. Each layer adds accuracy but requires progressively more runtime context.

The single most impactful capability is the one no existing tool provides: **pre-execution cost estimation** that answers "what will this notebook cost to run?" before the cluster starts. By combining SQLGlot's AST (for SQL complexity), Python AST (for PySpark patterns), historical query metrics (for calibration), and live pricing data (for dollar conversion), the package can deliver actionable estimates that fit naturally into development workflows and CI/CD pipelines. Everything else — forecasting, what-if modeling, attribution dashboards — builds on this foundation.