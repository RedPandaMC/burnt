<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="public/logo_text_dark.svg">
  <img src="public/logo_text.svg" alt="burnt" width="400">
</picture>

**Cost Compiler for Spark**

Per-operation, per-table, per-dollar.

[![Python](https://img.shields.io/badge/python-3.10+-blue)](https://www.python.org/)
[![Rust](https://img.shields.io/badge/engine-rust-orange)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-GPL--3.0-blue)](LICENSE)

</div>

---

Spark pipelines are expensive and hard to reason about. One unguarded `crossJoin` can multiply your bill 50×; a `collect()` on the wrong DataFrame silently moves terabytes to the driver. burnt tells you what each operation costs — before it runs, while it runs, and as a CI gate that blocks the merge.

Works on **any Spark deployment** — Databricks, EMR, Glue, Dataproc, and on-prem.

```python
import burnt
burnt.check().display()
```

```
daily_pipeline.py  │  Python  │  6 cells  │  2 via %run

  Cost: $18.45/run (24.6 DBU)  │  HIGH confidence

  spark.table("orders")         4.2GB scan      $1.20    6%
  spark.table("dim_products")   340MB scan      $0.08    0%
  crossJoin(dim_products)       1.4TB !!       $11.40   59%
  groupBy("region").agg()       shuffle SPILL   $4.80   25%
  toPandas()                    2.1GB driver    $0.60    3%
  write.saveAsTable()           800MB write     $0.17    1%

  ✗ BP014  line 34  CROSS JOIN → O(n×m)        $11.40
  ⚠ BP011  line 78  toPandas() full dataset     $0.60

  → Replace crossJoin: saves $12.36 (67%)
```

---

## Three Modes

Auto-detected. One command.

### Static Lint

Runs offline — zero credentials, no Spark required. 43 rules fire immediately.

```bash
burnt check ./notebooks/pipeline.py
burnt check ./notebooks/ --select BP* --ignore BNT_*
burnt check ./notebooks/ --fix              # autofix where possible
burnt check ./notebooks/ --diff main        # only files changed since main
```

### In-Notebook

Attach to a live Spark session. `burnt.check()` correlates actual stage metrics — shuffle bytes, spill, executor time — back to your source lines via the Spark monitoring REST API.

```python
import burnt

burnt.start_session()   # resolves the REST endpoint; no JVM bridge required

# ... write and run your Spark code ...

result = burnt.check()
result.display()        # rich table with observed vs. estimated costs
```

### CI Gate

Block pull requests on cost regressions or lint errors.

```bash
burnt check ./notebooks/ --max-cost 25          # fail if run cost exceeds $25
burnt check ./notebooks/ --output sarif > burnt.sarif
```

```yaml
# .github/workflows/burnt.yml
- name: Lint Spark notebooks
  run: burnt check ./notebooks/ --output sarif > burnt.sarif

- name: Upload to Code Scanning
  uses: github/codeql-action/upload-sarif@v3
  with:
    sarif_file: burnt.sarif
```

```
orders_pipeline.py  │  DLT PRO  │  3 tables

  Pipeline: $8.60/run  │  Overhead: 12%

  bronze_orders   STREAMING    $0.40/batch
  └→ silver_orders MAT. VIEW   $2.80/run
     └→ gold_revenue MAT. VIEW  $5.40/run

  ⚠ SDP004  silver_orders: materialized view forces full refresh — consider incremental
```

---

## Install

```bash
pip install burnt
```

Inside Databricks:

```
%pip install burnt
```

> **Current status (v0.2.0-dev):** Static lint (43 rules), CostGraph, and REST session
> enrichment are fully operational. Dollar estimates require a pricing-backend extra;
> see the matrix below.

### Install matrix

| Install | Lint (43 rules) | Compute-seconds | Live runtime | Dollars | System-table enrichment |
|---------|:--------------:|:---------------:|:------------:|:-------:|:----------------------:|
| `pip install burnt` | ✅ + `--fix` + `--diff` | ✅ | ✅ | ❌ | ❌ |
| `+ [onprem-spark]` | ✅ | ✅ | ✅ | ✅ user-supplied rates | ❌ |
| `+ [databricks]` alone | ✅ | ✅ | ✅ | ❌ | ✅ |
| `+ [azure-databricks]` | ✅ | ✅ | ✅ | ✅ Azure DBU + VM | ✅ |
| `+ [aws-databricks]` | ✅ | ✅ | ✅ | ✅ AWS DBU + EC2 | ✅ |
| `+ [gcp-databricks]` | ✅ | ✅ | ✅ | ✅ GCP DBU + GCE | ✅ |
| `[all]` | ✅ | ✅ | ✅ | ✅ selected backend | ✅ |

Every `[*-databricks]` extra automatically installs `[databricks]` (Databricks workspace API + system tables). `[onprem-spark]` is self-contained — configure your own `$/vCPU-hour` in `burnt.toml`.

---

## Notebook API

```python
import burnt

burnt.start_session()   # attach to the active SparkSession via REST API

# ... run your Spark code ...

result = burnt.check()
result.display()
result.findings         # list[Finding]
result.to_json()        # dict
result.to_markdown()    # str
result.to_sarif()       # SARIF 2.1.0 dict
```

## CLI

```bash
burnt check notebook.py
burnt check ./notebooks/
burnt check ./notebooks/ --output json
burnt check ./notebooks/ --output sarif > burnt.sarif
burnt check ./notebooks/ --max-cost 25
burnt check ./notebooks/ --select BP* --ignore BNT_*

# Autofix (ruff-style)
burnt check ./notebooks/ --fix
burnt check ./notebooks/ --unsafe-fixes

# Diff-aware lint — only files changed since main
burnt check ./notebooks/ --diff main

burnt rules                     # Browse all 43 rules (interactive TUI)
burnt init                      # Generate burnt.toml
burnt doctor                    # Check config, Spark availability, system-table access
burnt cache clear               # Clear the analysis cache
```

---

## Config

Standalone `burnt.toml`, or `[tool.burnt]` in `pyproject.toml` — same discovery as ruff. Walks up from the target path; falls back to `~/.config/burnt/burnt.toml`.

```toml
[lint]
ignore = ["BNT_001"]
fail-on = "warning"

[burnt.pricing]
backend = "azure-databricks"   # auto-detected if only one pricing extra is installed

[burnt.onprem_spark]
cost_per_vcpu_hour  = 0.048
cost_per_gb_hour    = 0.006
cost_per_gb_shuffle = 0.001

[burnt.databricks.system_tables]
enabled = true   # set false to skip system-table queries entirely
# query_history = "prod_observability.query_history"  # override if mirrored
```

---

## 43 Rules

Six categories. Three implementation tiers.

```
─── Performance (BP*)  ────────────────────────────────────────────────────
 error   BP008  collect() without limit — can OOM the driver
 error   BP010  Python UDF — serialisation overhead; use Spark SQL or a pandas UDF
 error   BP011  toPandas() without limit — moves the full dataset to the driver
 warning BP012  repartition(1) — forces all data through a single task
 warning BP014  crossJoin — O(n×m) row explosion; add an explicit join key
 warning BP020  withColumn() inside a loop — O(n²) Catalyst plan analysis
 warning BP023  Window.orderBy() without partitionBy() — unintended global sort
 warning BP030  cache() without unpersist() — leaked cached RDD
 warning BP032  repeated actions without cache — recomputes the same DAG

─── SQL Quality (BQ*, SQ*)  ───────────────────────────────────────────────
 warning BQ001  NOT IN with subquery — drops rows silently when subquery returns NULL
 warning BQ002  UNION without ALL — expensive dedup sort; use UNION ALL if safe
 warning BQ003  COUNT(DISTINCT) at scale — consider approx_count_distinct()
 warning BQ004  correlated subquery — re-executed once per outer row
 warning SQ001  SELECT * — schema drift silently breaks downstream consumers
 warning SQ002  CROSS JOIN — explicit cartesian product without a condition
 warning BP013  ORDER BY without LIMIT — sorts the full dataset before any filtering

─── Delta / Lakehouse (BD*)  ──────────────────────────────────────────────
 warning BD001  VACUUM called too frequently — imposes unnecessary read overhead
 info    BD002  OPTIMIZE without ZORDER — add ZORDER on your most-filtered column

─── DLT / SDP Pipelines (SDP*)  ───────────────────────────────────────────
 warning SDP001  table missing @expect — no data quality contract
 warning SDP002  incremental table without primary key — causes full reprocessing
 warning SDP003  streaming source without schema — breaks on schema evolution
 warning SDP004  materialized view forces full refresh — consider incremental
 info    SDP005  table without comment — undocumented in the data catalog

─── Notebook Style (BNT_*)  ───────────────────────────────────────────────
 info  BNT_N01  generic variable name (df, df1, df2) — use a descriptive name
 info  BNT_I01  from pyspark.sql.functions import * — shadows built-ins (max, min, sum)
 info  BNT_C01  bare DataFrame reference without action — possible stale reference

─── Notebook Structure (BB*, BN*)  ────────────────────────────────────────
 warning BB001  notebook missing cluster tag — cost attribution broken
 warning BN001  %run target does not exist
 warning BN003  circular %run — notebook calls itself
```

**Tier 1** — TOML pattern + tree-sitter query; no Rust required to contribute.  
**Tier 2** — Rust context-aware checks.  
**Tier 3** — Rust semantic/dataflow analysis.

Run `burnt rules` for the full list with descriptions, examples, and fix suggestions.

---

## Architecture

```
CLI: burnt check                 Notebook: burnt.check()
      │                                │
  Rust engine (PyO3)          Rust engine (same)
  43 rules, CostGraph         + REST API enrichment
  tree-sitter Py/SQL/DLT      + PricingBackend (optional)
                                    │
                          ┌─────────┴──────────┐
                          │                    │
                   [databricks]         [onprem-spark]
                 [azure-databricks]    user $/vCPU-hour
                 [aws-databricks]
                 [gcp-databricks]
```

**Rust engine:** tree-sitter Python + SQL + DLT, `%run` resolution, mode detection, semantic scope model, CostGraph builder, 43 rules.  
**Python layer:** Spark monitoring REST client, graph enrichment, cost estimation via `PricingBackend`, Rich display, typer CLI.  
**Core install:** zero cloud SDK, zero credentials required.

---

## Contributing

### Adding a Tier 1 rule — no Rust required

1. Create `src/burnt-engine/rules/{performance,sql,delta,sdp,notebook,style}/BXXX_rule_name.toml`
2. Add a fixture in `tests/fixtures/tier1/`
3. Run `cargo test tier1_rules`
4. Open a PR

See [`docs/writing-rules.md`](docs/writing-rules.md) for the full rule format, including the `[fix]` section for autofix-capable rules.

### Adding a Tier 2 / Tier 3 rule

Tier 2 (context-aware) and Tier 3 (semantic/dataflow) rules are written in Rust. See `src/burnt-engine/src/rules/context.rs` and `src/burnt-engine/src/rules/dataflow.rs` for examples.

---

## Development

```bash
# Install all dependencies including optional extras
uv sync --all-extras

# Build the Rust engine in dev mode
cd src/burnt-engine && maturin develop --release

# Tests and lint
uv run pytest -m unit -v
uv run ruff check src/ tests/

# Rust tests (includes rule snapshot tests)
cargo test
```

---

## License

[GPL-3.0](LICENSE)
