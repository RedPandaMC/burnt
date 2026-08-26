<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="public/logo_text_dark.svg">
  <img src="public/logo_text.svg" alt="burnt" width="400">
</picture>

**Cost Compiler for Spark**

Per-operation, per-table, per-dollar.

[![Python](https://img.shields.io/badge/python-3.10+-blue)](https://www.python.org/)
[![Rust](https://img.shields.io/badge/engine-rust-orange)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MPL--2.0-blue)](LICENSE)

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

Runs offline — zero credentials, no Spark required. 110 rules fire immediately.

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

> **Current status (v0.3.0):** Static lint (110 rules), unified PyGraph, REST session
> enrichment, and four pricing backends (Azure, AWS, GCP, on-prem) are fully operational.
> All parsing uses tree-sitter (Python + SQL) — no legacy string-match heuristics.

### Install matrix

| Install | Lint (110 rules) | Compute-seconds | Dollars | System tables |
|---------|:--------------:|:---------------:|:-------:|:-------------:|
| `pip install burnt` | ✅ + `--fix` + `--diff` | ✅ | ❌ | ❌ |
| `+ [azure-databricks]` | ✅ | ✅ | ✅ Azure DBU + VM retail prices | ✅ workspace API |
| `+ [aws-databricks]` | ✅ | ✅ | ✅ AWS EC2 bulk pricing | ✅ workspace API |
| `+ [gcp-databricks]` | ✅ | ✅ | ✅ GCP Compute catalog (API key required) | ✅ workspace API |
| `+ [onprem-spark]` | ✅ | ✅ | ✅ user-supplied rates in `burnt.toml` | ❌ |
| `[all]` | ✅ | ✅ | ✅ selected backend | ✅ |

All cloud pricing extras install `[databricks]` as a transitive dependency (workspace API + system tables). Azure and AWS use free unauthenticated APIs. GCP requires a free Cloud Billing API key set as `GCP_BILLING_API_KEY` or `BURNT_GCP_API_KEY`. `[onprem-spark]` is self-contained — configure `$/vCPU-hour` in `burnt.toml`.

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

burnt rules                     # Browse all 110 rules (interactive TUI)
burnt init                      # Generate burnt.toml
burnt doctor                    # Check config, Spark availability, system-table access
burnt cache clear               # Clear the analysis cache

# Pricing backends
burnt pricing list-backends     # Show available pricing backends
burnt pricing refresh           # Force-refresh pricing data from cloud APIs
burnt pricing list-instances    # List available instance types from the active backend
burnt pricing estimate          # Estimate cost for a given compute time and instance type
```

---

## Config

Standalone `burnt.toml`, or `[tool.burnt]` in `pyproject.toml` — same discovery as ruff. Walks up from the target path; falls back to `~/.config/burnt/burnt.toml`.

```toml
[lint]
ignore = ["BNT_001"]
fail-on = "warning"

[burnt.pricing]
backend  = "azure-databricks"   # auto-detected if only one pricing extra is installed
currency = "USD"

[burnt.onprem]
cost_per_vcpu_hour  = 0.048
cost_per_gb_hour    = 0.006
cost_per_gb_shuffle = 0.001
total_vcpus         = 16
total_memory_gb     = 64.0

[burnt.databricks.system_tables]
enabled = true   # set false to skip system-table queries entirely
# query_history = "prod_observability.query_history"  # override if mirrored
```

---

## 110 Rules

Eight categories covering performance, SQL quality, Delta, DLT/SDP pipelines, streaming,
security, Unity Catalog governance, and notebook style. Every rule is a graph-DSL
pattern — see [`docs/anti-pattern-rules.md`](docs/anti-pattern-rules.md) for the complete
index, and [`docs/dsl-reference.md`](docs/dsl-reference.md) for the pattern language.

```
─── Performance (BP*)  ────────────────────────────────────────────────────
 error   BP008  collect() without limit — can OOM the driver
 error   BP010  Python UDF — serialisation overhead; use Spark SQL or a pandas UDF
 error   BP011  toPandas() without limit — moves the full dataset to the driver
 warning BP012  repartition(1) — forces all data through a single task
 warning BP014  crossJoin — O(n×m) row explosion; add an explicit join key
 warning BP020  withColumn() inside a loop — O(n²) Catalyst plan analysis
 warning BP023  Window.orderBy() without partitionBy() — unintended global sort
 + 60 more BP*, BNT*, BO*, BC*, AQE, caching, RDD, Photon, Pandas-on-Spark rules

─── SQL Quality (BQ*, SQ*)  ───────────────────────────────────────────────
 warning BQ001  NOT IN with subquery — drops rows silently when subquery returns NULL
 warning BQ002  UNION without ALL — expensive dedup sort; use UNION ALL if safe
 error   BQ004  correlated subquery — re-executed once per outer row
 warning SQ001  SELECT * — schema drift silently breaks downstream consumers
 warning BP013  ORDER BY without LIMIT — sorts the full dataset before any filtering

─── Delta / Lakehouse (BD*)  ──────────────────────────────────────────────
 warning BD001  VACUUM called too frequently — imposes unnecessary read overhead
 warning BD010  mode('overwrite') without replaceWhere — full table replacement
 warning BD016  .write inside a loop — one small file per iteration
 + 9 more BD* rules covering OPTIMIZE, MERGE, CONVERT TO DELTA, Liquid Clustering

─── Declarative Pipelines / DLT / SDP (SDP*)  ─────────────────────────────
 warning SDP001  table missing @expect — no data quality contract
 warning SDP003  streaming source without schema — breaks on schema evolution
 + 4 more SDP* rules (fire on @dlt.table, @dp.table, and @sdp.table decorators)

─── Streaming (BS*)  ──────────────────────────────────────────────────────
 error   BS001  writeStream without checkpointLocation — loses progress on restart
 warning BS003  event-time aggregation without watermark — unbounded state

─── Security & Governance (BT*, BU*)  ─────────────────────────────────────
 error   BT002  hardcoded AWS/Databricks credentials in source
 error   BT003  JDBC URL with embedded password
 error   BU003  hardcoded DBFS/cloud storage paths (deprecated on Databricks)
 warning BU001  two-part table name omits Unity Catalog prefix
```

Run `burnt rules` for the full list with descriptions, examples, and fix suggestions.

---

## Architecture

```
CLI: burnt check                 Notebook: burnt.check()
      │                                │
   Rust engine (PyO3)          Rust engine (same)
   110 rules, PyGraph        + REST API enrichment
   graph-DSL over AST          + providers/ (optional)
   tree-sitter Python/SQL              │
                           ┌─────────┴──────────┐
                           │                    │
                    [azure-databricks]    [onprem-spark]
                    [aws-databricks]       user $/vCPU-hour
                    [gcp-databricks]
```

**Parsing:** All source analysis is CST-based via tree-sitter. Python files use [tree-sitter-python](https://github.com/tree-sitter/tree-sitter-python); SQL files use [tree-sitter-sql-extended](https://github.com/RelativelyUnknown/tree-sitter-sql-extended), a fork of DerekStride's grammar extended with Databricks DDL (`CREATE STREAMING TABLE`, `OPTIMIZE … ZORDER BY`, `VACUUM`, Unity Catalog statements, and more).

**Rust engine:** tree-sitter Python + [tree-sitter-sql-extended](https://github.com/RelativelyUnknown/tree-sitter-sql-extended) (Databricks/Spark/UC grammar), unified `PyGraph` with optional REST/Spark plan enrichment, graph-DSL rule engine, 110 rules.  
**Python layer:** Spark monitoring REST client, graph enrichment, cost estimation via `ProviderBackend` (providers/), Rich display, typer CLI.  
**Core install:** zero cloud SDK, zero credentials required.

---

## Contributing

### Adding a rule

1. Create `src/burnt-engine/rules/{performance,sql,delta,sdp,notebook,style,streaming,governance}/BXXX_rule_name.toml`
2. Write a `[graph]` DSL block — see [`docs/dsl-reference.md`](docs/dsl-reference.md)
3. Add `pass` / `fail` test cases in `[tests]`
4. Run `cargo test -p burnt-engine`
5. Open a PR

No Rust required for standard rules. Complex predicates that need new engine capabilities go in `src/burnt-engine/src/rules/graph_dsl/predicate.rs`.

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

[MPL-2.0](LICENSE)
