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

burnt parses Spark pipelines — Python, SQL, or DLT/SDP — and shows you what each operation costs: statically before the job runs, with live metrics while it runs, and as a CI gate before it ships.

Works on **Databricks today**. EMR, Glue, Dataproc, and on-prem Spark next.

```python
import burnt
burnt.check().display()
```

```
daily_pipeline.py  │  Python  │  6 cells  │  2 via %run

⏱ 22 min session │ 4 min code │ 18 min idle │ 18% util
💰 Code: $3.20 │ Idle: $14.80 │ Total: $18.00

  Cost: $18.45/run (24.6 DBU)  │  HIGH confidence

  spark.table("orders")         4.2GB scan      $1.20    6%
  spark.table("dim_products")   340MB scan      $0.08    0%
  crossJoin(dim_products)       1.4TB !!       $11.40   59%
  groupBy("region").agg()       shuffle SPILL   $4.80   25%
  toPandas()                    2.1GB driver    $0.60    3%
  write.saveAsTable()           800MB write     $0.17    1%

  ✗ BP007  line 34  CROSS JOIN → O(n×m)        $11.40
  ⚠ BP004  line 78  toPandas() full dataset     $0.60

  → Replace crossJoin: saves $12.36 (67%)
  → Jobs Compute: saves $5.11 (28%)
  → Serverless: kills $14.80 idle cost
```

---

## Three Modes

Auto-detected. One command.

**Static lint** — run offline, no credentials, no Spark. 84 rules fire immediately.

**In-notebook coaching** — attach to a live Spark session; `burnt.check()` correlates actual stage metrics to your source lines.

**CI gate** — block PRs on cost regressions or lint errors using `--output sarif` or `--max-cost`.

```
orders_pipeline.py  │  DLT PRO  │  3 tables

  Pipeline: $8.60/run  │  Overhead: 12%

  bronze_orders   STREAMING    $0.40/batch
  └→ silver_orders MAT. VIEW   $2.80/run
     └→ gold_revenue MAT. VIEW  $5.40/run

  ⚠ DLT001  silver_orders could be STREAMING → saves $2/run
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

> **Current status (v0.2.0-dev):** Static lint (84 rules) and CostGraph (compute-seconds)
> are fully operational. Live sparkMeasure runtime capture is wired but in active
> development (PX/02). Dollar estimates require a pricing-backend extra (Phase N).

### Install matrix

| Install | Lint (84 rules) | Compute-seconds | Live runtime | Dollars | System-table enrichment | HTML / .dbc |
|---------|:--------------:|:---------------:|:------------:|:-------:|:----------------------:|:-----------:|
| `pip install burnt` | ✅ + `--fix` + `--diff` | ✅ | ✅ core | ❌ | ❌ | ❌ |
| `+ [onprem-spark]` | ✅ | ✅ | ✅ | ✅ user-supplied rates | ❌ | ❌ |
| `+ [databricks]` alone | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ |
| `+ [azure-databricks]` | ✅ | ✅ | ✅ | ✅ Azure DBU + VM | ✅ | ❌ |
| `+ [aws-databricks]` | ✅ | ✅ | ✅ | ✅ AWS DBU + EC2 | ✅ | ❌ |
| `+ [gcp-databricks]` | ✅ | ✅ | ✅ | ✅ GCP DBU + GCE | ✅ | ❌ |
| `+ [notebook]` | ✅ | ✅ | ✅ | per other extras | per other extras | ✅ |
| `[all]` | ✅ | ✅ | ✅ | ✅ selected backend | ✅ | ✅ |

Every `[*-databricks]` extra auto-pulls `[databricks]` (workspace API + system tables). `[onprem-spark]` is self-contained — configure your own `$/vCPU-hour` in `burnt.toml`.

---

## Notebook API

```python
import burnt

burnt.start_session()   # attach sparkMeasure to the active SparkSession

# ... run your Spark code ...

result = burnt.check()
result.display()
result.findings         # list[Finding]
result.to_json()        # dict
result.to_markdown()    # str
result.to_sarif()       # SARIF 2.1.0 dict
result.to_html()        # requires pip install burnt[notebook]
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

burnt rules                     # Browse all 84 rules (interactive TUI)
burnt init                      # Generate burnt.toml
burnt doctor                    # Check config, Spark availability, system-table access
```

---

## Config

Standalone `burnt.toml`, or `[tool.burnt]` in `pyproject.toml` — same discovery as ruff.

**`burnt.toml`:**
```toml
[lint]
ignore = ["BNT_001"]
fail-on = "warning"

[burnt.pricing]
backend = "azure-databricks"   # auto if only one pricing extra installed

[burnt.onprem_spark]
cost_per_vcpu_hour  = 0.048
cost_per_gb_hour    = 0.006
cost_per_gb_shuffle = 0.001

[burnt.databricks.system_tables]
enabled = true   # set false to skip system-table queries entirely
# query_history = "prod_observability.query_history"  # override if mirrored
```

Discovery: walks up from target path looking for `burnt.toml`, `.burnt.toml`, or `pyproject.toml` with `[tool.burnt]`. Falls back to `~/.config/burnt/burnt.toml`.

---

## 84 Rules

```
ERROR  BP001   collect() without limit
ERROR  BP007   CROSS JOIN → O(n×m)
WARN   DLT001  MV could be streaming
WARN   BSQ002  SELECT * in final SQL cell
```

Three tiers: Tier 1 (TOML + tree-sitter query, no Rust needed), Tier 2 (Rust context-aware), Tier 3 (Rust semantic/dataflow). Six categories: Performance (`BP*`), SQL quality (`BQ*`, `SQ*`), Delta (`BD*`), DLT/SDP (`SDP*`), Notebook style (`BNT_*`), Notebook structure (`BB*`, `BN*`).

---

## Architecture

```
CLI: burnt check                 Notebook: burnt.check()
      │                                │
  Rust engine (PyO3)          Rust engine (same)
  84 rules, CostGraph         + sparkMeasure enrichment
  tree-sitter Py/SQL/DLT      + PricingBackend (optional)
                                    │
                          ┌─────────┴──────────┐
                          │                    │
                   [databricks]         [onprem-spark]
                 [azure-databricks]    user $/vCPU-hour
                 [aws-databricks]
                 [gcp-databricks]
```

Rust engine: tree-sitter Python + SQL, `%run` resolution, mode detection, semantic model, CostGraph, 84 rules.
Python: sparkMeasure session wrapper, graph enrichment, cost estimation via `PricingBackend`, display, CLI.
Core install: zero cloud SDK, zero credentials required.

---

## Contributing

Tier 1 rules = TOML + tree-sitter query. No Rust required.

1. `src/burnt-engine/rules/{performance,sql,delta,sdp,notebook,style}/BXXX_rule.toml`
2. Fixture in `tests/fixtures/tier1/`
3. `cargo test tier1_rules`
4. PR

See `docs/writing-rules.md` for the full rule format including the `[fix]` section for autofixable rules.

---

## Development

```bash
cd src/burnt-engine && maturin develop --release && cargo test
uv sync --all-extras && uv run pytest -m unit -v && uv run ruff check src/ tests/
```

---

## License

[GPL-3.0](LICENSE)
