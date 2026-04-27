<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="public/logo_text_dark.svg">
  <img src="public/logo_text.svg" alt="burnt" width="400">
</picture>

**Cost Compiler for Databricks**

Per-operation, per-table, per-dollar.

[![Python](https://img.shields.io/badge/python-3.10+-blue)](https://www.python.org/)
[![Rust](https://img.shields.io/badge/engine-rust-orange)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-GPL--3.0-blue)](LICENSE)

</div>

---

## What is this?

`burnt` parses Databricks notebooks — Python, SQL, or DLT — builds a cost graph, and shows you what each operation costs.

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

**Python** — per-operation cost. DataFrames traced across `%run` chains.

**SQL** — per-statement cost. CREATE TABLE AS, MERGE INTO, OPTIMIZE decomposed. Cross-cell table deps.

**DLT / SDP** — per-table cost. Streaming vs materialized views. DLT tier pricing.

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

Databricks:
```
%pip install burnt
```

Works without credentials — 84 lint rules run immediately. Add `pip install burnt[databricks]` for cost estimation and dollar figures.

---

## Notebook

```python
import burnt

burnt.start_session()   # attach sparkMeasure; requires pip install burnt[spark]

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

burnt rules                     # Browse all 84 rules (interactive TUI)
burnt init                      # Generate burnt.toml
burnt doctor                    # Check config and connectivity
```

---

## Config

Standalone `burnt.toml`, or `[tool.burnt]` in `pyproject.toml` — same as ruff.

**`burnt.toml`:**
```toml
[check]
ignore = ["BNT_001"]
max_cost = 50.0
severity = "warning"
```

**`pyproject.toml`:**
```toml
[tool.burnt.check]
ignore = ["BNT_001"]
max_cost = 50.0
```

Discovery: walks up from target path looking for `burnt.toml`, `.burnt.toml`, or `pyproject.toml` with `[tool.burnt]`. Falls back to `~/.config/burnt/burnt.toml`.

Priority: CLI flags > `burnt.config()` > config file > `BURNT_*` env vars > defaults.

```bash
burnt check --init    # Generate burnt.toml (or add [tool.burnt] to existing pyproject.toml)
```

---

## 84 Rules

```
ERROR  BP001   collect() without limit
ERROR  BP007   CROSS JOIN → O(n×m)
WARN   DLT001  MV could be streaming
WARN   BSQ002  SELECT * in final SQL cell
```

Three tiers: Tier 1 (TOML, no Rust needed), Tier 2 (Rust context), Tier 3 (Rust semantic).

---

## Access Levels

| Level | Output |
|-------|--------|
| Full | Graph + estimates + monitoring + alerts |
| SparkSession only | Graph + DESCRIBE + structural findings |
| REST only | Graph + Delta enrichment + medium confidence |
| Auth-only | 84 lint rules |

---

## Architecture

```
CLI: burnt check                 Python: burnt.check()
      │                                │
  Rust engine (PyO3)          Rust engine (same)
  84 rules, CostGraph         + sparkMeasure enrichment
  tree-sitter Py/SQL/DLT      + DatabricksBackend (optional)
```

Rust engine: tree-sitter Python + SQL, `%run` resolution, mode detection, semantic model, CostGraph, 84 rules across 6 categories.
Python: sparkMeasure session wrapper, graph enrichment, cost estimation, display, CLI.

---

## Contributing

Tier 1 rules = TOML + tree-sitter query. No Rust.

1. `src/burnt-engine/rules/tier1/{pyspark,sql,dlt}/BXXX_rule.toml`
2. Fixture in `tests/fixtures/tier1/`
3. `cargo test tier1_rules`
4. PR

---

## Development

```bash
cd src/burnt-engine && maturin develop --release && cargo test
uv sync && uv run pytest -m unit -v && uv run ruff check src/ tests/
```

---

## License

[GPL-3.0](LICENSE)
