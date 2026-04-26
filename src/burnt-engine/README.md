# burnt-engine

Rust/PyO3 engine that powers `burnt check`. Provides tree-sitter AST parsing,
cost-graph construction, and a multi-tier lint rule pipeline for Databricks
PySpark and SQL.

---

## Building

```bash
# Development build (installs the extension module into the active venv)
pip install maturin
maturin develop

# Release build
maturin develop --release

# Run Rust unit tests
cd src/burnt-engine
cargo test

# Lint (zero warnings expected)
cargo clippy -- -D warnings
```

---

## Module Map

```
ingestion/      Read files and notebooks from disk → SourceFile / Cell
parse/          tree-sitter Python/SQL parsers; notebook cell splitter
graph/          CostGraph (Python/SQL) and PipelineGraph (DLT) builders
  python.rs     Walk Python AST → CostNode/CostEdge + SemanticModel findings
  sql.rs        Walk SQL AST via sqlparser-rs → CostNode/CostEdge
  dlt.rs        DLT-specific pipeline graph builder
semantic/       SemanticModel: variable bindings, scope stack, shadow detection
rules/          Three-tier rule pipeline
  query.rs      Tier 1 — tree-sitter S-expression pattern matching
  context.rs    Tier 2 — loop/naming/chain context checks (HashMap dispatch)
  dataflow.rs   Tier 3 — cache lifecycle and multi-action tracking
  finding.rs    Shared make_finding() helper
  registry      Code-generated rule registry (built from TOML files in rules/)
detect.rs       Auto-detect language mode from source text
types.rs        Core types: Finding, CostNode, CostEdge, AnalysisResultPy, …
lib.rs          PyO3 entry points exposed to Python
```

---

## Rule TOML Format

Rules live in `rules/<category>/<language>/` as `.toml` files.

```toml
[rule]
id          = "collect_without_limit"   # snake_case unique ID
code        = "BP008"                   # display code
severity    = "error"                   # error | warning | info
language    = "python"                  # python | sql | all
description = "collect() without limit() can OOM the driver"
suggestion  = "Add .limit(n).collect() or use .take(n)"
category    = "Performance"
tags        = ["pyspark", "memory", "driver-bound"]

# Tier 1 — tree-sitter S-expression pattern (optional)
[query]
detect  = """
(call
  function: (attribute
    attribute: (identifier) @method)
  (#eq? @method "collect"))
"""
exclude = """            # optional — suppresses rule when this also matches
(call ...)
"""

# Tier 2 — context check implemented in context.rs (optional)
[context]
enabled = true           # routes rule_code to analyze_context_for_rule()

# Tier 3 — dataflow check implemented in dataflow.rs (optional)
[dataflow]
enabled = true           # routes rule_code to check_dataflow_rules()

# Inline test cases verified at build time
[tests]
pass = ["df.collect()"]
fail = ["df.limit(100).collect()"]
```

A rule may have any combination of `[query]`, `[context]`, and `[dataflow]`
sections. Rules with only `[context]` or `[dataflow]` must omit `[query]`.

---

## Adding a New Rule

1. Create `rules/<category>/<language>/BPXXX_my_rule.toml` following the
   format above.
2. Add a `[tests]` section with at least one `pass` and one `fail` example.
3. Run `cargo test` — the build script re-generates the registry and runs
   inline tests automatically.
4. If Tier 2 or Tier 3 logic is needed, add the handler function to
   `context.rs` or `dataflow.rs` and register it in the dispatch map.
