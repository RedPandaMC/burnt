# Writing Rules

Rules are defined as TOML files under `src/burnt-engine/rules/`. They are
compiled into the rule registry at build time by `build.rs` — no Rust code
changes are needed for most rules.

---

## Directory Layout

```
src/burnt-engine/rules/
├── delta/                   BD* rules
├── notebook/                BB*, BN*, notebook BP* rules
├── performance/
│   ├── python/              BP* PySpark rules
│   └── sql/                 SQL performance rules
├── sdp/                     SDP* rules
├── sql/                     BQ*, SQ* rules
└── style/                   BNT-* rules
```

Place new files in the most appropriate category directory. The file name
should follow the pattern `{CODE}_{id}.toml` (e.g. `BP008_collect_without_limit.toml`).

---

## Rule File Format

Every rule file must have a `[rule]` section. Detection is provided by at
least one of `[query]`, `[context]`, or `[dataflow]`.

```toml
[rule]
id          = "collect_without_limit"   # snake_case, unique
code        = "BP008"                   # public identifier (prefix + number)
severity    = "error"                   # error | warning | info
language    = "python"                  # python | sql | notebook | all
description = "collect() without limit() can OOM the driver"
suggestion  = "Add .limit(n).collect() or use .take(n)"
category    = "BestPractice"            # arbitrary string, for grouping
tags        = ["pyspark", "memory", "driver-bound"]
```

`tags` are the primary mechanism for bulk rule selection (`--ignore memory`,
`--select pyspark`). Use existing tags where possible; see the tag vocabulary
in `docs/anti-pattern-rules.md`.

---

## Detection: Tier 1 — Tree-sitter Query

Use `[query]` for structural pattern matching. `detect` is a raw
[tree-sitter S-expression](https://tree-sitter.github.io/tree-sitter/using-parsers/queries/index.html).
`exclude` is an optional pattern that, when matched, suppresses the finding.

```toml
[query]
detect = """
; Match df.collect()
(call
  function: (attribute
    object: (_) @df
    attribute: (identifier) @method)
  (#eq? @method "collect"))
"""

exclude = """
; Do not flag df.limit().collect()
(call
  function: (attribute
    object: (call
      function: (attribute
        object: (_)
        attribute: (identifier) @limit_method))
    attribute: (identifier) @collect_method)
  (#eq? @collect_method "collect")
  (#eq? @limit_method "limit"))
"""
```

The finding is reported at the position of the first capture in `detect`.
`exclude` is checked after `detect` — if any node matches `exclude`, the
rule does not fire.

### Finding the right S-expression

Use the tree-sitter playground or the CLI:

```bash
# Parse a snippet and print the AST
python -c "
from burnt._engine import run
findings = run('df.collect()', 'python')
print(findings)
"
```

For SQL rules, use `language = "sql"` — the grammar covers standard SQL and
Databricks extensions (MERGE INTO, CREATE STREAMING TABLE, etc.).

---

## Detection: Tier 2 — Context Rules

Use `[context]` for rules that require semantic analysis: loop detection,
naming patterns, import analysis, or any check that cannot be expressed as a
single-node tree-sitter query.

```toml
[context]
enabled = true
```

Adding this section sets `has_context = true` in the compiled rule, which
causes `context::analyze_context_for_rule(&rule.code, source)` to be called
in `src/burnt-engine/src/rules/context.rs`.

**You must also add a match arm** in `analyze_context_for_rule`:

```rust
pub fn analyze_context_for_rule(rule_code: &str, source: &str) -> Vec<Finding> {
    match rule_code {
        "BP020" => check_with_column_in_loop(source),
        // ... add your rule here:
        "BPxxx" => check_my_rule(source),
        _ => vec![],
    }
}
```

Then implement `check_my_rule`:

```rust
fn check_my_rule(source: &str) -> Vec<Finding> {
    let mut findings = Vec::new();
    for (i, line) in source.lines().enumerate() {
        if line.trim().contains("something_bad") {
            findings.push(make_finding(
                "BPxxx",
                Severity::Warning,
                "Description of the problem",
                "How to fix it",
                (i + 1) as u32,
                Confidence::High,
            ));
        }
    }
    findings
}
```

Context rules do **not** need a `[query]` section (the `[context]` section
alone is enough for `build.rs` to include the rule).

---

## Detection: Tier 3 — Dataflow Rules

Use `[dataflow]` for cross-statement analysis: cache lifecycle, DataFrame
reuse, or any check that requires tracking bindings across multiple lines.

```toml
[dataflow]
enabled = true
```

Adding this section calls `dataflow::check_dataflow_rules(source)` in
`src/burnt-engine/src/rules/dataflow.rs`. Unlike context rules, all dataflow
rules share one Rust function — add detection logic there and filter by the
relevant condition.

---

## Test Cases

Every rule should include pass and fail examples. These are compiled into
`generated_tests.rs` by `build.rs` and run as part of `cargo test`.

```toml
[tests]
pass = [
    "df.limit(100).collect()",
    "df.take(10)",
]
fail = [
    "df.collect()",
    "result = spark.table('orders').collect()",
]
```

Each string is a complete code snippet (not a file path). For multi-line
snippets use `\n`:

```toml
fail = [
    "for col in columns:\n    df = df.withColumn(col, F.col(col) * 2)",
]
```

---

## Adding a New Rule — Checklist

1. Choose a code (`BP`, `BQ`, `SQ`, `BD`, `SDP`, `BN`, `BNT-`, `BB` prefix)
2. Create `src/burnt-engine/rules/{category}/{CODE}_{id}.toml`
3. Fill in `[rule]` with `tags = [...]` using the shared vocabulary
4. Add detection:
   - Pattern-only rule: add `[query]` with `detect` (and optionally `exclude`)
   - Context rule: add `[context]\nenabled = true` and implement in `context.rs`
5. Add `[tests]` with at least one pass and one fail case
6. Run `cargo build` — the rule is compiled into the registry automatically
7. Run `cargo test` — the generated test cases run automatically
8. Verify with `python -c "from burnt._engine import list_rules; print([r.code for r in list_rules()])"`

---

## Tag Vocabulary

Use these tags consistently so `--ignore tag` and `--select tag` work as
expected. Add new tags sparingly.

| Tag | Purpose |
|-----|---------|
| `pyspark` | Any PySpark-specific rule |
| `sql` | Any SQL-language rule |
| `python` | Python style/structure (not PySpark-specific) |
| `performance` | Has a direct performance or cost impact |
| `memory` | Risk of driver or executor OOM |
| `driver-bound` | Materialises data on the driver |
| `shuffle` | Triggers or worsens a shuffle |
| `correctness` | Can produce wrong results silently |
| `style` | Style / readability advisory |
| `notebook` | Notebook structure or metadata |
| `sdp` | Spark Declarative Pipelines / DLT |
| `delta` | Delta Lake specific |
| `caching` | DataFrame cache/persist lifecycle |
| `udf` | Python or Pandas UDF |
