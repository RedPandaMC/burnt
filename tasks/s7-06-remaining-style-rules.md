# Task: burnt check — Remaining BNT Style Rules in Rust

---

## Metadata

```yaml
id: s7-06-remaining-style-rules
status: todo
phase: 7
priority: medium
agent: ~
blocked_by: [s7-04-rust-semantic-core]
created_by: planner
```

---

## Context

### Goal

Implement all BNT style rules not shipped in s2-05b into the Rust `PySparkRuleVisitor`. All detection in `src/burnt_rs/src/rules/pyspark.rs`. 39 BNT rules total were identified in the research compendium; 20 were implemented in s2-05b. This task implements the remaining 19.

### Files to read

```
# Required
src/burnt_rs/src/rules/pyspark.rs
src/burnt/parsers/registry.py        # existing BNT codes — verify no duplicates

# Reference
tasks/s2-05b-check-research-compendium.md.completed  # what's already implemented
```

### Background

**Rules implemented in s2-05b (skip these)**

BNT-I01, BNT-I02, BNT-I03, BNT-C02, BNT-C03, BNT-C04, BNT-N02, BNT-N04, BNT-M03, BNT-S01, BNT-S02, BNT-SP1, BNT-J01, BNT-Q01, BNT-Q02, BNT-W01, BNT-W02, BNT-W03, BNT-L01, BNT-D02, BNT-D05, BNT-P01.

**Rules to implement in this task**

| Code | Rule ID | Severity | Detection |
|------|---------|----------|-----------|
| BNT-C01 | `df_bracket_or_dot_reference` | WARNING | `df["col"]` or `df.col` outside a join condition |
| BNT-N01 | `generic_dataframe_variable_name` | STYLE | Assignment target named `df`, `df1`–`df9`, `dataframe` |
| BNT-N03 | `non_snake_case_alias` | STYLE | `.alias("camelCase")` or alias with spaces/special chars |
| BNT-M01 | `backslash_chain_continuation` | WARNING | `\` line continuation before `.method(` |
| BNT-M02 | `excessive_chain_length` | WARNING | > 5 consecutive method calls in a single chain |
| BNT-S03 | `cache_with_delta_table` | WARNING | `.cache()` or `.persist()` on a `spark.read.table()` / `spark.table()` result |
| BNT-SP2 | `conf_set_in_transform` | WARNING | `.conf.set(...)` inside a non-entrypoint function |
| BNT-T01 | `mixed_io_and_transform` | WARNING | Function containing both `spark.read`/`.load()` and `.write`/`.save()` |
| BNT-T02 | `collect_comparison_in_test` | WARNING | `.collect()` used in assert/comparison in `test_*.py` files |
| BNT-T03 | `sparksession_per_test_function` | WARNING | `SparkSession.builder` inside a `test_*` function body (not a fixture) |
| BNT-L02 | `right_join_prefer_left` | STYLE | `how="right"` in `.join()` call |
| BNT-D01 | `missing_3level_namespace` | WARNING | Table reference missing catalog prefix — `spark.table("schema.table")` |
| BNT-D03 | `path_based_read_prefer_table` | INFO | `.read.parquet("/path/")` or `.read.csv("/path/")` — prefer `spark.table()` |
| BNT-D04 | `python_udf_native_exists` | WARNING | `@udf` decorator where a native Spark function exists (BUILTIN_REPLACEMENTS map) |
| BNT-A01 | `missing_dataframe_type_annotation` | WARNING | `def transform(df)` without `: DataFrame` annotation on a parameter named `df`/`sdf`/`spark_df` |
| BNT-A02 | `missing_transform_docstring` | INFO | Function decorated with `@dlt.table`/`@dp.table` with no docstring |

**BNT-C01 — `df_bracket_or_dot_reference`**

```rust
fn check_df_bracket_dot_ref(&mut self, node: &ExprSubscript) {
    // df["col"] — subscript on a Name that looks like a DataFrame variable
    // Allowed exception: inside a join condition (visit_expr_call for .join() sets a flag)
    if self.in_join_condition { return; }
    if is_df_variable(&node.value) && is_string_subscript(&node.slice) {
        self.findings.push(Finding::warning(
            "df_bracket_or_dot_reference",
            line_of(node) + self.cell.line_offset as u32,
            "df[\"col\"] creates a stale column reference bound to the DataFrame object",
            "Use F.col(\"col\") instead — resolves at evaluation time and works in chains",
        ));
    }
}
```

**BNT-N01 — `generic_dataframe_variable_name`**

```rust
fn check_generic_df_name(&mut self, name: &str, line: u32) {
    // Called from visit_stmt_assign when binding a DataFrame
    const GENERIC: &[&str] = &["df", "df1", "df2", "df3", "df4", "df5", "df6", "df7", "df8", "df9", "dataframe", "data_frame"];
    if GENERIC.contains(&name) {
        self.findings.push(Finding::style(
            "generic_dataframe_variable_name",
            line,
            &format!("'{name}' is a generic DataFrame variable name"),
            "Use a descriptive name: orders, active_users, daily_revenue, etc.",
        ));
    }
}
```

**BNT-M01 — `backslash_chain_continuation`**

This is a token-level check, not AST — the Python AST strips backslash continuations. Detect by scanning the raw source text for lines ending in `\` that are followed by a line starting with `.`:

```rust
fn check_backslash_continuation(source: &str, line_offset: u32) -> Vec<Finding> {
    let lines: Vec<&str> = source.lines().collect();
    let mut findings = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim_end();
        if trimmed.ends_with('\\') {
            let next = lines.get(i + 1).map(|l| l.trim()).unwrap_or("");
            if next.starts_with('.') {
                findings.push(Finding::warning(
                    "backslash_chain_continuation",
                    line_offset + i as u32 + 1,
                    "Backslash line continuation before method chain — trailing whitespace silently breaks code",
                    "Wrap the chain in parentheses instead: (df\\n    .method())",
                ));
            }
        }
    }
    findings
}
```

This runs in the `HybridChecker` on each Python cell's raw source before AST analysis.

**BNT-M02 — `excessive_chain_length`**

Track consecutive method calls on the same receiver in `visit_expr_call`. A chain of N is detected by counting how many levels deep the receiver nesting goes before hitting a non-Call node:

```rust
fn chain_depth(node: &ExprCall) -> usize {
    let mut depth = 0;
    let mut current = receiver(node);
    while let Some(c) = as_method_call(current) {
        depth += 1;
        current = receiver(c);
    }
    depth
}

fn check_excessive_chain_length(&mut self, node: &ExprCall, method: &str) {
    if chain_depth(node) > 5 {
        self.findings.push(Finding::warning(
            "excessive_chain_length",
            self.current_line,
            "Chain of >5 method calls — consider intermediate variables at logical boundaries",
            "Break at natural points (after joins, before writes) for readability",
        ));
    }
}
```

**BNT-S03 — `cache_with_delta_table`**

```rust
fn check_cache_with_delta(&mut self, node: &ExprCall, method: &str) {
    if !matches!(method, "cache" | "persist") { return; }
    let chain = preceding_methods(node);
    // If the chain contains spark.table() or spark.read.table() or .delta()
    if chain.contains("table") || chain.iter().any(|m| m == "delta") {
        self.findings.push(Finding::warning(
            "cache_with_delta_table",
            self.current_line,
            ".cache()/.persist() on a Delta table read adds memory overhead without benefit — Delta already caches at the file level",
            "Remove .cache() and rely on Delta's native caching; use .persist() only if you apply expensive transforms",
        ));
    }
}
```

**BNT-T02 / BNT-T03 — test file rules**

These only fire when the source file path contains `test_` or is in a `tests/` directory. The `Cell.path` is available to all rules via `self.cell.path`:

```rust
fn is_test_file(&self) -> bool {
    let p = self.cell.path.to_string_lossy();
    p.contains("test_") || p.contains("/tests/")
}
```

**BNT-D01 — `missing_3level_namespace`**

```rust
fn check_three_level_namespace(&mut self, node: &ExprCall, method: &str) {
    if method != "table" { return; }
    if let Some(first_arg) = node.arguments.args.first() {
        if let Expr::StringLiteral(s) = first_arg {
            let table_ref = s.value.to_str();
            let parts: Vec<&str> = table_ref.split('.').collect();
            if parts.len() < 3 {
                self.findings.push(Finding::warning(
                    "missing_3level_namespace",
                    self.current_line,
                    &format!("Table reference '{table_ref}' missing catalog prefix"),
                    "Use full three-level namespace: catalog.schema.table",
                ));
            }
        }
    }
}
```

**Registry additions (Rust `registry.rs`)**

Add all 16 new rules with appropriate severity, confidence, and language fields.

---

## Acceptance Criteria

- [ ] `df["col"]` outside join → `df_bracket_or_dot_reference` WARNING; inside `.join()` condition → NO finding
- [ ] `df = spark.sql("...")` → `generic_dataframe_variable_name` STYLE
- [ ] `.alias("camelCase")` → `non_snake_case_alias` STYLE
- [ ] Backslash continuation before `.method(` → `backslash_chain_continuation` WARNING
- [ ] Chain of 6+ methods → `excessive_chain_length` WARNING; 5 methods → NO finding
- [ ] `.cache()` on `spark.table("t")` result → `cache_with_delta_table` WARNING
- [ ] `spark.conf.set(...)` inside transform function → `conf_set_in_transform` WARNING
- [ ] `.collect()` in `assert` in `test_my_jobs.py` → `collect_comparison_in_test` WARNING
- [ ] `SparkSession.builder.getOrCreate()` inside `def test_my_function():` → `sparksession_per_test_function` WARNING
- [ ] `.join(other, ..., how="right")` → `right_join_prefer_left` STYLE
- [ ] `spark.table("schema.table")` (no catalog) → `missing_3level_namespace` WARNING
- [ ] `spark.read.parquet("/mnt/data")` → `path_based_read_prefer_table` INFO
- [ ] All 451+ existing unit tests pass
- [ ] `cargo test rules::pyspark` passes
- [ ] `uv run ruff check src/ tests/` clean

---

## Verification

```bash
cd src/burnt_rs && cargo test
maturin develop --release
uv run pytest -m unit -v

# Spot checks
python3 -c "
import burnt_rs, tempfile, pathlib

cases = {
    'df[\"col\"]': 'df_bracket_or_dot_reference',
    'df = spark.sql(\"x\")': 'generic_dataframe_variable_name',
    'spark.table(\"schema.t\")': 'missing_3level_namespace',
    'spark.read.parquet(\"/mnt/x\")': 'path_based_read_prefer_table',
}
for src, rule in cases.items():
    p = pathlib.Path(tempfile.mktemp(suffix='.py'))
    p.write_text(src)
    found = {f['rule_id'] for f in burnt_rs.analyze_file(str(p), ['ALL'], [])}
    status = 'OK' if rule in found else f'MISSING {rule}'
    print(f'{status}: {src[:50]}')
"
```

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Requires s7-04 (Rust `PySparkRuleVisitor` scaffolding in place).
