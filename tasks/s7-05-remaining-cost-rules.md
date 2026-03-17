# Task: burnt check — Remaining Cost Rules in Rust (BP020–BP031, BQ004–BQ005)

---

## Metadata

```yaml
id: s7-05-remaining-cost-rules
status: todo
phase: 7
priority: high
agent: ~
blocked_by: [s7-04-rust-semantic-core]
created_by: planner
```

---

## Context

### Goal

Implement all cost-semantic rules not shipped in s2-05b into the Rust `PySparkRuleVisitor` and SQL checker. All detection logic lives in `src/burnt_rs/src/rules/pyspark.rs` and `src/burnt_rs/src/rules/sql.rs`. No Python analysis code.

### Files to read

```
# Required
src/burnt_rs/src/rules/pyspark.rs   # add new check_ methods here
src/burnt_rs/src/rules/sql.rs       # add BQ004/BQ005 here
src/burnt_rs/src/semantic.rs        # SparkSemanticModel — for BP020/BP021/BP024/BP031

# Reference
tasks/s2-05b-check-research-compendium.md.completed   # what was already implemented
tasks/s7-04-rust-semantic-core.md
```

### Background

**PySpark rules to implement (all in `PySparkRuleVisitor`)**

| Code | Rule ID | Detection | Needs semantic model? |
|------|---------|-----------|----------------------|
| BP020 | `cache_without_unpersist` | `.cache()` binding, no `.unpersist()` before scope exit | Yes — s7-02 already implements this |
| BP021 | `repartition_before_write` | `.repartition(n)` immediately before `.write` in call chain | No — chain walk |
| BP023 | `count_for_emptiness_check` | `df.count() == 0` or `df.count() > 0` in boolean/comparison context | No — AST pattern |
| BP024 | `single_use_cache` | `.cache()` on a variable used only once before scope exit | Yes — s7-02 already implements this |
| BP028 | `non_equi_join` | `.join(other, condition)` where condition contains `>`, `<`, `!=` | No — inspect join arg |
| BP029 | `udf_in_filter` | `.filter(udf_call(...))` or `.where(udf_call(...))` | No — check if filter arg is a UDF call |
| BP031 | `repeated_actions_no_cache` | Same DataFrame ≥2 action calls without intervening `.cache()` | Yes — s7-02 already implements this |

Note: BP020, BP024, BP031 are already implemented in `SparkSemanticModel.emit_scope_findings()` from s7-02. This task wires them into the rule table and adds tests. The remaining four (BP021, BP023, BP028, BP029) need new `check_` methods.

**BP021 — `repartition_before_write`**

```rust
fn check_repartition_before_write(&mut self, node: &ExprCall, method: &str) {
    if method != "write" && method != "save" && method != "saveAsTable" { return; }
    let chain = preceding_methods(node);
    // repartition(n) directly before write — coalesce(n) is preferred
    if chain.first().map(|m| m == "repartition").unwrap_or(false) {
        self.findings.push(Finding::warning(
            "repartition_before_write",
            self.current_line,
            ".repartition() before .write triggers a full shuffle — prefer .coalesce() for output file count control",
            "Use .coalesce(n) before .write to avoid the extra shuffle stage",
        ));
    }
}
```

**BP023 — `count_for_emptiness_check`**

Detect `df.count() == 0`, `df.count() > 0`, `df.count() != 0` in any comparison context:

```rust
fn visit_expr_compare(&mut self, node: &ExprCompare) {
    // Check if left or any comparator is a .count() call
    if is_count_call(&node.left) || node.comparators.iter().any(|c| is_count_call(c)) {
        self.findings.push(Finding::warning(
            "count_for_emptiness_check",
            line_of(node) + self.cell.line_offset as u32,
            "df.count() for emptiness check triggers a full dataset scan",
            "Use df.isEmpty (Spark 3.3+) or df.limit(1).count() == 0",
        ));
    }
    self.walk_expr_compare(node);
}

fn is_count_call(expr: &Expr) -> bool {
    matches!(expr, Expr::Call(c) if extract_method_name(c).as_deref() == Some("count"))
}
```

**BP028 — `non_equi_join`**

```rust
fn check_non_equi_join(&mut self, node: &ExprCall, method: &str) {
    if method != "join" || node.arguments.args.len() < 2 { return; }
    let cond = &node.arguments.args[1];
    if contains_comparison_op(cond, &[CmpOp::Gt, CmpOp::Lt, CmpOp::NotEq, CmpOp::GtE, CmpOp::LtE]) {
        self.findings.push(Finding::warning(
            "non_equi_join",
            self.current_line,
            "Non-equi join condition (>, <, !=) prevents hash join — forces sort-merge or nested loop",
            "Consider restructuring the join condition or using a broadcast hint",
        ));
    }
}
```

**BP029 — `udf_in_filter`**

```rust
fn check_udf_in_filter(&mut self, node: &ExprCall, method: &str) {
    if !matches!(method, "filter" | "where") { return; }
    if let Some(first_arg) = node.arguments.args.first() {
        if is_udf_call(first_arg) {
            self.findings.push(Finding::error(
                "udf_in_filter",
                self.current_line,
                ".filter(udf(...)) forces all rows through Python — defeats predicate pushdown",
                "Rewrite the filter condition using native Spark functions (F.col, F.when, etc.)",
            ));
        }
    }
}

fn is_udf_call(expr: &Expr) -> bool {
    // A UDF call is a Name call where the callee is known to be decorated with @udf/@pandas_udf,
    // OR any call that isn't a method call on a Column (F.col, F.lit, etc.)
    // Heuristic: if the callee is a bare Name (not an attribute), treat as potential UDF
    matches!(expr, Expr::Call(c) if matches!(&*c.func, Expr::Name(_)))
}
```

**SQL rules to implement (in `rules/sql.rs`)**

| Code | Rule ID | Detection | sqlparser-rs node |
|------|---------|-----------|------------------|
| BQ004 | `correlated_subquery` | Subquery referencing an outer column by name | `Subquery` containing `Identifier` that matches outer `SelectItem` alias |
| BQ005 | `implicit_type_cast_join` | JOIN ON columns of mismatched types | Requires schema metadata — flag as LOW confidence without schema; skip if no `--schema` flag |

**BQ004 — Correlated subquery**

Correlated subqueries are the worst-case join pattern — they force a nested loop execution. Detection: a `Subquery` expression that contains a column reference matching a column defined in the outer `FROM` clause.

```rust
fn check_correlated_subquery(stmt: &Statement, frag: &SqlFragment, out: &mut Vec<Finding>) {
    // Walk all Subquery nodes; for each, check if any Identifier inside it
    // could only be resolved from the outer query's table list.
    // Heuristic: if the subquery WHERE clause contains a column.table reference
    // where `table` matches an alias in the outer FROM, flag it.
    // Full implementation requires scope-aware identifier resolution — use
    // sqlparser-rs's visitor to walk and maintain an alias set.
}
```

**BQ005 — Implicit type cast join**

Without schema metadata this cannot be definitively detected. Flag at LOW confidence only when `--schema` mode is active (future sprint). For now, register the rule in the registry with `confidence: Low` and `fixable: false`, but emit no findings until schema-aware mode (s7-09) is available.

**Registry additions**

Add to `src/burnt_rs/src/registry.rs`:

```rust
Rule { id: "repartition_before_write",  code: "BP021", severity: Warning, confidence: Medium, .. },
Rule { id: "count_for_emptiness_check", code: "BP023", severity: Warning, confidence: High,   .. },
Rule { id: "non_equi_join",             code: "BP028", severity: Warning, confidence: Medium, .. },
Rule { id: "udf_in_filter",             code: "BP029", severity: Error,   confidence: High,   .. },
Rule { id: "correlated_subquery",       code: "BQ004", severity: Error,   confidence: Medium, .. },
Rule { id: "implicit_type_cast_join",   code: "BQ005", severity: Warning, confidence: Low,    .. },
```

---

## Acceptance Criteria

- [ ] `df.repartition(10).write.parquet("/out")` → `repartition_before_write` WARNING
- [ ] `df.repartition(10).select("x").write.parquet("/out")` → NO finding (not immediately before write)
- [ ] `if df.count() == 0:` → `count_for_emptiness_check` WARNING
- [ ] `df.count() > 0` in boolean context → WARNING
- [ ] `.join(other, left.id > right.id)` → `non_equi_join` WARNING
- [ ] `.filter(my_udf(F.col("x")))` → `udf_in_filter` ERROR
- [ ] `SELECT * FROM t WHERE id IN (SELECT id FROM s WHERE s.dept = t.dept)` → `correlated_subquery` ERROR
- [ ] All 451+ existing unit tests pass
- [ ] `cargo test rules::pyspark` and `cargo test rules::sql` pass
- [ ] `uv run ruff check src/ tests/` clean

---

## Verification

```bash
cd src/burnt_rs && cargo test
maturin develop --release

python3 -c "
import burnt_rs
cases = [
    ('df.repartition(10).write.parquet(\"/out\")', {'repartition_before_write'}),
    ('x = df.count() == 0', {'count_for_emptiness_check'}),
    ('df.filter(my_udf(F.col(\"x\")))', {'udf_in_filter'}),
]
for src, expected in cases:
    import tempfile, pathlib
    p = pathlib.Path(tempfile.mktemp(suffix='.py'))
    p.write_text(src)
    found = {f['rule_id'] for f in burnt_rs.analyze_file(str(p), ['ALL'], [])}
    missing = expected - found
    if missing: print(f'MISSING {missing} in: {src}')
    else: print(f'OK: {src}')
"

uv run pytest -m unit -v
```

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Requires s7-04 (Rust `PySparkRuleVisitor` and `HybridChecker` in place).
