# Task s7-04: Complete Rule Engine (76 rules)

## Metadata

```yaml
id: s7-04-rules
status: todo
phase: 7
priority: high
agent: ~
blocked_by: [s7-03-analysis-pipeline]
created_by: planner
```

## Goal

Port every rule from the Python implementation to Rust. Rules organized into 4 phases matching ruff's checker architecture. Every `check_` method receives pre-computed `chain` and `ChainContext`. SQL rules use `ChainContext` to suppress false positives. New diagnostic rules from `%run` resolution (BN001, BN003) already fire from s7-01 — this task handles the remaining 76.

## Background

### Rule dispatch — chain computed ONCE per `visit_expr_call`

```rust
fn visit_expr_call(&mut self, node: &ExprCall) {
    let method = extract_method_name(node);
    let chain = collect_preceding_methods(node);  // computed ONCE
    let chain_ctx = ChainContext::from_chain(&chain);

    self.try_extract_sql(node, &method, ...);
    self.update_semantic(node, &method, ...);
    self.dispatch_rules(node, &method, &chain, &chain_ctx, ...);

    self.walk_expr_call(node);
}
```

### Phase 0: Source-text rules

BNT-M01 `backslash_chain_continuation` — raw text scan before AST parse.

### Phase 1: PySpark rules (in `dispatch_rules`)

All BP performance rules (BP001-BP031) and BNT style rules (BNT-I01 through BNT-D04).

### Phase 2: SQL rules (with `ChainContext`)

`select_star` suppressed when `ChainContext.has_select == true` (Python `.select()` follows `spark.sql()`). `filter_after_spark_sql` includes actual suggestion referencing SQL WHERE clause existence.

### Phase 3: Semantic model rules

BP020, BP024, BP031 fire from `SemanticModel.finalize()` and `SemanticModel.bind()` overwrite. These work across `%run` boundaries because inlined cells update the model before parent cells.

### BP029 `udf_in_filter` — reduced false positives

Skip `SPARK_BUILTINS` (col, lit, when, etc.). Only flag bare name calls that aren't known builtins.

### BQ004 `correlated_subquery` — working heuristic

Collect outer table aliases, walk subqueries, check if inner column references match outer aliases. Not a TODO.

## Acceptance Criteria

- [ ] All 76 rules from `registry.py` implemented in Rust
- [ ] Chain computed once per `visit_expr_call`, passed to all `check_*` methods
- [ ] `select_star` suppressed when `ChainContext.has_select == true`
- [ ] BP029 skips `SPARK_BUILTINS`
- [ ] BQ004 has working heuristic (not TODO)
- [ ] `%run`-inlined cells: rules fire correctly on inlined code
- [ ] `cargo test rules::pyspark && cargo test rules::sql` pass
