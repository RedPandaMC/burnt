# Task: burnt check — PySpark Semantic Model in Rust

---

## Metadata

```yaml
id: s7-02-semantic-model
status: todo
phase: 7
priority: high
agent: ~
blocked_by: [s7-01-notebook-ingestion]
created_by: planner
```

---

## Context

### Goal

Implement `SparkSemanticModel` entirely in Rust — a binding tracker that follows DataFrame variables across statements, function scopes, and notebook cells. Python never touches AST nodes or binding state. This enables the class of rules that require inter-statement data flow: `filter_after_spark_sql` (two-statement form), `cache_without_unpersist`, `single_use_cache`, and `repeated_actions_no_cache`.

### Files to read

```
# Required
src/burnt_rs/src/lib.rs
src/burnt_rs/src/notebook.rs     # Cell struct from s7-01
src/burnt_rs/Cargo.toml

# Reference
tasks/s7-01-notebook-ingestion.md
tasks/s3-10-hybrid-ast.md        # SQL provenance concept (superseded by this task in Rust)
```

### Background

**Parser choice: `ruff_python_parser`**

Ruff's Python parser (`ruff_python_parser`) is the right choice over `rustpython-parser`:
- Actively maintained by Astral; handles all modern Python syntax (walrus, match, PEP 695 generics)
- Produces a typed, traversable `ast::Mod` with full visitor support via `ruff_python_ast::visitor`
- Battle-tested across the entire PyPI ecosystem
- MIT licensed

```toml
# Cargo.toml
[dependencies]
ruff_python_parser = { git = "https://github.com/astral-sh/ruff", tag = "0.9.0" }
ruff_python_ast    = { git = "https://github.com/astral-sh/ruff", tag = "0.9.0" }
```

Alternatively, use the published crate if available on crates.io at time of implementation; check `ruff_python_parser` on crates.io first.

**Parser choice: `sqlparser-rs` for SQL**

```toml
sqlparser = "0.54"   # check crates.io for latest; used by Apache Arrow DataFusion
```

`sqlparser-rs` has a `Dialect` trait. Use `GenericDialect` or implement a `DatabricksDialect` extending `AnsiDialect` for Databricks-specific syntax (USING, QUALIFY, ILIKE, etc.).

**`DFProvenance` — what produced this DataFrame**

```rust
// src/burnt_rs/src/semantic.rs

#[derive(Debug, Clone, PartialEq)]
pub enum DFProvenance {
    SparkSql {
        sql: String,                    // raw SQL string (for SQL rule analysis)
        has_where: bool,                // pre-computed: does the SQL have a WHERE clause?
    },
    SparkTable { table: String },
    SparkRead  { format: String },      // "csv", "json", "parquet", "jdbc", etc.
    Transform  { source: String, method: String },   // source var + method name
    Join       { left: String, right: String },
    Union      { sources: Vec<String> },
    Unknown,
}

#[derive(Debug, Clone)]
pub struct DFBinding {
    pub name: String,
    pub provenance: DFProvenance,
    pub line: u32,
    pub action_count: u32,             // .collect(), .count(), .show(), .write called N times
    pub cache_line: Option<u32>,       // line of .cache()/.persist()
    pub unpersist_line: Option<u32>,   // line of .unpersist()
    pub use_count: u32,                // total references (for single_use_cache)
}
```

**`SparkSemanticModel` — scope-aware binding table**

```rust
#[derive(Debug, Default)]
pub struct SparkSemanticModel {
    scope_stack: Vec<HashMap<String, DFBinding>>,
    pub findings: Vec<Finding>,
}

impl SparkSemanticModel {
    pub fn new() -> Self {
        Self { scope_stack: vec![HashMap::new()], findings: vec![] }
    }

    pub fn bind(&mut self, name: &str, binding: DFBinding) {
        self.current_scope_mut().insert(name.to_string(), binding);
    }

    pub fn lookup(&self, name: &str) -> Option<&DFBinding> {
        // Walk scope stack from innermost to outermost
        for scope in self.scope_stack.iter().rev() {
            if let Some(b) = scope.get(name) { return Some(b); }
        }
        None
    }

    pub fn lookup_mut(&mut self, name: &str) -> Option<&mut DFBinding> {
        for scope in self.scope_stack.iter_mut().rev() {
            if let Some(b) = scope.get_mut(name) { return Some(b); }
        }
        None
    }

    pub fn record_action(&mut self, name: &str, method: &str, line: u32) {
        if let Some(b) = self.lookup_mut(name) {
            b.action_count += 1;
        }
    }

    pub fn record_cache(&mut self, name: &str, line: u32) {
        if let Some(b) = self.lookup_mut(name) {
            b.cache_line = Some(line);
        }
    }

    pub fn record_unpersist(&mut self, name: &str, line: u32) {
        if let Some(b) = self.lookup_mut(name) {
            b.unpersist_line = Some(line);
        }
    }

    pub fn record_use(&mut self, name: &str) {
        if let Some(b) = self.lookup_mut(name) {
            b.use_count += 1;
        }
    }

    pub fn push_scope(&mut self) {
        self.scope_stack.push(HashMap::new());
    }

    /// Pop scope and emit findings for bindings that go out of scope.
    pub fn pop_scope(&mut self) {
        let scope = self.scope_stack.pop().unwrap_or_default();
        self.emit_scope_findings(scope);
    }

    /// Call at end of file/notebook — emit findings for module-level bindings.
    pub fn finalize(&mut self) {
        let scope = self.scope_stack.drain(..).next().unwrap_or_default();
        self.emit_scope_findings(scope);
    }

    fn emit_scope_findings(&mut self, scope: HashMap<String, DFBinding>) {
        for (name, binding) in scope {
            // cache_without_unpersist
            if binding.cache_line.is_some() && binding.unpersist_line.is_none() {
                self.findings.push(Finding {
                    rule_id: "cache_without_unpersist".to_string(),
                    severity: Severity::Warning,
                    line: binding.cache_line.unwrap(),
                    description: format!("'{name}' is cached but never unpersisted — potential memory leak"),
                    suggestion: "Call .unpersist() when the cached DataFrame is no longer needed".to_string(),
                });
            }
            // single_use_cache
            if binding.cache_line.is_some() && binding.action_count <= 1 && binding.use_count <= 1 {
                self.findings.push(Finding {
                    rule_id: "single_use_cache".to_string(),
                    severity: Severity::Info,
                    line: binding.cache_line.unwrap(),
                    description: format!("'{name}' is cached but used only once — cache adds overhead without benefit"),
                    suggestion: "Remove .cache() if the DataFrame is only consumed once".to_string(),
                });
            }
            // repeated_actions_no_cache
            if binding.action_count >= 2 && binding.cache_line.is_none() {
                self.findings.push(Finding {
                    rule_id: "repeated_actions_no_cache".to_string(),
                    severity: Severity::Warning,
                    line: binding.line,
                    description: format!("'{name}' triggers {n} actions without .cache() — plan re-evaluated each time", n = binding.action_count),
                    suggestion: "Add .cache() before the first action if the DataFrame is reused".to_string(),
                });
            }
        }
    }

    fn current_scope_mut(&mut self) -> &mut HashMap<String, DFBinding> {
        self.scope_stack.last_mut().expect("scope stack never empty")
    }
}
```

**Walking the Python AST with `ruff_python_ast`**

`ruff_python_ast` provides a `Visitor` trait for zero-cost traversal. Implement it for `SemanticModelBuilder`:

```rust
use ruff_python_ast::{visitor::Visitor, *};

pub struct SemanticModelBuilder<'a> {
    pub model: &'a mut SparkSemanticModel,
    current_line: u32,
}

impl<'a> Visitor<'_> for SemanticModelBuilder<'a> {
    fn visit_stmt_assign(&mut self, node: &StmtAssign) {
        self.current_line = node.range.start().to_u32() / 1000; // convert offset to approx line
        if let Some(name) = extract_single_name_target(&node.targets) {
            if let Some(provenance) = classify_rhs(&node.value) {
                self.model.bind(&name, DFBinding {
                    name: name.clone(),
                    provenance,
                    line: self.current_line,
                    ..Default::default()
                });
            }
        }
        self.walk_stmt_assign(node);
    }

    fn visit_expr_call(&mut self, node: &ExprCall) {
        if let Some((obj_name, method)) = extract_method_call(node) {
            match method.as_str() {
                "filter" | "where" => {
                    if let Some(binding) = self.model.lookup(&obj_name) {
                        if matches!(binding.provenance, DFProvenance::SparkSql { .. }) {
                            let has_where = match &binding.provenance {
                                DFProvenance::SparkSql { has_where, .. } => *has_where,
                                _ => false,
                            };
                            let suggestion = if has_where {
                                "Add an AND condition to the existing SQL WHERE clause"
                            } else {
                                "Move the filter into the SQL WHERE clause or rewrite using the DataFrame API"
                            };
                            self.model.findings.push(Finding {
                                rule_id: "filter_after_spark_sql".to_string(),
                                severity: Severity::Warning,
                                line: self.current_line,
                                description: format!("'{obj_name}' came from spark.sql() — filter logic split across SQL and Python"),
                                suggestion: suggestion.to_string(),
                            });
                        }
                    }
                    self.model.record_use(&obj_name);
                }
                "cache" | "persist"   => self.model.record_cache(&obj_name, self.current_line),
                "unpersist"           => self.model.record_unpersist(&obj_name, self.current_line),
                "collect" | "count" | "show" | "take" | "first" | "head" => {
                    self.model.record_action(&obj_name, &method, self.current_line);
                }
                _ => { self.model.record_use(&obj_name); }
            }
        }
        self.walk_expr_call(node);
    }

    fn visit_stmt_function_def(&mut self, node: &StmtFunctionDef) {
        self.model.push_scope();
        self.walk_stmt_function_def(node);
        self.model.pop_scope();
    }
}

fn classify_rhs(expr: &Expr) -> Option<DFProvenance> {
    // Walk the call chain to identify DataFrame-producing expressions
    // spark.sql("...") → SparkSql { sql, has_where }
    // spark.table("t") → SparkTable
    // spark.read.csv() → SparkRead { format: "csv" }
    // df.filter(...) → Transform { source: "df", method: "filter" }
    // etc.
    todo!()
}
```

**Cross-cell binding propagation**

For notebooks, a single `SparkSemanticModel` is constructed once and passed to the `SemanticModelBuilder` for each Python cell in order. Module-level bindings persist across cells; function-scoped bindings are popped at function exit as usual. `finalize()` is called after the last cell.

---

## Acceptance Criteria

- [ ] `df = spark.sql("SELECT * FROM t"); df.filter(...)` → `filter_after_spark_sql` WARNING from Rust
- [ ] `df = spark.sql("SELECT * FROM t WHERE x > 5"); df.filter(...)` → suggestion mentions existing WHERE
- [ ] `df = spark.table("t"); df.filter(...)` → NO `filter_after_spark_sql` (not SparkSql provenance)
- [ ] `df.cache()` with no `.unpersist()` before scope exit → `cache_without_unpersist` WARNING
- [ ] `df.cache(); result = df.collect()` (one action) → `single_use_cache` INFO
- [ ] `df.count(); df.collect()` (two actions, no cache) → `repeated_actions_no_cache` WARNING
- [ ] Cross-cell: binding from cell 1 resolved in cell 5
- [ ] `cargo test` passes for all semantic model tests
- [ ] No `unwrap()` panics on malformed/edge-case Python input

---

## Verification

```bash
cd src/burnt_rs && cargo test semantic

# Python-level smoke via PyO3
python3 -c "
import burnt_rs
findings = burnt_rs.analyze_cells([
    {'language': 'python', 'source': 'df = spark.sql(\"SELECT * FROM t\")', 'line_offset': 1},
    {'language': 'python', 'source': 'result = df.filter(df.x > 5)', 'line_offset': 2},
])
names = {f['rule_id'] for f in findings}
assert 'filter_after_spark_sql' in names, names
assert 'select_star' in names, names
print('Semantic model OK:', names)
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

Requires s7-01 for `Cell` struct definition. `ruff_python_parser` and `sqlparser` must be added to `Cargo.toml`.
