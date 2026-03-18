# Task s7-02: Dual-Parser Integration & Semantic Model

## Metadata

```yaml
id: s7-02-dual-parser-semantic
status: todo
phase: 7
priority: critical
agent: ~
blocked_by: [s7-01-foundation-ingestion]
created_by: planner
```

## Goal

Integrate `ruff_python_parser` and `sqlparser-rs` behind a stable adapter, build the scope-aware `SemanticModel` that tracks DataFrame bindings across cells (including those inlined from `%run` targets), implement `ChainContext`, and provide a concrete `classify_rhs` for the top 12 DataFrame creation patterns. After this task, the semantic model can trace `df` in cell 5 back to `spark.sql("SELECT * FROM orders")` defined in a `%run`-inlined cell from a different file.

## Background

### Parser dependencies

```toml
ruff_python_parser = { git = "https://github.com/astral-sh/ruff.git", tag = "0.15.6" }
ruff_python_ast    = { git = "https://github.com/astral-sh/ruff.git", tag = "0.15.6" }
ruff_text_size     = { git = "https://github.com/astral-sh/ruff.git", tag = "0.15.6" }
ruff_source_file   = { git = "https://github.com/astral-sh/ruff.git", tag = "0.15.6" }
sqlparser          = { version = "0.60", features = ["visitor"] }
```

**Use built-in `sqlparser::dialect::DatabricksDialect`.** Do NOT write a custom dialect.

### Adapter module — ruff firewall

```rust
// src/burnt_rs/src/python/parser.rs — THE ONLY FILE that calls ruff_python_parser

pub struct ParseResult {
    pub module: Parsed<ModModule>,
    pub errors: Vec<ParseError>,
    pub has_dlt_import: bool,
}

pub fn parse_python(source: &str) -> ParseResult {
    let parsed = ruff_python_parser::parse_module(source);
    let errors = parsed.errors().to_vec();
    let has_dlt_import = check_dlt_imports(parsed.syntax());
    ParseResult { module: parsed, errors, has_dlt_import }
}

fn check_dlt_imports(module: &ModModule) -> bool {
    module.body.iter().take(30).any(|stmt| match stmt {
        Stmt::Import(s) => s.names.iter().any(|a| a.name.as_str() == "dlt"),
        Stmt::ImportFrom(s) => s.module.as_ref().map(|m| m.as_str()) == Some("dlt"),
        _ => false,
    })
}
```

DLT detection via AST, not string matching. `# import dlt` (comment) and `from x import dlt_utils` (substring) correctly rejected.

### Semantic Model — binding overwrite emits findings

```rust
pub struct SemanticModel {
    scope_stack: Vec<HashMap<String, Binding>>,
    pub findings: Vec<Finding>,
}

pub struct Binding {
    pub name: String,
    pub origin: BindingOrigin,
    pub cell_idx: u32,
    pub origin_path: PathBuf,       // which file defined this binding (may be %run target)
    pub sql_frag_idx: Option<u32>,  // index into sql_fragments vec
    pub action_history: Vec<(String, u32)>,
    pub cache_line: Option<u32>,
    pub unpersist_line: Option<u32>,
    pub use_count: u32,
    pub chain_context: ChainContext,
}

#[derive(Debug, Clone, Default)]
pub struct ChainContext {
    pub has_limit: bool,
    pub has_select: bool,
    pub has_filter: bool,
    pub filter_is_sql_string: bool,
    pub action: Option<String>,
    pub is_cached: bool,
}

pub enum BindingOrigin {
    SparkSql,
    SparkTable { table: String },
    SparkRead { format: String },
    Transform { source: String, method: String },
    Join { left: String, right: String },
    Union { sources: Vec<String> },
    Unknown,
}

impl SemanticModel {
    /// CRITICAL: emit findings for OLD binding before overwrite
    pub fn bind(&mut self, name: &str, binding: Binding) {
        if let Some(old) = self.current_scope_mut().remove(name) {
            self.emit_binding_findings(name, &old);
        }
        self.current_scope_mut().insert(name.to_string(), binding);
    }

    pub fn record_action(&mut self, name: &str, method: &str, line: u32) {
        if let Some(b) = self.lookup_mut(name) {
            b.action_history.push((method.to_string(), line));
        }
    }

    // ... push_scope, pop_scope, finalize — emit_binding_findings on scope exit
}
```

### `classify_rhs` — concrete, not `todo!()`

```rust
pub fn classify_rhs(call: &ExprCall) -> Option<(BindingOrigin, Option<String>)> {
    let chain = collect_method_chain(call);

    // spark.sql("...")
    if chain_ends_with(&chain, "spark", "sql") {
        return Some((BindingOrigin::SparkSql, extract_first_string_arg(call)));
    }
    // spark.table("catalog.schema.table")
    if chain_ends_with(&chain, "spark", "table") {
        let table = extract_first_string_arg(call).unwrap_or_default();
        return Some((BindingOrigin::SparkTable { table }, None));
    }
    // spark.read.csv/json/parquet/format().load()
    if chain_contains_spark_read(&chain) {
        return Some((BindingOrigin::SparkRead { format: detect_read_format(&chain) }, None));
    }
    // df.join(other, ...)
    if chain_last_method(&chain) == Some("join") {
        let left = chain_receiver_name(&chain);
        let right = extract_first_name_arg(call).unwrap_or_default();
        return Some((BindingOrigin::Join { left, right }, None));
    }
    // df.union/unionAll/unionByName
    if matches!(chain_last_method(&chain), Some("union" | "unionAll" | "unionByName")) {
        return Some((BindingOrigin::Union { sources: vec![
            chain_receiver_name(&chain),
            extract_first_name_arg(call).unwrap_or_default(),
        ]}, None));
    }
    // df.filter/select/groupBy/... → Transform
    if let Some(method) = chain_last_method(&chain) {
        return Some((BindingOrigin::Transform {
            source: chain_receiver_name(&chain),
            method: method.to_string(),
        }, None));
    }
    None
}
```

### Cross-file binding via `%run`

Because `%run` cells are inlined before analysis (s7-01), the semantic model naturally sees bindings from `%run` targets. A function defined in `helpers.py` at cell index 2 is available at cell index 5 (which is from `main.py`) without any special handling. The `origin_path` field on `Binding` tracks provenance for diagnostics.

## Acceptance Criteria

- [ ] ruff adapter is sole touchpoint for `ruff_python_parser`
- [ ] `sqlparser::dialect::DatabricksDialect::default()` used — no custom dialect
- [ ] DLT detection via AST (StmtImport/StmtImportFrom), not string matching
- [ ] Parse errors → partial AST processed + `Finding` emitted (not silent skip)
- [ ] `bind()` emits findings for old binding before overwrite
- [ ] `action_history: Vec<(String, u32)>` not `action_count: u32`
- [ ] `classify_rhs` handles 6 DataFrame creation patterns concretely
- [ ] `Binding.origin_path` correctly tracks which file defined the binding
- [ ] Bindings from `%run`-inlined cells available to subsequent cells
- [ ] `cargo test semantic` passes
- [ ] Weekly CI job builds against ruff `main` for breakage early warning
