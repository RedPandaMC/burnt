# P1/18 – Refactor Analysis: `burnt-engine`

**Scope:** `src/burnt-engine/src/` (Rust engine)  
**Task:** Identify all refactoring targets and optimisation opportunities, prioritised by impact.

---

## 1. Duplicate `Finding` Struct (High Priority)

**Files:** `src/types.rs:204` and `src/rules/mod.rs:21`

Two nearly identical `Finding` structs exist. `types::Finding` carries typed `Severity` / `Confidence` enum fields; `rules::Finding` carries the same fields as raw `String`s. The pipeline converts between them on every rule execution (e.g. `mod.rs:142–149`, `162–169`, `195–204`), which is noisy and error-prone.

**Refactor:** Delete `rules::Finding`. Make `rules` import and return `types::Finding` directly. The only change needed is that tier-1 rule construction already uses `Confidence::Medium` — this is already typed. Remove the conversion blocks entirely.

---

## 2. `mode_to_lang` Logic Duplicated Four Times (High Priority)

**File:** `src/lib.rs:71–76`, `128–133`, `211–216` + `src/lib.rs:analyze_file_internal:~211`

The same `match mode { Dlt => "dlt", Sql => "sql", Python => "python" }` block appears four times. Similarly, the entire `(graph, pipeline)` dispatch block is copy-pasted across `analyze_source`, `analyze_file`, and `analyze_file_internal`.

**Refactor:**

```rust
// Add to AnalysisMode impl:
fn as_lang_str(&self) -> &'static str {
    match self {
        AnalysisMode::Dlt => "dlt",
        AnalysisMode::Sql => "sql",
        AnalysisMode::Python => "python",
    }
}
```

Extract a free function `build_graph_and_pipeline(mode, source) -> Result<(Option<PyGraph>, Option<PyPipeline>), PyErr>` and call it from all three public functions. `analyze_file_internal` can then simply delegate to `analyze_file` logic without duplicating the graph construction.

---

## 3. `RulePipeline` Re-created on Every `run()` Call (High Priority)

**File:** `src/rules/mod.rs:276`

`run()` calls `RulePipeline::new()` on every invocation. `new()` loads and compiles all rules from the generated registry on each call. For `analyze_directory` this means the pipeline is rebuilt once per file — fully serial rule initialisation inside a Rayon parallel loop.

**Refactor:** Use `std::sync::OnceLock` (stable since Rust 1.70) to create a global `RulePipeline` singleton:

```rust
static PIPELINE: OnceLock<RulePipeline> = OnceLock::new();

pub fn run(source: &str, language: &str) -> Result<Vec<TypesFinding>, String> {
    let pipeline = PIPELINE.get_or_init(RulePipeline::new);
    // ...
}
```

`RulePipeline` must be `Send + Sync` (it already is — `QueryEngine` holds only `TreeSitterLanguage` which is `Send + Sync`).

---

## 4. Debug `eprintln!` Statements Left in Production Code (High Priority)

**File:** `src/rules/mod.rs:124, 131, 140, 156`

Three `eprintln!("DEBUG ...")` calls are shipped in production. These pollute stderr on every tier-2 rule execution.

**Refactor:** Remove all three `eprintln!("DEBUG …")` calls. If runtime diagnostics are wanted, gate them behind `tracing::debug!()` or a compile-time `#[cfg(debug_assertions)]` guard.

---

## 5. Inconsistent Language Comparison in Tier-1 vs Tier-2/3 (Medium Priority)

**File:** `src/rules/mod.rs:195`

Tier-1 checks `rule.language == "All"` (capital A), while tier-2 and tier-3 normalise both sides with `.to_lowercase()` first. If any rule TOML uses `language = "all"` (lowercase), it will be silently skipped by tier-1.

**Refactor:** Normalise consistently. Add a small helper:

```rust
fn lang_matches(rule_lang: &str, query_lang: &str) -> bool {
    let rl = rule_lang.to_lowercase();
    rl == query_lang.to_lowercase() || rl == "all" || rl == "notebook"
}
```

Use it in all three `execute_tierN_rules` methods.

---

## 6. Dead Variable `known_dfs` at Top of `check_dataflow_rules` (Medium Priority)

**File:** `src/rules/dataflow.rs:174`

```rust
let known_dfs: Vec<String> = cache_ops.keys().chain(action_ops.keys()).cloned().collect();
```

This line runs **before** any entries are inserted into `cache_ops` or `action_ops`, so `known_dfs` is always an empty `Vec` and is never used. The identical expression is recomputed correctly inside the loop at line 204.

**Refactor:** Delete line 174. This also silences a Clippy `unused_variable` lint.

---

## 7. `or_insert_with(Vec::new)` → `or_default()` (Low Priority / Clippy)

**Files:** `src/rules/mod.rs`, `src/rules/dataflow.rs` (7 occurrences total)

`HashMap::entry(...).or_insert_with(Vec::new)` is identical to `.or_default()` when the value type implements `Default`. Clippy (`clippy::or_fun_call`) flags this.

**Refactor:** Replace all `.or_insert_with(Vec::new)` with `.or_default()`.

---

## 8. `Default` impl missing on several types (Low Priority)

**Files:** `src/rules/query.rs` (`QueryEngine`), `src/rules/cinder/compiler.rs` (`CinderCompiler`), `src/rules/context.rs` (`ContextAnalyzer`), `src/rules/dataflow.rs` (`DataflowTracker`)

All four types implement `new() -> Self` with no state arguments but do **not** `#[derive(Default)]` or `impl Default`. This means they cannot be used with `..Default::default()` struct update syntax or in generic contexts requiring `Default`.

**Refactor:** Add `impl Default for QueryEngine { fn default() -> Self { Self::new() } }` (and equivalent) for each type. For `DataflowTracker` and `ContextAnalyzer` the `Default` impls are already written but call `Self::new()` manually — replace with `#[derive(Default)]` where all fields are `Default`.

---

## 9. `Parser` Re-created on Every Parse Call (Low Priority / Perf)

**Files:** `src/graph/python.rs:30`, `src/graph/dlt.rs:35`, `src/rules/query.rs:44`

`tree_sitter::Parser::new()` is called every time `build_from_source` or `parse_source` is invoked. `Parser` is reusable via `parser.reset()`.

**Refactor:** Store the `Parser` inside `QueryEngine`, `PythonGraphBuilder`, and `DltGraphBuilder`, reusing it across calls. This avoids repeated grammar re-initialisation overhead on hot paths (particularly `analyze_directory`).

---

## 10. `detect_mode` (multi-cell) is Unused Dead Code (Low Priority)

**File:** `src/detect.rs:3`

`pub fn detect_mode(cells: &[(String, String)])` exists and is not called from anywhere in the codebase. It also contains a logic bug: `all_sql` is set to `false` on the very first iteration and never set back, making the `Sql` branch unreachable.

**Refactor:** Remove `detect_mode` entirely, or fix the logic and wire it to the notebook ingestion path if multi-cell detection is actually needed.

---

## 11. `check_star_import_pyspark` Boolean Short-Circuit Bug (Low Priority)

**File:** `src/rules/context.rs:~225`

```rust
if trimmed.contains("from pyspark.sql.functions import *")
    || trimmed.contains("from pyspark.sql import functions as F") == false
        && trimmed.starts_with("from pyspark.sql.functions import *")
```

Due to operator precedence, the `== false` negation binds to the second `contains(...)` call only, not the whole OR expression. The intent appears to be "match `import *` only when the safe `as F` form isn't present", but the condition is equivalent to the first clause alone. The `== false` branch adds no filtering and is misleading.

**Refactor:** Simplify to `trimmed.contains("from pyspark.sql.functions import *")` or write the intent explicitly with a separate `!trimmed.contains(...)` guard.

---

## 12. `RuleTable` Misses `Default` and `impl Default` (Low Priority)

**File:** `src/types.rs:~248`

`RuleTable` has a `#[pymethods] fn new()` but no `impl Default`. It is a bitset wrapper and trivially defaultable.

**Refactor:** Add `impl Default for RuleTable { fn default() -> Self { Self::new() } }`.

---

## Summary Table

| # | File(s) | Issue | Impact |
|---|---------|-------|--------|
| 1 | `types.rs`, `rules/mod.rs` | Duplicate `Finding` structs | High |
| 2 | `lib.rs` | `mode_to_lang` + graph dispatch duplicated 4×  | High |
| 3 | `rules/mod.rs` | `RulePipeline` rebuilt on every `run()` | High |
| 4 | `rules/mod.rs` | `eprintln!("DEBUG …")` in production | High |
| 5 | `rules/mod.rs` | Inconsistent `"All"` vs `"all"` language match | Medium |
| 6 | `rules/dataflow.rs:174` | Dead `known_dfs` variable before map population | Medium |
| 7 | Multiple | `.or_insert_with(Vec::new)` → `.or_default()` | Low |
| 8 | `query.rs`, `compiler.rs`, `context.rs`, `dataflow.rs` | `Default` trait not implemented | Low |
| 9 | `graph/python.rs`, `graph/dlt.rs`, `rules/query.rs` | `Parser` re-created per call | Low |
| 10 | `detect.rs` | Dead + buggy `detect_mode` function | Low |
| 11 | `rules/context.rs` | Boolean precedence bug in star-import check | Low |
| 12 | `types.rs` | `RuleTable` missing `impl Default` | Low |

## Verification

After refactoring, the task specifies:

```
cargo test      # must pass
pytest          # must pass
cargo clippy    # must pass (zero warnings)
ruff check      # must pass (Python side unchanged but verify)
```

The changes above are all internal — no public Python API surface changes.