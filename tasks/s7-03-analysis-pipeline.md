# Task s7-03: Index-Based HybridAST & Analysis Pipeline

## Metadata

```yaml
id: s7-03-analysis-pipeline
status: todo
phase: 7
priority: critical
agent: ~
blocked_by: [s7-01-foundation-ingestion, s7-02-dual-parser-semantic]
created_by: planner
```

## Goal

Build the `HybridAST`, the `BurntVisitor`, suppression handling, and the 4-phase analysis pipeline. The pipeline receives the already-resolved cell list from s7-01 (no `RunRef` cells), parses in parallel, analyzes sequentially, and produces `Vec<Finding>`.

## Background

### HybridAST — index-based, no lifetime parameters

```rust
pub struct HybridAST {
    pub cells: Vec<Cell>,                            // only Python and Sql
    pub py_asts: Vec<Option<Parsed<ModModule>>>,     // indexed by cell.index
    pub sql_fragments: Vec<SqlFragment>,             // indexed by SqlFragIdx
    pub sql_to_call_site: Vec<CallSite>,             // parallel to sql_fragments
    pub semantic: SemanticModel,
    pub line_indices: HashMap<PathBuf, LineIndex>,    // one per origin file
    pub is_dlt: bool,
}

pub struct SqlFragment {
    pub ast: Arc<Vec<SqlStatement>>,
    pub raw: String,
    pub source_line: u32,
    pub call_form: SqlCallForm,
}

pub enum SqlCallForm { SparkSql, SelectExpr, FilterString, ExprCall, SqlCell, MagicSql }

pub struct CallSite {
    pub cell_idx: u32,
    pub byte_range: TextRange,
    pub call_form: SqlCallForm,
    pub chain_context: ChainContext,
}
```

### `BurntVisitor` — single walk per cell, multiple responsibilities

SQL extraction distinguishes `Expr::StringLiteral` (parse as SQL) from `Expr::FString` (emit `dynamic_sql_unanalyzable` INFO) at the AST level. No string heuristics.

```rust
fn try_extract_sql(&mut self, node: &ExprCall, method: &str, ...) {
    for arg in &node.arguments.args {
        match arg {
            Expr::StringLiteral(s) => { /* parse SQL, store fragment */ }
            Expr::FString(_) => {
                self.findings.push(Finding::info(
                    "dynamic_sql_unanalyzable", "BN002", line,
                    "f-string in spark.sql() — cannot analyze statically",
                    "Use a string literal for lintable SQL",
                ));
            }
            _ => {} // .format(), concatenation — also unanalyzable
        }
    }
}
```

### Suppression in Rust

```rust
pub struct SuppressionMap {
    line_suppressions: HashMap<u32, HashSet<String>>,  // line → rule ids
    file_suppressions: HashSet<String>,                 // top-of-file
}
```

Parses `# burnt: ignore[rule_id]` (line-level) and `# burnt: ignore-file[rule_id]` (file-level, first 5 lines). Accepts both rule codes (`BP001`) and rule names (`collect_without_limit`).

### 4-Phase pipeline

```
Phase 0: Source-text rules (backslash continuation — raw text, not AST)
Phase 1: Parallel parse — all cells via rayon (Python → ruff, SQL → sqlparser)
Phase 2: Sequential semantic walk — BurntVisitor per cell in resolved order
           (extracts SQL, updates semantic model, computes ChainContext, fires inline rules)
Phase 3: SQL rule pass — iterate sql_fragments with ChainContext
Phase 4: Post-processing — DLT severity escalation, suppression filtering, sort
```

Note: The cell list entering Phase 0 is already `%run`-resolved from s7-01. Every cell is either `Python` or `Sql`. The pipeline has no `RunRef` handling — that's done.

## Acceptance Criteria

- [ ] `HybridAST` uses indices, no lifetime references
- [ ] Phase 1 parallel parse via rayon
- [ ] Phase 2 sequential walk: one `BurntVisitor` pass per cell handles SQL extraction + semantic model + ChainContext + rule dispatch
- [ ] SQL extraction uses AST type check (`StringLiteral` vs `FString`), not string heuristic
- [ ] f-string in `spark.sql()` → `dynamic_sql_unanalyzable` INFO
- [ ] Suppression: `# burnt: ignore[BP001]` and `# burnt: ignore[collect_without_limit]` both work
- [ ] Syntax errors → `Finding` emitted, partial AST still walked
- [ ] DLT severity escalation: `global_auto_merge_schema` → ERROR in DLT
- [ ] Findings sorted by file, line for stable output
- [ ] No `CellKind::RunRef` match arms anywhere in the pipeline
- [ ] `cargo test pipeline` passes
