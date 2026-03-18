# Task s7-01: Foundation, Ingestion & `%run` Resolution

## Metadata

```yaml
id: s7-01-foundation-ingestion
status: todo
phase: 7
priority: critical
agent: ~
blocked_by: [s6-11-rust-acceleration]
created_by: planner
```

## Goal

Build every foundational Rust type (`Cell`, `CellKind`, `Finding`, `RuleTable`, rule registry), implement all Databricks format parsers, and — critically — implement recursive `%run` inline resolution with cycle detection. After this task, `parse_and_resolve(path, root)` returns a flat `Vec<Cell>` containing only `Python` and `Sql` cells, with `%run` targets already inlined in execution order. No downstream component ever sees a `RunRef`.

## Background

### `CellKind` — three variants, nothing else

```rust
pub enum CellKind {
    Python,
    Sql,
    RunRef(String),  // Transient. Resolved to inlined cells before analysis.
}
```

`%sh`, `%md`, `%r`, `%scala` — filtered out at parse time. They don't get a `Cell` struct. They don't enter the pipeline. The ingestion parser sees the magic prefix and skips to the next `# COMMAND ----------`.

```rust
fn classify_magic(first_line: &str) -> Option<CellKind> {
    let trimmed = first_line.trim();
    if trimmed.starts_with("%sql") {
        Some(CellKind::Sql)
    } else if trimmed.starts_with("%run") {
        let path = trimmed.strip_prefix("%run").unwrap_or("").trim();
        Some(CellKind::RunRef(path.to_string()))
    } else if trimmed.starts_with("%sh")
        || trimmed.starts_with("%md")
        || trimmed.starts_with("%r ")    // %r not %run
        || trimmed.starts_with("%scala")
    {
        None  // SKIP. No Cell created. Does not exist downstream.
    } else if trimmed.starts_with("%python") {
        Some(CellKind::Python)
    } else {
        Some(CellKind::Python)  // default
    }
}
```

### `Cell` struct

```rust
#[derive(Debug, Clone)]
pub struct Cell {
    pub kind: CellKind,
    pub source: String,
    pub index: u32,             // position in the resolved cell list
    pub origin_path: PathBuf,   // which notebook this cell came from (may differ from analysis target)
    pub analysis_path: PathBuf, // the notebook being analyzed (the one the user passed to `burnt check`)
    pub byte_offset: usize,     // byte position of first char in the ORIGIN file
    pub is_dlt: bool,           // set after AST-based detection in s7-02
}
```

`origin_path` and `analysis_path` are separate because `%run` inlines cells from other files. A finding in `shared/helpers.py` line 42 (reached via `%run` from `main.py`) must report: "file: shared/helpers.py, line: 42, referenced from: main.py cell 1."

### `Finding` — full metadata from the start

```rust
#[derive(Debug, Clone)]
pub struct Finding {
    pub rule_id: &'static str,     // "collect_without_limit"
    pub code: &'static str,        // "BP001"
    pub severity: Severity,
    pub confidence: Confidence,
    pub file: PathBuf,             // origin_path — where the violation IS
    pub cell_index: Option<u32>,
    pub byte_range: TextRange,
    pub description: String,
    pub suggestion: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity { Error, Warning, Info, Style }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confidence { High, Medium, Low }
```

### `RuleTable` — bitset, not HashSet

```rust
pub struct RuleTable {
    bits: [u64; 2],  // 128 rules capacity
}

impl RuleTable {
    pub fn all() -> Self { /* set all registered bits */ }
    pub fn from_select_ignore(select: &[&str], ignore: &[&str]) -> Self { ... }

    #[inline(always)]
    pub fn is_enabled(&self, idx: u16) -> bool {
        self.bits[(idx / 64) as usize] & (1u64 << (idx % 64)) != 0
    }
}
```

### Rule registry — static array, 76 entries

```rust
pub struct RuleEntry {
    pub idx: u16,
    pub id: &'static str,       // "collect_without_limit"
    pub code: &'static str,     // "BP001"
    pub severity: Severity,
    pub confidence: Confidence,
    pub language: Language,      // Python, Sql, All
}

pub static REGISTRY: &[RuleEntry] = &[
    RuleEntry { idx: 0, id: "collect_without_limit", code: "BP001", severity: Severity::Error, confidence: Confidence::High, language: Language::Python },
    // ... all 76 rules from registry.py
];
```

### Format parsers — dispatch by extension and header

| Format | Detection | Output |
|--------|-----------|--------|
| Plain `.py` | Extension, no notebook header | 1 Python cell |
| Plain `.sql` | Extension | 1 SQL cell |
| Databricks export `.py` | `# Databricks notebook source` header | N Python/SQL cells, `%run` RunRefs |
| Jupyter `.ipynb` | JSON `nbformat` key | N Python/SQL cells from code cells |
| DBSQL export `.sql` | `-- Databricks notebook source` header | N SQL cells |

Non-Python/SQL cells in notebooks are filtered by `classify_magic()` at parse time — they never become `Cell` structs.

### `%run` resolution — recursive inline with cycle detection

This is the most important new function. `%run` is not metadata — it's synchronous code injection. The linter must read the target notebook, parse it, and insert its cells before the cells that follow the `%run` in the calling notebook.

```rust
pub fn parse_and_resolve(path: &Path, root: &Path) -> (Vec<Cell>, Vec<Finding>) {
    let mut diagnostics = Vec::new();
    let mut seen = HashSet::new();
    seen.insert(path.canonicalize().unwrap_or_else(|_| path.to_path_buf()));

    let source = match read_source(path) {
        Ok(s) => s,
        Err(e) => {
            diagnostics.push(Finding::error(
                "file_read_error", "BE002", 0,
                format!("Cannot read {}: {}", path.display(), e),
                "Check file path and permissions",
            ));
            return (vec![], diagnostics);
        }
    };

    let raw_cells = parse_file_from_source(&source, path);
    let resolved = resolve_runs(raw_cells, path, root, &mut seen, &mut diagnostics);
    (resolved, diagnostics)
}

fn resolve_runs(
    cells: Vec<Cell>,
    caller_path: &Path,
    root: &Path,
    seen: &mut HashSet<PathBuf>,
    diagnostics: &mut Vec<Finding>,
) -> Vec<Cell> {
    let mut resolved = Vec::new();

    for cell in cells {
        match &cell.kind {
            CellKind::RunRef(target) => {
                let target_path = resolve_run_path(caller_path, target, root);

                let canonical = target_path.canonicalize()
                    .unwrap_or_else(|_| target_path.clone());

                // Cycle detection
                if seen.contains(&canonical) {
                    diagnostics.push(Finding::error(
                        "circular_run_reference", "BN003", cell.byte_offset as u32,
                        format!("Circular %run: {} → {}", caller_path.display(), target_path.display()),
                        "Break the circular dependency between notebooks",
                    ));
                    continue;
                }

                // Read and parse target
                match read_source(&target_path) {
                    Ok(target_source) => {
                        seen.insert(canonical.clone());
                        let target_cells = parse_file_from_source(&target_source, &target_path);
                        // Recurse — target may have its own %run cells
                        let inlined = resolve_runs(target_cells, &target_path, root, seen, diagnostics);
                        resolved.extend(inlined);
                        seen.remove(&canonical);
                    }
                    Err(_) => {
                        diagnostics.push(Finding::warning(
                            "unresolved_run_target", "BN001", cell.byte_offset as u32,
                            format!("%run {} — target not found in analysis scope", target),
                            "Ensure the target notebook is in the analysis directory",
                        ));
                    }
                }
            }
            // Python and SQL cells pass through unchanged
            _ => resolved.push(cell),
        }
    }

    resolved
}

fn resolve_run_path(caller: &Path, target: &str, root: &Path) -> PathBuf {
    let target = target.trim();
    let base = caller.parent().unwrap_or(root);

    let raw = if target.starts_with('/') {
        root.join(target.trim_start_matches('/'))
    } else {
        base.join(target)
    };

    // Databricks %run doesn't require file extension
    if raw.exists() { return raw; }
    let with_py = raw.with_extension("py");
    if with_py.exists() { return with_py; }
    raw // return as-is, will fail at read_source and produce BN001
}
```

After `resolve_runs()`, the returned `Vec<Cell>` contains **only `CellKind::Python` and `CellKind::Sql`**. `RunRef` is fully consumed. No downstream code ever pattern-matches on `RunRef`.

### Re-indexing after inlining

After `%run` resolution, cell indices need to reflect the resolved order:

```rust
fn reindex_cells(cells: &mut Vec<Cell>) {
    for (i, cell) in cells.iter_mut().enumerate() {
        cell.index = i as u32;
    }
}
```

## Acceptance Criteria

- [ ] `CellKind` has 3 variants: `Python`, `Sql`, `RunRef(String)` — nothing else
- [ ] `%sh`, `%md`, `%r`, `%scala` cells are filtered at parse time — never become `Cell` structs
- [ ] `parse_and_resolve(path, root)` returns only `Python` and `Sql` cells — all `RunRef` resolved
- [ ] `%run ./helpers` reads helpers file, parses it, inlines its cells before the next cell
- [ ] Recursive: `%run ./a` where `a` does `%run ./b` → both inlined correctly
- [ ] Circular `%run` → `circular_run_reference` ERROR (BN003), no infinite loop
- [ ] `%run` target not found → `unresolved_run_target` WARNING (BN001)
- [ ] `origin_path` on each cell correctly identifies which file the cell came from
- [ ] `byte_offset` is byte position in the origin file, not line number
- [ ] `Finding` struct has `code`, `cell_index`, `byte_range`, `confidence`
- [ ] `RuleTable` bitset: `is_enabled()` is single bitwise AND
- [ ] Static `REGISTRY` with all 76 rules, indexed by `u16`
- [ ] All 6 Databricks formats parsed correctly
- [ ] Malformed `.ipynb` JSON → empty cell list, no panic
- [ ] `cargo test foundation && cargo test ingestion && cargo test run_resolution` pass

## Verification

```bash
cd src/burnt_rs && cargo test foundation
cd src/burnt_rs && cargo test ingestion
cd src/burnt_rs && cargo test run_resolution

# %run resolution test
python3 -c "
import pathlib, tempfile

d = pathlib.Path(tempfile.mkdtemp())
# Helper notebook
(d / 'helpers.py').write_text('''# Databricks notebook source

# COMMAND ----------

def read_orders():
    return spark.sql('SELECT * FROM orders')
''')
# Main notebook that %runs helpers
(d / 'main.py').write_text('''# Databricks notebook source

# COMMAND ----------

# MAGIC %run ./helpers

# COMMAND ----------

df = read_orders()
df.collect()
''')

import burnt_rs
cells = burnt_rs.parse_and_resolve(str(d / 'main.py'), str(d))
# Should have: helper cells + main cells, NO RunRef
kinds = [c['kind'] for c in cells]
assert 'run_ref' not in kinds, f'RunRef should be resolved: {kinds}'
assert len(cells) >= 2, f'Expected inlined cells: {cells}'
# The helper's Python cell should come BEFORE main's cells
print(f'OK: {len(cells)} cells, kinds={kinds}')
print(f'Cell origins: {[c[\"origin_path\"] for c in cells]}')
"
```
