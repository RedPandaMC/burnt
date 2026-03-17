# Task: burnt check — Multi-format Notebook & Script Ingestion (Rust)

---

## Metadata

```yaml
id: s7-01-notebook-ingestion
status: todo
phase: 7
priority: high
agent: ~
blocked_by: [s6-11-rust-acceleration]
created_by: planner
```

---

## Context

### Goal

Implement the ingestion layer entirely in Rust inside `src/burnt_rs/`. Every Databricks code format is read from disk and split into typed `Cell` structs in Rust — no Python involved. This is the foundation that s7-02, s7-03, and s7-04 build on. Python never touches raw source text during analysis.

### Files to read

```
# Required
src/burnt_rs/src/lib.rs        # existing Maturin module (from s6-11)
src/burnt_rs/Cargo.toml        # dependency manifest to extend
src/burnt/cli/main.py          # file collection loop — will be replaced by Rust call

# Reference
tasks/s6-11-rust-acceleration.md
```

### Background

**Every Databricks code format to handle**

| Format | Detection | Cell language |
|--------|-----------|---------------|
| Plain `.py` | file extension, no notebook header | Python |
| Plain `.sql` | file extension | SQL |
| Databricks notebook export `.py` | first line `# Databricks notebook source` | per-cell magic |
| Jupyter notebook `.ipynb` | JSON `nbformat` key | per-cell kernel + magic override |
| DLT pipeline `.py` | `import dlt` or `@dlt.table` in first 20 lines | Python (DLT context) |
| DBSQL notebook `.sql` | `-- Databricks notebook source` header | SQL |

**`Cell` struct in Rust**

```rust
// src/burnt_rs/src/notebook.rs

#[derive(Debug, Clone, PartialEq)]
pub enum CellLanguage {
    Python,
    Sql,
    Run(String),   // %run path — unresolvable reference
    Shell,         // %sh — flag for security patterns later
    Other,         // %md, %r, %scala — skip
}

#[derive(Debug, Clone)]
pub struct Cell {
    pub language: CellLanguage,
    pub source: String,
    pub index: usize,          // position within the file
    pub path: PathBuf,
    pub line_offset: usize,    // first line of this cell in the original file
    pub is_dlt: bool,          // true if the file is a DLT pipeline
}
```

**Databricks notebook export parser**

The `# COMMAND ----------` separator and `# MAGIC %<lang>` prefixes are entirely ASCII — trivial to parse in Rust with no allocations beyond the cell strings:

```rust
pub fn parse_notebook_export(source: &str, path: &Path) -> Vec<Cell> {
    const SEP: &str = "# COMMAND ----------";
    const MAGIC: &str = "# MAGIC ";

    let mut cells = Vec::new();
    let mut line_offset = 1usize;

    // skip header line "# Databricks notebook source"
    let body = source.trim_start_matches("# Databricks notebook source").trim_start_matches('\n');

    for block in body.split(SEP) {
        let trimmed = block.trim();
        if trimmed.is_empty() {
            line_offset += block.chars().filter(|&c| c == '\n').count() + 1;
            continue;
        }

        let (language, cell_source) = if trimmed.lines().all(|l| {
            l.trim_start().starts_with(MAGIC) || l.trim().is_empty()
        }) {
            // All lines are magic — strip prefix, detect language
            let stripped: String = trimmed
                .lines()
                .map(|l| l.trim_start().strip_prefix(MAGIC).unwrap_or(""))
                .collect::<Vec<_>>()
                .join("\n");
            let lang = detect_magic_language(&stripped);
            (lang, stripped)
        } else {
            (CellLanguage::Python, trimmed.to_string())
        };

        cells.push(Cell {
            language,
            source: cell_source,
            index: cells.len(),
            path: path.to_path_buf(),
            line_offset,
            is_dlt: false,  // set by caller after full parse
        });

        line_offset += block.chars().filter(|&c| c == '\n').count() + 1;
    }
    cells
}

fn detect_magic_language(stripped_source: &str) -> CellLanguage {
    let first = stripped_source.lines().next().unwrap_or("").trim();
    match first {
        s if s.starts_with("%sql")    => CellLanguage::Sql,
        s if s.starts_with("%python") => CellLanguage::Python,
        s if s.starts_with("%sh")     => CellLanguage::Shell,
        s if s.starts_with("%run")    => {
            let path = s.strip_prefix("%run").unwrap_or("").trim().to_string();
            CellLanguage::Run(path)
        }
        _ => CellLanguage::Other,
    }
}
```

**Jupyter `.ipynb` parser**

Use `serde_json` — no external notebook library needed:

```rust
// Cargo.toml additions:
// serde = { version = "1", features = ["derive"] }
// serde_json = "1"

use serde_json::Value;

pub fn parse_ipynb(source: &str, path: &Path) -> Vec<Cell> {
    let nb: Value = serde_json::from_str(source).unwrap_or(Value::Null);
    let default_lang = nb["metadata"]["kernelspec"]["language"]
        .as_str()
        .unwrap_or("python")
        .to_lowercase();

    nb["cells"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .enumerate()
        .filter(|(_, cell)| cell["cell_type"].as_str() == Some("code"))
        .map(|(i, cell)| {
            let source: String = cell["source"]
                .as_array()
                .unwrap_or(&vec![])
                .iter()
                .filter_map(|s| s.as_str())
                .collect();
            let lang = detect_magic_language(source.lines().next().unwrap_or(""))
                .or_default_to(&default_lang);
            Cell {
                language: lang,
                source,
                index: i,
                path: path.to_path_buf(),
                line_offset: 0,   // ipynb has no global line numbers
                is_dlt: false,
            }
        })
        .collect()
}
```

**DLT detection**

Scan the first 30 lines for DLT markers. If found, set `is_dlt = true` on all cells:

```rust
pub fn is_dlt_file(source: &str) -> bool {
    source.lines().take(30).any(|l| {
        l.contains("import dlt")
            || l.contains("@dlt.table")
            || l.contains("@dp.table")
            || l.contains("@dp.materialized_view")
    })
}
```

**Top-level dispatch**

```rust
// src/burnt_rs/src/notebook.rs
pub fn parse_file(path: &Path) -> Vec<Cell> {
    let source = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(_) => return vec![],
    };

    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    let is_dlt = is_dlt_file(&source);

    let mut cells = match ext {
        "ipynb" => parse_ipynb(&source, path),
        "sql"   => vec![Cell {
            language: CellLanguage::Sql,
            source,
            index: 0,
            path: path.to_path_buf(),
            line_offset: 1,
            is_dlt: false,
        }],
        "py" if source.starts_with("# Databricks notebook source") => {
            parse_notebook_export(&source, path)
        }
        _ => vec![Cell {
            language: CellLanguage::Python,
            source,
            index: 0,
            path: path.to_path_buf(),
            line_offset: 1,
            is_dlt,
        }],
    };

    if is_dlt {
        for cell in &mut cells {
            cell.is_dlt = true;
        }
    }
    cells
}
```

**PyO3 export for testing**

```rust
// lib.rs — expose for Python-level tests
#[pyfunction]
pub fn parse_cells(path: &str) -> PyResult<Vec<PyObject>> {
    // ... convert Vec<Cell> to Python dicts for test assertions
}
```

**Cargo.toml additions**

```toml
[dependencies]
serde_json = "1"
serde = { version = "1", features = ["derive"] }
```

---

## Acceptance Criteria

- [ ] `parse_file("script.py")` → 1 Python cell
- [ ] `parse_file("query.sql")` → 1 SQL cell
- [ ] `parse_file("notebook.ipynb")` with 3 Python cells + 1 SQL cell → 4 cells, correct language tags
- [ ] Databricks export `.py` with `%sql` magic → SQL cells extracted with correct `line_offset`
- [ ] `%run ./helpers` cells → `CellLanguage::Run("./helpers")`, not analysed
- [ ] DLT file detection: `import dlt` in source → `is_dlt = true` on all cells
- [ ] Malformed JSON `.ipynb` → empty `Vec<Cell>`, no panic
- [ ] `burnt_rs.parse_cells(path)` callable from Python tests
- [ ] `cargo test` passes; `maturin develop` builds without error

---

## Verification

```bash
cd src/burnt_rs && cargo test
maturin develop --release

python3 -c "
import burnt_rs, json
# Test notebook export
import pathlib
src = '# Databricks notebook source\n\n# COMMAND ----------\n\ndf = spark.read.csv(\"/data\")\n\n# COMMAND ----------\n\n# MAGIC %sql\n# MAGIC SELECT * FROM orders\n'
pathlib.Path('/tmp/nb.py').write_text(src)
cells = burnt_rs.parse_cells('/tmp/nb.py')
assert cells[0]['language'] == 'python', cells
assert cells[1]['language'] == 'sql', cells
assert cells[1]['line_offset'] == 9, cells
print('OK', cells)
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

Requires s6-11 Phase 2 (Maturin workspace) to be complete.
