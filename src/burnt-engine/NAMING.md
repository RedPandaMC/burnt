# Naming Conventions — `burnt-engine`

> Code conventions for the Rust engine crate. These are not enforced by the
> compiler but are the agreed-upon style so multiple contributors produce
> consistent code. When in doubt: match the surrounding file.

---

## 1. Module and File Naming

| Thing | Convention | Example |
|-------|------------|---------|
| Source module | `snake_case` | `graph/mod.rs`, `rules/graph_dsl/matcher.rs` |
| Module directory | `snake_case` | `src/resolved/`, `src/rules/graph_dsl/` |
| `pub mod` declaration | exact filename | `pub mod graph;` in `lib.rs` |
| Private helper module | `snake_case`, `mod.rs` when grouped | `semantic/mod.rs` |
| Build script | `build.rs` | `build.rs` at crate root |

---

## 2. Type Naming (Rust)

| Kind | Convention | Example |
|------|------------|---------|
| Struct | `PascalCase` | `Graph`, `PipelineGraph`, `Finding` |
| Enum | `PascalCase` | `EngineError`, `PredResult`, `CellKind` |
| Enum variant | `PascalCase` | `EngineError::Parse`, `PredResult::Bool` |
| Type alias | `PascalCase` | `type PredicateFn = fn(&[PredArg], &MatchCtx) -> PredResult;` |
| Trait | `PascalCase` | `Rule`, `CatalogClient` |

---

## 3. Function and Variable Naming

| Kind | Convention | Example |
|------|------------|---------|
| Public function | `snake_case` | `run_rules`, `ingest_file`, `parse_rule_file` |
| Private helper | `snake_case` | `first_value`, `escape`, `validate_predicate_names` |
| Predicate function | `pred_<name>` | `pred_in_loop`, `pred_shares_receiver_impl` |
| PyO3 wrapper struct | `Py<PascalCase>` | `PyGraph`, `PyPipeline`, `PyResolvedGraph` |
| Boolean getter | `is_<adj>`, `has_<noun>`, `<verb>s` | `is_active`, `has_provenance` |
| Collection | plural noun or `<noun>_vec` | `nodes`, `findings`, `rules`, `test_cases` |

---

## 4. PyO3 Wrapper Convention

Python-facing types are prefixed with `Py`:

| Rust type | Python class name | Notes |
|-----------|-----------------|-------|
| `Graph` | `Graph` (via `PyGraph`) | Conversions via `impl From<Graph> for PyGraph` |
| `PipelineGraph` | `PipelineGraph` (via `PyPipeline`) | |
| `Node` | `Node` (via `PyNode`) | |
| `Edge` | `Edge` (via `PyEdge`) | |

`#[pyclass]` structs use `Py{Entity}` naming so `m.add_class::<PyGraph>()` registers `Graph` in Python.

---

## 5. Predicate Names in DSL Rules

Predicates in TOML `detect = """..."""` blocks use kebab-case with a leading `#`:

```
#and, #or, #not          — composition
#binds, #reads           — dataflow
#in-loop, #method-chain-contains — scope
#shares-receiver         — receiver identity
#arg-is-dynamic          — argument analysis
```

Adding a new predicate:
1. Add `m.insert("<name>", pred_<name>);` in `build_registry()` in `predicate.rs`
2. Add `fn pred_<name>(...)` below in the same file
3. Add `"<name>"` to the `validate_predicate_names()` allowlist in `build.rs`
4. Document the signature in the doc comment above the function

---

## 6. Rule Code Prefixes

Rule codes follow a tier + category system:

| Prefix | Tier | Category |
|--------|------|----------|
| `BP*` | Performance | Python cell-level |
| `BD*` | Performance | Delta / SQL |
| `BJ*` | Performance | JOIN patterns |
| `BN*` | Performance | Notebook structure |
| `BO*` | Observability | |
| `BC*` | Configuration | |
| `BU*` | Governance | |
| `BQ*` | Performance | SQL query |
| `SDP*` | SDP / DLT | |
| `BB*` | Notebook | |

The code prefix is the primary identifier — never assume alphabetical sort order for anything.

---

## 7. Error Variants (EngineError)

Use `thiserror` with kebab-case display strings:

```rust
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("parse error: {0}")]
    Parse(String),

    #[error("file not found: {0}")]
    FileNotFound(String),

    #[error("unsupported file format: {0}")]
    UnsupportedFormat(String),
}
```

Display strings are user-facing — keep them lowercase and actionable.

---

## 8. Graph Node and Edge IDs

Node IDs are kebab-case strings derived from source position:
```
read-at-line-42
join-at-line-15
source-at-line-1
```

Edge kinds are kebab-case:
```
dataflow
alias
scope
```

---

## 9. Imports

- Standard library first, then third-party (`use pyo3::`, `use serde::`, etc.), then `crate::`
- Within `crate::`, prefer grouped imports: `use crate::{graph, rules::finding, types::Finding};`
- Never use wildcard imports in library code
- Suppress `unused_imports` with `#[allow(unused)]` on the import line, not the block

---

## 10. Abbreviations

| Used | Meaning |
|------|---------|
| `ctx` | Match context / analysis context |
| `fqn` | Fully-qualified name |
| `pred` | Predicate function |
| `arg` | Argument |
| `imr`, `imap` | Import map |
| `uc` | Unity Catalog |
| `dabs` | Databricks Assets |
| `sdp` | Streaming Data Pipeline (DLT) |

Avoid single-letter variable names except in very short closures or obvious iterator patterns (`i`, `n`, `k`, `v`).
