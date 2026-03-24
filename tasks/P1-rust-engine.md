# Phase 1: Rust Engine

> `burnt-engine`: parse any notebook, detect mode, build CostGraph or PipelineGraph, evaluate 84 rules, expose via PyO3.

**Duration:** 6 weeks
**Gate:** `cargo test` green. `import burnt_engine` works. ≥5× throughput. v1 parity tests pass.

---

## Tasks

### P1-01: Cargo Setup & Core Types (Week 1)

`Cell`, `CellKind` (Python/Sql/RunRef), `AnalysisMode` (Python/Sql/Dlt), `Finding`, `Severity`, `Confidence`, `RuleEntry`, `RuleTable` (128-bit bitset). `build.rs` reads `rules/registry.toml` → static `REGISTRY` (84 entries). `maturin develop` → importable module.

### P1-02: Format Parsers (Week 1)

5 formats: Databricks `.py`, plain `.py`, plain `.sql`, `.ipynb`, DBSQL `.sql`. `classify_magic()` routes cells. `byte_offset` per cell. Malformed → Finding, no panic.

### P1-03: `%run` Resolution (Week 1–2)

`parse_and_resolve(path, root)` → flat `Vec<Cell>`, Python and Sql only. Recursive inline. Cycle detection. Circular → BN003. Missing → BN001. `origin_path` tracks source file.

### P1-04: tree-sitter Python (Week 2)

Grammar init once. DLT/SDP detection from AST: `import dlt`, `from dlt import`, `@dlt.table`, `@dp.table`, `@dp.materialized_view`. SQL extraction: `spark.sql(string)` → fragment, `spark.sql(f_string)` → BN002. Syntax error → partial tree + finding.

### P1-05: tree-sitter SQL + sqlparser-rs (Week 2)

tree-sitter-sql for patterns. sqlparser-rs `DatabricksDialect::default()` for typed AST. DLT SQL: `CREATE STREAMING TABLE`, `CREATE MATERIALIZED VIEW`, `LIVE.ref`. `SqlFragment` with provenance.

### P1-06: Mode Detection (Week 2)

After %run resolution: DLT signal → Dlt. All cells SQL → Sql. Otherwise → Python.

### P1-07: Semantic Model (Week 3)

`SemanticModel`: scope stack, `bind()` with overwrite findings, `classify_rhs` for 14 patterns including `dlt.read`, `dp.read`, `spark.readStream`. `ChainContext`: has_limit, has_select, has_filter, action, is_streaming.

### P1-08: Python-Mode CostGraph Builder (Week 3)

SemanticModel → CostGraph. Operations → CostNodes. Bindings → CostEdges. OperationKind, ScalingBehavior type, photon_eligible, shuffle_required, driver_bound.

### P1-09: SQL-Mode CostGraph Builder (Week 3–4)

SQL cells → CostGraph via sqlparser-rs decomposition. CREATE TABLE AS SELECT → chain. MERGE INTO → chain. OPTIMIZE → Maintenance. Cross-cell table deps → edges.

### P1-10: DLT PipelineGraph Builder (Week 4)

`@dlt.table`/`@dp.table` functions and `CREATE STREAMING TABLE` SQL → PipelineGraph. Streaming/MV/temp classification. `dlt.read()`/`dp.read()`/`LIVE.ref` → edges. Inner CostNodes from function bodies. Expectations from decorators/CONSTRAINT.

### P1-11: Tier 1 Rules (~48 TOML) (Week 4–5)

~28 PySpark, ~15 SQL (incl. BSQ001-003), ~5 DLT (DLT002-003). TOML + tree-sitter queries. Fixture + insta snapshot per rule.

### P1-12: Tier 2 Context Rules (~25 Rust) (Week 5)

BP010, BP011, BP012, BP013, BP029, BNT-W/L/D/SP/J/T/M rules, BSQ004-005, DLT001/004/005. Fixture + snapshot per rule.

### P1-13: Tier 3 Semantic Rules (~11 Rust) (Week 5)

BP020, BP024, BP031, BNT-T01, BP019. Cross-cell, cross-%run.

### P1-14: Rule Pipeline & Suppression (Week 5)

TOML → CompiledRule. Phase execution (0–6). Suppression: codes and names. DLT escalation. Sorted output. Graph node linking.

### P1-15: PyO3 Bridge (Week 6)

`analyze_file`, `analyze_source`, `analyze_directory`. serde_json → PyDict. Returns: `mode`, `graph`/`pipeline`, `findings`, `cells`. `analyze_directory` releases GIL, rayon parallel.

### P1-16: Parity Validation (Week 6)

Backward shim: `detect_antipatterns()` calls Rust. Rust ≥ Python on all fixtures. proptest: no panics 10k inputs. ≥5× throughput. v1 tests pass.

---

## Gate

- [ ] `cargo test` all modules
- [ ] 4 platform wheels build
- [ ] Rust ≥ Python on all fixtures
- [ ] v1 tests pass via shim
- [ ] ≥5× throughput
- [ ] Correct mode detection
- [ ] CostGraph for Python + SQL, PipelineGraph for DLT
- [ ] 84 rules with fixtures
