# Task s7-06: Testing, Benchmarks & Parity Validation

## Metadata

```yaml
id: s7-06-testing
status: todo
phase: 7
priority: high
agent: ~
blocked_by: [s7-05-pyo3-cli]
created_by: planner
```

## Goal

Seven-layer testing infrastructure. Every rule has a fixture. Fuzzer runs without panics. Benchmark proves ≥5× throughput. Parity validator confirms Rust ≡ Python findings on all 451+ existing tests. `%run` resolution has dedicated multi-notebook test corpus.

## Background

### Layer 1: Snapshot testing (insta)

One fixture per rule/rule-group. Glob-based test auto-discovers new fixtures.

```rust
#[test]
fn test_pyspark_rules() {
    insta::glob!("fixtures/pyspark/*.py", |path| {
        let source = std::fs::read_to_string(path).unwrap();
        let findings = analyze_single_source(&source, "python");
        insta::assert_yaml_snapshot!(normalize(&findings));
    });
}
```

### Layer 2: Property testing (proptest)

Parser never panics on arbitrary input. Linter is deterministic (same input → same findings).

### Layer 3: Fuzzing (cargo-fuzz)

```rust
fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = analyze_single_source(s, "python");  // must not panic
    }
});
```

### Layer 4: PyO3 integration (pytest)

Test `analyze_file`, `analyze_source`, `analyze_directory` from Python.

### Layer 5: Parity validation

```python
@pytest.mark.parametrize("fixture", FIXTURE_DIR.glob("pyspark/*.py"))
def test_parity(fixture):
    source = fixture.read_text()
    rust = {f["rule_id"] for f in burnt_rs.analyze_source(source, fixture.name, ["ALL"], [])}
    python = {p.name for p in detect_antipatterns(source, "pyspark")}
    missing = python - rust
    assert not missing, f"Rust missing {missing} on {fixture.name}"
```

### Layer 6: Benchmarks

`criterion` for Rust microbenchmarks. `hyperfine` for CLI end-to-end. Target: ≥5× vs Python.

### Layer 7: Security

`cargo audit` + `cargo deny` in CI.

### `%run` test corpus — multi-notebook fixtures

```
tests/fixtures/notebooks/run_resolution/
├── main.py                      # %run ./helpers → inlines helpers cells
├── helpers.py                   # def read_orders(): return spark.sql(...)
├── chained_a.py                 # %run ./chained_b → tests recursive resolution
├── chained_b.py                 # defines functions used by chained_a
├── circular_a.py                # %run ./circular_b
├── circular_b.py                # %run ./circular_a → BN003 circular_run_reference
├── missing_target.py            # %run ./nonexistent → BN001 unresolved_run_target
└── cross_file_cache.py          # %run ./helpers, then df.cache() on helper's df → BP020
```

Test cases:

```python
def test_run_inline_basic(fixtures_dir):
    findings = burnt_rs.analyze_file(
        str(fixtures_dir / "run_resolution" / "main.py"),
        ["ALL"], [],
    )
    # helpers.py defines read_orders() with spark.sql("SELECT * FROM orders")
    # main.py calls read_orders().collect() — should fire collect_without_limit
    assert any(f["rule_id"] == "collect_without_limit" for f in findings)

def test_run_circular(fixtures_dir):
    findings = burnt_rs.analyze_file(
        str(fixtures_dir / "run_resolution" / "circular_a.py"),
        ["ALL"], [],
    )
    assert any(f["rule_id"] == "circular_run_reference" for f in findings)

def test_run_missing_target(fixtures_dir):
    findings = burnt_rs.analyze_file(
        str(fixtures_dir / "run_resolution" / "missing_target.py"),
        ["ALL"], [],
    )
    assert any(f["rule_id"] == "unresolved_run_target" for f in findings)

def test_run_cross_file_semantic(fixtures_dir):
    """Cache in main.py on a DataFrame created in helpers.py via %run"""
    findings = burnt_rs.analyze_file(
        str(fixtures_dir / "run_resolution" / "cross_file_cache.py"),
        ["ALL"], [],
    )
    # cache_without_unpersist should fire even though the df came from a %run'd file
    assert any(f["rule_id"] == "cache_without_unpersist" for f in findings)
```

## Acceptance Criteria

- [ ] Every rule has at least one fixture file
- [ ] `insta` snapshots committed for all fixtures
- [ ] `proptest`: parser never panics on 10,000 iterations
- [ ] `cargo fuzz run` → 0 crashes on 100,000 iterations
- [ ] PyO3 integration tests pass
- [ ] Parity: Rust ≥ Python findings on all existing fixtures
- [ ] `%run` resolution test corpus: basic inline, recursive, circular, missing target, cross-file semantic
- [ ] Benchmark: ≥5× throughput vs Python baseline
- [ ] `cargo audit` and `cargo deny` pass
- [ ] All 451+ existing Python unit tests pass via compat shim
