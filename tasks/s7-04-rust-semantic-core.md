# Task: burnt check — Rust PySpark Rule Visitor & PyO3 Integration

---

## Metadata

```yaml
id: s7-04-rust-semantic-core
status: todo
phase: 7
priority: high
agent: ~
blocked_by: [s7-03-full-hybrid-ast]
created_by: planner
```

---

## Context

### Goal

Implement `PySparkRuleVisitor` in Rust — the visitor that walks `ruff_python_parser` ASTs and fires all BP and BNT rules. Wire everything into `burnt_rs::analyze_file()` and `burnt_rs::analyze_directory()` as the primary (and only) analysis entry points exposed to Python. `main.py` becomes a thin shell: read CLI args, call `burnt_rs`, format and print findings.

### Files to read

```
# Required
src/burnt_rs/src/hybrid.rs      # HybridChecker (s7-03)
src/burnt_rs/src/semantic.rs    # SparkSemanticModel (s7-02)
src/burnt/cli/main.py           # thin CLI wrapper — stays Python
src/burnt/parsers/pyspark.py    # current Python rules — port to Rust

# Reference
tasks/s7-03-full-hybrid-ast.md
tasks/s6-11-rust-acceleration.md
```

### Background

**`PySparkRuleVisitor` — firing BP and BNT rules from ruff_python_ast**

```rust
// src/burnt_rs/src/rules/pyspark.rs
use ruff_python_ast::{visitor::Visitor, *};

pub struct PySparkRuleVisitor<'a> {
    pub findings: Vec<Finding>,
    cell: &'a Cell,
    is_dlt: bool,
    context_stack: Vec<Context>,
    current_line: u32,
    // Per-function state (reset on function enter/exit)
    in_loop: bool,
    consecutive_with_column: u32,
}

#[derive(Clone, PartialEq)]
enum Context { Loop, SdpFunction, UdfBody }

impl<'a> Visitor<'_> for PySparkRuleVisitor<'a> {
    fn visit_expr_call(&mut self, node: &ExprCall) {
        self.current_line = line_of(node) + self.cell.line_offset as u32;
        let method = extract_method_name(node);

        // Dispatch all enabled call-based rules
        self.check_collect_without_limit(node, &method);
        self.check_to_pandas(node, &method);
        self.check_repartition_one(node, &method);
        self.check_show_left_in(node, &method);
        self.check_jdbc_incomplete_partition(node, &method);
        self.check_window_without_partition_by(node, &method);
        self.check_schema_inference_on_read(node, &method);
        self.check_aggregation_without_alias(node, &method);
        self.check_window_missing_frame_spec(node, &method);
        self.check_first_last_without_ignorenulls(node, &method);
        self.check_debug_call_in_production(node, &method);
        self.check_streaming_await_termination(node, &method);
        self.check_consecutive_with_column(node, &method);
        self.check_join_missing_how(node, &method);
        self.check_expression_join_duplicate_cols(node, &method);
        self.check_create_temp_view(node, &method);
        self.check_spark_session_in_transform(node, &method);
        self.check_shuffle_partitions_in_code(node, &method);
        self.check_global_auto_merge(node, &method);
        self.check_empty_string_instead_of_null(node, &method);
        self.check_sdp_prohibited_ops(node, &method);
        // ... all remaining BP/BNT rule check methods

        self.walk_expr_call(node);
    }

    fn visit_stmt_for(&mut self, node: &StmtFor) {
        // BP026: iterating_over_collect
        self.check_iterating_over_collect(node);

        // BP010: withColumn in loop
        self.context_stack.push(Context::Loop);
        self.in_loop = true;
        self.walk_stmt_for(node);
        self.context_stack.retain(|c| c != &Context::Loop);
        self.in_loop = self.context_stack.contains(&Context::Loop);
    }

    fn visit_stmt_function_def(&mut self, node: &StmtFunctionDef) {
        let is_sdp = has_sdp_decorator(node);
        if is_sdp { self.context_stack.push(Context::SdpFunction); }
        let saved_wc = self.consecutive_with_column;
        self.consecutive_with_column = 0;
        self.walk_stmt_function_def(node);
        if is_sdp { self.context_stack.retain(|c| c != &Context::SdpFunction); }
        self.consecutive_with_column = saved_wc;
    }

    fn visit_stmt_import_from(&mut self, node: &StmtImportFrom) {
        // BNT-I01: star import, BNT-I02: non-F alias, BNT-I03: non-T alias
        self.check_import_aliases(node);
        self.walk_stmt_import_from(node);
    }

    fn visit_stmt_import(&mut self, node: &StmtImport) {
        self.check_import_aliases_direct(node);
        self.walk_stmt_import(node);
    }

    fn visit_stmt_global(&mut self, node: &StmtGlobal) {
        // BNT-SP side effects in SDP
        if self.context_stack.contains(&Context::SdpFunction) {
            self.findings.push(Finding::warning("sdp_side_effects", self.current_line,
                "global statement in SDP function causes non-deterministic behavior",
                "Remove global/nonlocal from SDP pipeline functions"));
        }
    }
}
```

**Rule implementation examples**

```rust
impl<'a> PySparkRuleVisitor<'a> {
    // BP001: collect_without_limit
    fn check_collect_without_limit(&mut self, node: &ExprCall, method: &str) {
        if method != "collect" { return; }
        let chain = preceding_methods(node);
        if !chain.contains("limit") && !chain.contains("take") {
            self.findings.push(Finding::error("collect_without_limit", self.current_line,
                "collect() without limit() can OOM the driver",
                "Add .limit(n).collect() or use .take(n)"));
        }
    }

    // BP011: JDBC incomplete partition (chain walker — same logic as Python fix from s2-05b)
    fn check_jdbc_incomplete_partition(&mut self, node: &ExprCall, method: &str) {
        if !matches!(method, "load" | "save") { return; }
        let mut is_jdbc = false;
        let mut options: HashSet<&str> = HashSet::new();
        let mut obj = receiver(node);
        while let Some(call) = as_method_call(obj) {
            match call.method {
                "format" => if call.first_str_arg() == Some("jdbc") { is_jdbc = true; }
                "jdbc"   => is_jdbc = true,
                "option" => { if let Some(k) = call.first_str_arg() { options.insert(k); } }
                _ => {}
            }
            obj = receiver(call.node);
        }
        if is_jdbc {
            let missing: Vec<&str> = JDBC_REQUIRED.iter()
                .filter(|k| !options.contains(*k)).copied().collect();
            if !missing.is_empty() {
                self.findings.push(Finding::warning("jdbc_incomplete_partition", self.current_line,
                    &format!("JDBC read missing partition options: {}", missing.join(", ")),
                    "Add partitionColumn, numPartitions, lowerBound, upperBound to parallelise JDBC reads"));
            }
        }
    }

    // BNT-M03: consecutive withColumn chain > 3
    fn check_consecutive_with_column(&mut self, node: &ExprCall, method: &str) {
        if method == "withColumn" {
            self.consecutive_with_column += 1;
            if self.consecutive_with_column == 4 {
                self.findings.push(Finding::warning("consecutive_with_column_chain",
                    self.current_line,
                    "More than 3 consecutive .withColumn() calls cause O(n²) Catalyst plan analysis",
                    "Use .withColumns({...}) (Spark 3.3+) or a single .select() instead"));
            }
        } else {
            self.consecutive_with_column = 0;
        }
    }
}
```

**`RuleTable` — zero-cost disabled rules**

```rust
pub struct RuleTable {
    enabled: HashSet<String>,
}

impl RuleTable {
    pub fn all() -> Self {
        Self { enabled: REGISTRY.keys().cloned().collect() }
    }
    pub fn from_select(select: &[String], ignore: &[String]) -> Self {
        let mut enabled: HashSet<String> = if select == ["ALL"] {
            REGISTRY.keys().cloned().collect()
        } else {
            select.iter().cloned().collect()
        };
        for id in ignore { enabled.remove(id); }
        Self { enabled }
    }
    pub fn is_enabled(&self, rule_id: &str) -> bool {
        self.enabled.contains(rule_id)
    }
}
```

**`analyze_file` and `analyze_directory` — the PyO3 exports**

```rust
// src/burnt_rs/src/lib.rs

#[pyfunction]
pub fn analyze_file(
    py: Python<'_>,
    path: &str,
    select: Vec<String>,
    ignore: Vec<String>,
) -> PyResult<Vec<PyObject>> {
    let path = PathBuf::from(path);
    let cells = notebook::parse_file(&path);
    let rules = RuleTable::from_select(&select, &ignore);
    let hybrid = hybrid::build(cells);
    let findings = checker::HybridChecker::run(hybrid, &rules);
    findings.iter().map(|f| f.to_pydict(py)).collect()
}

#[pyfunction]
pub fn analyze_directory(
    py: Python<'_>,
    root: &str,
    extensions: Vec<String>,
    select: Vec<String>,
    ignore: Vec<String>,
) -> PyResult<Vec<PyObject>> {
    let paths = scanner::scan_directory(root, &extensions);  // Rayon parallel from s6-11
    let rules = Arc::new(RuleTable::from_select(&select, &ignore));

    // Analyze all files in parallel — no GIL held during analysis
    let all_findings: Vec<Finding> = paths
        .par_iter()
        .flat_map(|p| {
            let cells = notebook::parse_file(p);
            let hybrid = hybrid::build(cells);
            checker::HybridChecker::run(hybrid, &rules)
        })
        .collect();

    // GIL acquired once for conversion
    all_findings.iter().map(|f| f.to_pydict(py)).collect()
}
```

**`main.py` after this task — the full CLI becomes a 50-line shell**

```python
# src/burnt/cli/main.py — simplified
import typer
import burnt_rs   # the entire analysis is here
from rich.console import Console
from rich.table import Table

@app.command()
def check(path: str, fail_on: str = "error", output: str = "table",
          ignore_rule: list[str] = []) -> None:
    """Check SQL/PySpark/notebook files for cost anti-patterns."""
    _config_path, settings = Settings.discover()
    effective_ignore = list(set(settings.lint.ignore) | set(ignore_rule))

    target = Path(path)
    if not target.exists():
        console.print(f"[red]Error:[/red] Path not found: {path}")
        raise typer.Exit(1)

    if target.is_file():
        findings = burnt_rs.analyze_file(str(target), ["ALL"], effective_ignore)
    else:
        findings = burnt_rs.analyze_directory(
            str(target), [".py", ".sql", ".ipynb"], ["ALL"], effective_ignore
        )

    _render(findings, output, fail_on)
```

**`Finding` → Python dict schema**

```rust
impl Finding {
    pub fn to_pydict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let d = pyo3::types::PyDict::new(py);
        d.set_item("rule_id",     &self.rule_id)?;
        d.set_item("severity",    self.severity.as_str())?;
        d.set_item("line",        self.line)?;
        d.set_item("description", &self.description)?;
        d.set_item("suggestion",  &self.suggestion)?;
        d.set_item("file",        self.file.to_string_lossy().as_ref())?;
        Ok(d.into())
    }
}
```

**`AntiPattern` compatibility shim**

The existing Python `AntiPattern` dataclass and `detect_antipatterns()` function in `antipatterns.py` become a thin adapter so existing tests keep passing during migration:

```python
# antipatterns.py — compat shim
def detect_antipatterns(source: str, language: str = "sql") -> list[AntiPattern]:
    """Compat shim: write source to temp file, call Rust, convert findings."""
    import tempfile, burnt_rs
    suffix = ".py" if language == "pyspark" else ".sql"
    with tempfile.NamedTemporaryFile(suffix=suffix, mode="w", delete=False) as f:
        f.write(source)
        tmp = f.name
    raw = burnt_rs.analyze_file(tmp, ["ALL"], [])
    return [AntiPattern(
        name=r["rule_id"],
        severity=Severity(r["severity"]),
        description=r["description"],
        suggestion=r["suggestion"],
        line_number=r["line"],
    ) for r in raw]
```

This shim means the 451 existing unit tests pass unchanged while the analysis engine is fully Rust.

---

## Acceptance Criteria

- [ ] `burnt_rs.analyze_file(path, ["ALL"], [])` returns identical findings to the Python implementation on all existing test fixtures
- [ ] `burnt_rs.analyze_directory(root, exts, rules, ignore)` uses Rayon; all files analysed in parallel without GIL held during analysis
- [ ] `main.py` reduced to CLI shell + output formatting; no analysis logic remains in Python
- [ ] `detect_antipatterns()` compat shim passes all 451 existing unit tests
- [ ] Syntax errors in Python cells → partial findings returned, no panic
- [ ] Malformed SQL in `spark.sql()` → partial findings, no panic
- [ ] `maturin build --release` produces wheel; `pip install` in clean venv; tests pass
- [ ] `hyperfine` shows ≥5× improvement on 100+ file corpus vs Python-only baseline
- [ ] `uv run ruff check src/ tests/` clean on Python side
- [ ] `cargo clippy` clean on Rust side

---

## Verification

```bash
cd src/burnt_rs && cargo clippy && cargo test
maturin develop --release

# Parity check
python3 -c "
import burnt_rs
from burnt.parsers.antipatterns import detect_antipatterns
from pathlib import Path

for fixture in Path('tests/fixtures/pyspark').glob('*.py'):
    rust = {f['rule_id'] for f in burnt_rs.analyze_file(str(fixture), ['ALL'], [])}
    py   = {p.name for p in detect_antipatterns(fixture.read_text(), 'pyspark')}
    if rust != py:
        print(f'MISMATCH {fixture.name}: rust={rust-py} py={py-rust}')
    else:
        print(f'OK {fixture.name}')
"

# Regression tests
uv run pytest -m unit -v

# Benchmark
hyperfine --warmup 3 \
  'python -m burnt check tests/fixtures/' \
  'BURNT_RUST=1 python -m burnt check tests/fixtures/'
```

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Requires s7-03 (full `HybridAST` + `HybridChecker` structure defined in Rust).
