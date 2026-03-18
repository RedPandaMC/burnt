# Task s7-05: PyO3 Bridge, CLI & JSON Output

## Metadata

```yaml
id: s7-05-pyo3-cli
status: todo
phase: 7
priority: high
agent: ~
blocked_by: [s7-04-rules]
created_by: planner
```

## Goal

Wire the Rust engine into Python via PyO3 with three entry points, reduce `main.py` to a thin CLI shell, implement the in-memory compat shim, and deliver JSON output with GitHub Actions annotations.

## Background

### Three PyO3 entry points

```rust
#[pyfunction]
pub fn analyze_file(py: Python, path: &str, select: Vec<String>, ignore: Vec<String>) -> PyResult<Vec<PyObject>> {
    // Calls parse_and_resolve (which handles %run inline resolution)
    let (cells, mut diagnostics) = ingestion::parse_and_resolve(&PathBuf::from(path), &root);
    let rules = RuleTable::from_select_ignore_strings(&select, &ignore);
    let mut findings = checker::analyze_cells(cells, &rules);
    findings.extend(diagnostics);  // BN001, BN003 from %run resolution
    findings.iter().map(|f| finding_to_pydict(py, f)).collect()
}

#[pyfunction]
pub fn analyze_source(py: Python, source: &str, filename: &str, select: Vec<String>, ignore: Vec<String>) -> PyResult<Vec<PyObject>> {
    // In-memory, no disk I/O, no %run resolution (single source string)
    let cells = ingestion::parse_file_from_source(source, &PathBuf::from(filename));
    let rules = RuleTable::from_select_ignore_strings(&select, &ignore);
    let findings = checker::analyze_cells(cells, &rules);
    findings.iter().map(|f| finding_to_pydict(py, f)).collect()
}

#[pyfunction]
pub fn analyze_directory(py: Python, root: &str, extensions: Vec<String>, select: Vec<String>, ignore: Vec<String>) -> PyResult<Vec<PyObject>> {
    let paths = scanner::scan_directory(root, &extensions);
    let rules = Arc::new(RuleTable::from_select_ignore_strings(&select, &ignore));
    // Release GIL, analyze in parallel with rayon
    let findings: Vec<Finding> = py.allow_threads(|| {
        paths.par_iter().flat_map(|p| {
            let (cells, diags) = ingestion::parse_and_resolve(p, &PathBuf::from(root));
            let mut f = checker::analyze_cells(cells, &rules);
            f.extend(diags);
            f
        }).collect()
    });
    findings.iter().map(|f| finding_to_pydict(py, f)).collect()
}
```

Note: `analyze_directory` calls `parse_and_resolve` per file, which handles `%run` resolution per notebook. Files referenced by `%run` from multiple notebooks are parsed multiple times (each in the context of their caller). This is correct — the analysis context differs per caller.

### Compat shim — zero temp files

```python
def detect_antipatterns(source: str, language: str = "sql") -> list[AntiPattern]:
    import burnt_rs
    filename = "source.py" if language == "pyspark" else "source.sql"
    raw = burnt_rs.analyze_source(source, filename, ["ALL"], [])
    return [AntiPattern(name=r["rule_id"], severity=Severity(r["severity"]),
            description=r["description"], suggestion=r["suggestion"],
            line_number=r.get("line")) for r in raw]
```

### JSON output

```python
def _print_json(findings, target):
    result = {
        "burnt_version": importlib.metadata.version("burnt"),
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "files_checked": len({f["file"] for f in findings}),
            "total_violations": len(findings),
            "errors": sum(1 for f in findings if f["severity"] == "error"),
            "warnings": sum(1 for f in findings if f["severity"] == "warning"),
            "info": sum(1 for f in findings if f["severity"] == "info"),
            "style": sum(1 for f in findings if f["severity"] == "style"),
        },
        "violations": findings,
    }
    print(json.dumps(result, indent=2))
```

### GitHub Actions annotations

```python
def _emit_github_annotations(findings):
    if not (os.getenv("CI") and os.getenv("GITHUB_ACTIONS")): return
    for f in findings:
        level = f["severity"] if f["severity"] in ("error", "warning") else "notice"
        print(f'::{level} file={f["file"]},line={f.get("line",1)},col={f.get("column",1)},'
              f'title={f.get("code","")} {f["rule_id"]}::{f["description"]}', file=sys.stderr)
```

## Acceptance Criteria

- [ ] `analyze_file` handles `%run` resolution automatically via `parse_and_resolve`
- [ ] `analyze_source` works in-memory — no temp files, no disk I/O
- [ ] `analyze_directory` releases GIL, uses rayon, resolves `%run` per notebook
- [ ] Compat shim calls `analyze_source` (not `analyze_file` with temp file)
- [ ] `--output json` → valid JSON with summary + violations
- [ ] `--output table` and `--output text` still work
- [ ] GitHub Actions annotations emitted when `CI=true && GITHUB_ACTIONS=true`
- [ ] Exit code controlled by `--fail-on`, not `--output`
- [ ] `main.py` has no analysis logic — CLI shell only
- [ ] `maturin build --release` produces wheel
