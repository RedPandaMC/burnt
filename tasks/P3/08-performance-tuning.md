```yaml
id: P3-08-performance-tuning
status: todo
phase: 3
priority: low
agent: ~
blocked_by: [P3-06-cli-implementation]
created_by: planner
```

## Context

### Goal

Measure and validate that `burnt check` meets its latency and memory targets with real notebooks. Add a benchmark script that can be run in CI to catch regressions.

### Files to read

```
# Required
src/burnt/_check/__init__.py
src/burnt-engine/src/lib.rs      (analyze_file, analyze_directory)

# Reference
DESIGN.md §1 (Metrics table)
```

### Background

Targets from DESIGN.md:
- Static analysis latency: < 3s for a 50-cell notebook
- Cold import + first check: < 1s
- Driver memory overhead: < 50MB RSS
- sparkMeasure listener overhead: < 5% CPU (measured separately, not in this task)

---

## Acceptance Criteria

- [ ] Benchmark script exists at `tests/bench/bench_check.py` — runnable with `uv run python tests/bench/bench_check.py`
- [ ] Script reports: cold-start time, per-notebook analysis time, peak RSS memory
- [ ] Static analysis of a 50-cell `.ipynb` fixture completes in < 3s (median over 5 runs)
- [ ] `import burnt` + `burnt.check("x.py")` cold start < 1s
- [ ] Peak RSS for static-only analysis < 50MB (measured via `tracemalloc` or `resource`)
- [ ] Script exits non-zero if any target is missed, so it can run as a CI step
- [ ] Benchmark notebooks (at least 3, ranging 10–100 cells) stored in `tests/bench/notebooks/`

## Verification

```bash
uv run python tests/bench/bench_check.py
# Expected output:
# cold_start_ms: 342     OK (< 1000)
# notebook_50cell_ms: 1847   OK (< 3000)
# peak_rss_mb: 38.2      OK (< 50)
```

### Integration Check

- [ ] Add benchmark step to `.azure/pipelines.yml` in the Build stage (non-blocking, publish results as artifact)
