# Phase 6: Validation

> Dogfood. Security. Performance. Edge cases. Ship.

**Duration:** 1 week
**Depends on:** Phase 5
**Gate:** 5+ real notebooks, 2+ workspaces. < 3s. < 50 MB. Zero HIGH security. All edge cases. v2.0.0 tagged.

---

## Tasks

### P6-01: Dogfood (Week 19)

5+ real notebooks across 2+ workspaces: Python with %run, pure SQL, DLT, heavy dynamic SQL, 50+ cells. Document accuracy vs actual billing, false positive rate, usability.

### P6-02: Performance (Week 19)

Real-notebook latency. Memory profiling. Fix > 3s or > 50 MB.

### P6-03: Security (Week 19)

`cargo audit`, `cargo deny`, `pip-audit`, `bandit`. Zero HIGH/CRITICAL.

### P6-04: Edge Cases (Week 19)

| Input | Expected |
|-------|----------|
| Empty notebook | Empty graph, no crash |
| Only `%md` cells | Empty graph |
| 100+ cells | < 5 seconds |
| 5-level nested `%run` | Resolves, no overflow |
| 3-way circular `%run` | BN003, no loop |
| Every cell syntax error | Findings, partial trees |
| `.ipynb` only markdown | Empty graph |
| Mixed DLT + non-DLT | Mode = DLT |
| SQL + one Python comment | Mode = Python |
| `import dlt` no `@dlt.table` | Mode = DLT |
| Triple-quoted SQL in spark.sql | Parsed correctly |
| Backtick table names | Parsed correctly |
| `%run` plain .py (not notebook) | Single cell, inlined |
| Invalid `burnt.toml` | Clear error, no crash |
| `pyproject.toml` with `[tool.burnt]` | Config loaded correctly |
| `pyproject.toml` without `[tool.burnt]` | Ignored, search continues |
| Both `burnt.toml` and `pyproject.toml` `[tool.burnt]` | `burnt.toml` wins (found first) |

### P6-05: Version Pins (Week 19)

`Cargo.lock` committed. Python upper bounds. Test on DBR 14.3 LTS + 15.x.

### P6-06: Ship (Week 19)

Clean checkout. `cargo test && pytest`. `ruff check`. `cargo audit`. Tag `v2.0.0`. Build wheels. Publish.

---

## Gate

- [ ] 5+ real notebooks, useful output
- [ ] Accuracy within targets
- [ ] False positive rate < 20%
- [ ] < 3s, < 50 MB
- [ ] Zero HIGH security
- [ ] All edge cases handled
- [ ] DBR 14.3 + 15.x compatible
- [ ] Config system works on real workspaces
- [ ] v2.0.0 tagged, wheels built
