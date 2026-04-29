status: done
agent: executor
completed_by: moonshotai/kimi-k2.6

## Implementation
### Changes Made
- `src/burnt/_check/__init__.py` - `run()` now orchestrates:
  1. Static analysis via Rust engine (`_engine.analyze_file` / `analyze_source`)
  2. Runtime metric merge from session listener (if active)
  3. Severity / skip / only filtering
  4. Returns `CheckResult` with findings, graph, and compute_seconds

### Implementation Notes
- No more "access levels" — just static analysis + optional runtime enrichment
- If no session is active, check() runs pure static analysis (fast, always works)
- If session is active, findings are tagged with actual compute_seconds from SparkListener

### Verification Results
- Tests: 300 passed
- Lint: pass
