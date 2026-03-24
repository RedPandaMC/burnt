status: done
agent: opencode
completed_by: opencode

## Implementation
### Changes Made
- Created burnt-engine/ directory with Cargo.toml, build.rs, and Rust source stubs
- Created new src/burnt/ subdirectories per DESIGN.md §14:
  - graph/ (model.py, enrich.py, estimate.py, scaling.py)
  - intelligence/ (recommend.py, feedback.py, session.py)
  - watch/ (core.py, tags.py, idle.py, drift.py)
  - alerts/ (dispatch.py)
  - runtime/ (kept existing modules)
  - catalog/ (instances.py, pricing.py)
  - display/ (notebook.py, terminal.py, export.py)
- Updated pyproject.toml version to 0.2.0

### Implementation Notes
- Rust engine scaffolded with placeholder modules for parsing, rules, semantic analysis
- Python modules raise NotImplementedError - full implementation in later phases
- Used hatchling build backend (maturin deferred to future integration)

### Verification Results
- Tests: 262 passed
- Lint: pass
