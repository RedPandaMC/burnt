```yaml
id: P2-01b-subtree-removal
status: done
phase: 2
priority: critical
agent: ~
blocked_by: [P2-07]
created_by: planner
```

## Context

### Goal

Delete all pre-pivot module subtrees that are dead code after the April 2026 pivot.
No relocation — delete outright. Public API cleanup is handled in P2-07 first.

### Paths to delete

```
src/burnt/watch/            # 7 files: workspace monitoring
src/burnt/_watch/           # 1 file: thin wrapper
src/burnt/alerts/           # 2 files: Slack/Teams/webhook dispatch
src/burnt/intelligence/recommend.py   # NotImplementedError stub
src/burnt/intelligence/feedback.py    # NotImplementedError stub
src/burnt/tables/attribution.py       # off-mission per modular-architecture §3.5
templates/burnt_monitor.py            # references deleted watch() + [alerts] extra
run_ts.rs                             # stray tree-sitter test at repo root
tests/unit/watch/                     # tests for deleted watch module
```

After deletion: clean `src/burnt/intelligence/__init__.py` to remove imports of
`recommend` and `feedback`. If nothing remains, delete the `__init__.py` too.
(Note: `intelligence/session.py` was already moved to `core/session_cost.py`.)

---

## Acceptance Criteria

- [ ] None of the above paths exist in the repo
- [ ] `src/burnt/intelligence/` directory does not exist (was emptied by move + deletes)
- [ ] No import of `burnt.watch`, `burnt._watch`, `burnt.alerts`, `burnt.intelligence` anywhere in `src/` or `tests/`
- [ ] `uv run ruff check src/ tests/` passes
- [ ] `uv run pytest -m unit -v` passes (watch tests are gone)

## Verification

```bash
# No dead imports remain
grep -r "from burnt\.watch\|from burnt\.alerts\|from burnt\.intelligence\|from burnt\._watch" src/ tests/
# → must return nothing

uv run ruff check src/ tests/
uv run pytest -m unit -v
```
