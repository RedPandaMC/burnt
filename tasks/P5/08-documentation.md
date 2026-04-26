status: todo
agent: executor
completed_by: moonshotai/kimi-k2.6

## Redesign Notes
Documentation needs to reflect the new architecture:
- README.md: Spark-first positioning, `pip install burnt` vs `burnt[databricks]`
- DESIGN.md: Already updated (done)
- AGENTS.md: Update dev workflow notes
- CHANGELOG.md: Document v0.2.0 changes

## Remaining Work
- Rewrite README.md for new product vision (performance coach, not crystal ball)
- Document `burnt.start_session()` → `burnt.check()` workflow
- Document optional Databricks module
- Update installation instructions
