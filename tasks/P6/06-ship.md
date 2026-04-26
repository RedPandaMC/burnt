status: todo
agent: executor
completed_by: moonshotai/kimi-k2.6

## Redesign Notes
Ship checklist updated for new architecture:
- [ ] All tests pass (`cargo test && pytest`)
- [ ] Lint passes (`ruff check`)
- [ ] Security audit clean
- [ ] Documentation updated (README, DESIGN, AGENTS)
- [ ] Wheels build for Linux + macOS
- [ ] `pip install burnt` works in clean environment
- [ ] `pip install burnt[databricks]` adds Databricks features

## Remaining Work
- Execute final release process after all P5/P6 tasks complete
