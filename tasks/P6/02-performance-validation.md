status: todo
agent: executor
completed_by: moonshotai/kimi-k2.6

## Redesign Notes
Performance targets updated for new architecture:
- **Static analysis latency**: < 3s for 50-cell notebook (Rust engine)
- **Memory overhead**: < 50 MB (mostly Rust engine footprint)
- **Listener overhead**: < 5% CPU, negligible memory

## Remaining Work
- Profile `burnt.check()` with large notebooks (static-only)
- Measure listener overhead in running Spark sessions
- Optimize if targets are exceeded
