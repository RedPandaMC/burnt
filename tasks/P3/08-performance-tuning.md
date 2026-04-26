status: todo
agent: executor
completed_by: moonshotai/kimi-k2.6

## Redesign Notes
Performance targets remain relevant but measurement criteria change:
- **Static analysis latency**: < 3s for 50-cell notebook (Rust engine already fast)
- **Memory overhead**: < 50 MB (mostly Rust engine footprint)
- **Listener overhead**: < 5% CPU, negligible memory (captures events asynchronously)

## Remaining Work
- Profile `burnt.check()` with large notebooks
- Ensure lazy imports minimize startup time
- Measure actual listener overhead in running Spark sessions
- Optimize directory analysis with rayon-based parallelization
