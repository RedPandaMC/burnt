status: todo
agent: executor
completed_by: moonshotai/kimi-k2.6

## Redesign Notes
E2E tests should cover the new hybrid analysis flow:
1. Static-only analysis (no Spark, no Databricks)
2. Static + runtime listener (Spark session active)
3. Static + Databricks backend (optional extra installed)

Remove DLT fixture from core E2E tests — DLT is Databricks-specific.
Add fixtures for:
- Python notebook with expensive patterns (collect, crossJoin, repartition)
- SQL file with anti-patterns (SELECT *, missing WHERE)
- Empty/minimal files (edge cases)

## Remaining Work
- Create fixtures in `tests/fixtures/e2e/`
- Write E2E tests that verify full `burnt.check()` pipeline
- Test both static-only and spark-integrated paths
