status: todo
agent: executor
completed_by: moonshotai/kimi-k2.6

## Redesign Notes
Dogfooding now targets the new workflow:
1. `burnt.start_session()` → run Spark code → `burnt.check()` → review findings
2. Test across multiple environments: local pyspark, Databricks, EMR/Dataproc (if possible)
3. Focus on actionable advice quality, not dollar accuracy

Remove DLT-specific dogfooding from core — that belongs in `burnt[databricks]` testing.

## Remaining Work
- Run `burnt.check()` on 5+ real-world notebooks
- Verify actionable advice is specific and correct
- Document compute time estimates vs actual Spark metrics
