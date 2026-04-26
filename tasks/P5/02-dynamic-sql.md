status: todo
agent: executor
completed_by: moonshotai/kimi-k2.6

## Redesign Notes
Dynamic SQL resolution remains valid for generic Spark:
- `spark.sql(f"SELECT FROM {table}")` should be flagged
- Variable resolution: simple cases where the value is defined earlier in the notebook
- Unresolvable: finding with clear message

This is primarily a Rust engine task, but the Python layer should surface the findings correctly.

## Remaining Work
- Verify Rust engine flags dynamic SQL patterns
- Ensure `_check.py` surfaces BN002 (or equivalent) for unresolvable dynamic SQL
