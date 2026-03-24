status: todo
agent:
completed_by:

## Implementation
### Changes Made
- String variable resolution: `table = "c.s.t"` + `spark.sql(f"SELECT FROM {table}")` → resolve.
- Widget defaults: `dbutils.widgets.text("t", "default")` → use.
- Unresolvable: BN002 + partial graph.

### Implementation Notes
- Support dynamic table names and SQL statements where variables can be resolved statically.
- Provide clear findings for unresolvable dynamic SQL fragments.

### Verification Results
- Tests: `pytest` pass
- Lint: `ruff check` pass
