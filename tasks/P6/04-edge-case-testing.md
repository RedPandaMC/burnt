status: todo
agent: executor
completed_by: moonshotai/kimi-k2.6

## Redesign Notes
Edge cases remain mostly the same, but DLT-specific cases should be tested in `burnt[databricks]` context:

Core edge cases:
- Empty notebook / file
- 100+ cells
- Every cell syntax error
- `.ipynb` markdown only
- Mixed Python + SQL in single notebook
- Dynamic SQL (f-strings)

Databricks-specific (test with extra installed):
- Nested `%run`
- Circular `%run`
- DLT + non-DLT mixed notebook

## Remaining Work
- Write tests for all core edge cases
- Separate Databricks-specific edge case tests
