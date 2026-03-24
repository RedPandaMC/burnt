status: done
agent: opencode
completed_by: opencode

## Implementation
### Changes Made
- Removed tests for deleted modules:
  - tests/unit/estimators/ (all removed)
  - tests/unit/test_advisor.py (removed)
  - tests/benchmarks/ (removed)
  - tests/unit/parsers/test_antipatterns.py (removed)
  - tests/unit/parsers/test_new_rules.py (removed)
- Removed tests for old v1.0 API:
  - tests/unit/test_api.py (removed)
  - tests/unit/core/test_cluster_config_factory.py (removed)
  - tests/unit/parsers/test_sql.py (removed)
- Preserved 262 working tests in tables/, core/, parsers/, runtime/ directories

### Implementation Notes
- Backward compatibility exports added to __init__.py (CostBudgetExceeded, CostEstimate, etc.)
- Tests for core/models.py work with preserved models
- Tables and parsers tests preserved for future phases

### Verification Results
- Tests: 262 passed
- Lint: pass
