```yaml
id: P6-00-onprem-spark-backend
status: todo
phase: 6
priority: high
agent: ~
blocked_by: [P3-02, P3-05]
created_by: planner
```

## Context

### Goal

Implement the `[onprem-spark]` `PricingBackend` — converts compute-seconds to USD using
user-supplied rates in `burnt.toml`. No cloud SDKs, no network calls: pure config +
arithmetic.

### Background

`[onprem-spark]` has zero additional Python dependencies (`onprem-spark = []` in
`pyproject.toml`). The pricing logic reads three rates from `burnt.toml` and multiplies
by the stage metrics the REST session client collected.

**Config schema (from P3-05):**
```toml
[burnt.onprem_spark]
cost_per_vcpu_hour  = 0.048   # required
cost_per_gb_hour    = 0.006   # optional (memory cost)
cost_per_gb_shuffle = 0.001   # optional (shuffle cost)
```

**Cost formula:**
```
cpu_cost     = executor_run_time_hours * num_vcpus * cost_per_vcpu_hour
memory_cost  = executor_memory_gb * executor_run_time_hours * cost_per_gb_hour
shuffle_cost = total_shuffle_gb * cost_per_gb_shuffle
total        = cpu_cost + memory_cost + shuffle_cost
```

### Files to modify

```
src/burnt/cloud/onprem_spark/backend.py   (new — PricingBackend impl)
src/burnt/cloud/onprem_spark/__init__.py  (new)
src/burnt/_config/__init__.py             (add onprem_spark rate fields — done in P3-05)
tests/unit/test_onprem_backend.py         (new)
```

---

## Acceptance Criteria

- [ ] `OnPremSparkBackend` implements the `PricingBackend` protocol from P3-02
- [ ] `OnPremSparkBackend.estimate(session_state, config)` returns a `CostEstimate`
  with `usd`, `backend = "onprem-spark"`, and `confidence = "medium"`
- [ ] If any required rate is missing from config → raises `ConfigError` with field name
- [ ] `pip install burnt[onprem-spark]` installs no additional packages beyond `burnt` core
- [ ] Unit tests: correct arithmetic for known inputs; ConfigError on missing rate
- [ ] `uv run pytest tests/unit/test_onprem_backend.py -v` passes

## Verification

```bash
uv run pytest tests/unit/test_onprem_backend.py -v
python -c "
import burnt
burnt.config(pricing_backend='onprem-spark')
result = burnt.check('tests/fixtures/e2e/cross_join.py')
print(result.cost_estimate)
"
```
