# Task: Advisory → Simulation Integration (Architecture)

---

## Metadata

```yaml
id: s2-13-advisory-simulation-integration
status: todo
phase: 2
priority: high
agent: ~
blocked_by: [s2-05a-cli-api-redesign, s2-09-cluster-config-enrichment]
created_by: planner
```

---

## Context

### Goal

`AdvisoryReport.simulate()` is currently stubbed as `NotImplementedError`. The obvious fix would reconstruct a `CostEstimate` from advisory data and pass that to the `Simulation` builder — but this is the wrong approach: it discards the richest data context in the entire package. The advisory holds actual cluster config, real execution metrics (peak memory/CPU, spill bytes), and a `WorkloadProfile`. The simulation builder should consume this directly so that scenario calculations use real observed metrics instead of generic multiplier heuristics.

### Files to read

```
# Required
src/burnt/advisor/report.py        ← AdvisoryReport model + simulate() stub
src/burnt/advisor/session.py       ← advise_current_session() / advise() — builds AdvisoryReport
src/burnt/estimators/whatif.py     ← WhatIfBuilder / Simulation builder (pre-rename)
src/burnt/core/models.py           ← CostEstimate, ClusterConfig, WorkloadProfile

# Reference
DESIGN.md
tasks/s2-05a-cli-api-redesign.md   ← Simulation rename, new Simulation class signature
tasks/s2-12-cluster-profile.md     ← ClusterProfile
```

### Background

**Current state:** `AdvisoryReport.simulate()` raises `NotImplementedError`. The `Simulation` builder only accepts a `CostEstimate`.

**Proposed architecture:** Update `Simulation.__init__` to accept optional `profile` and `metrics` parameters:

```python
class Simulation:
    def __init__(
        self,
        estimate: CostEstimate,
        cluster: ClusterConfig | None = None,
        profile: WorkloadProfile | None = None,   # from advisory
        metrics: dict | None = None,               # raw SparkSession metrics
    ):
        ...
```

When `profile` is present, scenario calculations use real metrics instead of generic multipliers:

| Simulation method | Without profile (heuristic) | With profile (real metrics) |
|---|---|---|
| `enable_photon()` | Default 2.5× DBU, assumes "complex_join" | Photon score from actual query mix |
| `to_instance("DS3_v2")` | Generic memory check | Spill risk from actual peak memory vs target instance memory |
| `to_serverless()` | DBU heuristic | TCO from actual run duration × serverless rate |
| `set_workers(n)` | Linear scaling assumption | Validates against actual task parallelism |
| `use_spot()` | Fixed discount | Adjusted for actual job duration (shorter = lower interruption risk) |

**`AdvisoryReport.simulate()` implementation:**

```python
def simulate(self) -> Simulation:
    from burnt.estimators.simulation import Simulation
    return Simulation(
        estimate=self.baseline_estimate,
        cluster=self.current_cluster,
        profile=self.workload_profile,    # real metrics, not reverse-engineered
        metrics=self.raw_metrics,
    )
```

The standalone `estimate(sql).simulate()` path continues to use heuristic multipliers (no profile available). The advisory path becomes the high-fidelity entry point.

---

## Acceptance Criteria

- [ ] `Simulation.__init__` accepts `profile: WorkloadProfile | None` and `metrics: dict | None`
- [ ] `AdvisoryReport.simulate()` no longer raises `NotImplementedError`
- [ ] `AdvisoryReport.simulate()` returns a `Simulation` instance with `profile` and `cluster` populated from advisory data
- [ ] `enable_photon()` uses actual query mix from `profile` when available (not generic "complex_join" default)
- [ ] `to_instance()` computes spill risk against actual peak memory when `metrics` present
- [ ] `to_serverless()` uses actual run duration from `metrics` when available
- [ ] `use_spot()` adjusts discount rate based on actual job duration when `metrics` present
- [ ] Standalone `estimate(sql).simulate()` path unchanged — heuristic multipliers still apply
- [ ] `advice.simulate().cluster().enable_photon().compare()` works end-to-end without error
- [ ] Unit tests cover: advisory→simulation path, profile-based multiplier selection, fallback to heuristics when no profile
- [ ] All existing tests still pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

### Commands

```bash
uv run pytest -m unit -v -k "advisory_simulation or simulate"
uv run ruff check src/ tests/
```

### Integration Check

- [ ] In a connected notebook or with mock metrics:
  ```python
  advice = burnt.advise_current_session()
  result = advice.simulate().cluster().enable_photon().compare()
  result.display()
  # Should show photon scenario using actual query-mix data, not generic heuristic
  ```

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked on s2-05a (Simulation class rename/refactor must be done first) and s2-12 (ClusterProfile for extended cluster data).
