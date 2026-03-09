# Task: advise_current_session() + advise CLI + AdvisoryReport

---

## Metadata

```yaml
id: s1-03-advisor
status: todo
sprint: 1
priority: critical
agent: ~
blocked_by: [s1-01-runtime-backend, s1-02-instance-catalog]
created_by: planner
```

---

## Context

### Goal

Implement the **flagship feature**: `dburnrate.advise_current_session()`. This is the "Developer's Best Friend" — a data engineer finishes testing a notebook on All-Purpose compute, runs one cell, and gets a production cluster recommendation with cost savings and Databricks API JSON.

Also implement `dburnrate.advise(run_id=)` for the CLI workflow and the `dburnrate advise` CLI command.

### Files to Read

```
src/dburnrate/__init__.py              # Current stub (raises NotImplementedError)
src/dburnrate/estimators/whatif.py     # Existing scenario functions
src/dburnrate/tables/queries.py        # Query history + fingerprinting
src/dburnrate/cli/main.py             # Current CLI commands
docs/cli-workflows.md                  # Target UX for advise output
docs/programmatic-workflows.md         # Target programmatic UX
DESIGN.md § "Workflow 1"               # End-of-notebook advisor spec
```

### Files to Create

```
src/dburnrate/advisor/__init__.py
src/dburnrate/advisor/session.py       # advise_current_session() + advise(run_id=)
src/dburnrate/advisor/report.py        # AdvisoryReport model + display rendering
tests/unit/test_advisor.py
```

### Files to Modify

```
src/dburnrate/__init__.py              # Replace stub with real implementation
src/dburnrate/cli/main.py             # Add `advise` command
```

---

## Specification

### AdvisoryReport Model (`advisor/report.py`)

```python
class ComputeScenario(BaseModel):
    compute_type: str                   # "All-Purpose", "Jobs Compute", "Serverless"
    sku: str                            # "ALL_PURPOSE", "JOBS_COMPUTE", "SERVERLESS"
    estimated_cost_usd: float
    savings_pct: float                  # vs baseline (negative = cheaper)
    tradeoff: str                       # e.g., "Recommended", "Fastest cold start"

class AdvisoryReport(BaseModel):
    baseline: ComputeScenario           # The current run (All-Purpose)
    scenarios: list[ComputeScenario]    # Jobs Compute, Serverless, etc.
    recommended: ClusterConfig          # Best cluster config with to_api_json()
    recommendation: ClusterRecommendation  # economy/balanced/performance tiers
    insights: list[str]                 # e.g., "Peak memory 14%, downsize DS4→DS3"
    run_metrics: dict[str, Any]         # Raw metrics from the analyzed run

    def display(self) -> None:
        """Render rich table. Uses displayHTML() in Databricks, rich.Table otherwise."""

    def comparison_table(self) -> str:
        """ASCII table matching docs/cli-workflows.md Compute Migration Analysis format."""

    def what_if(self) -> WhatIfBuilder:
        """Chain into what-if scenarios from this advice."""
```

### Session Advisor (`advisor/session.py`)

```python
def advise_current_session(backend: Backend | None = None) -> AdvisoryReport:
    """
    Analyze queries executed in the current SparkSession.

    1. Detect backend via auto_backend() if not provided
    2. Get session metrics: peak memory, disk spill, total duration, read_bytes
    3. Identify the current cluster config (instance type, workers)
    4. Calculate All-Purpose baseline cost
    5. Project onto Jobs Compute, Serverless, Spot scenarios
    6. Run right_size() to recommend optimal cluster
    7. Build AdvisoryReport
    """

def advise(
    run_id: str | None = None,
    statement_id: str | None = None,
    backend: Backend | None = None,
) -> AdvisoryReport:
    """
    Advise for a specific historical run via system tables.

    1. Fetch metrics from system.query.history or system.lakeflow.job_run_timeline
    2. Extract: duration, read_bytes, spill_to_disk, peak memory
    3. Same projection logic as advise_current_session()
    """
```

### CLI Command

```bash
# Advise for a specific run (external or in-cluster)
dburnrate advise --run-id 1234567890

# Advise for current notebook (in-cluster only)
dburnrate advise --self

# JSON output for CI/CD
dburnrate advise --run-id 1234567890 --output json
```

### Display Format (from `docs/cli-workflows.md`)

```
  Compute Migration Analysis
┌──────────────────┬───────────┬──────────┬────────────────────┐
│ Compute Type     │ Est. Cost │ Savings  │ Tradeoff           │
├──────────────────┼───────────┼──────────┤────────────────────┤
│ All-Purpose      │ $45.12    │ baseline │ (Your test run)    │
│ Jobs Compute     │ $18.25    │ -60%     │ Recommended        │
│ SQL Serverless   │ $28.50    │ -37%     │ Fastest cold start │
└──────────────────┴───────────┴──────────┴────────────────────┘
💡 Peak memory utilization was 14%. Downsizing from DS4_v2 to DS3_v2 saves an additional 50%.

Recommended Cluster (paste into Job definition):
{
  "new_cluster": {
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 3,
    ...
  }
}
```

---

## Acceptance Criteria

- [ ] `dburnrate.advise_current_session()` works inside a mocked Databricks notebook context
- [ ] `dburnrate.advise(run_id="...")` fetches metrics from system tables and returns AdvisoryReport
- [ ] AdvisoryReport includes: baseline cost, Jobs/Serverless/Spot scenarios with savings %
- [ ] AdvisoryReport includes: cluster recommendation with `to_api_json()` output
- [ ] `advice.display()` renders the Compute Migration Analysis table (rich for CLI, displayHTML for notebook)
- [ ] `advice.what_if()` chains into `WhatIfBuilder` (can be stub until s2-01)
- [ ] CLI `dburnrate advise --run-id X` prints the table and JSON recommendation
- [ ] CLI `dburnrate advise --self` detects current notebook path and advises
- [ ] Graceful error when called outside Databricks: "advise_current_session() requires a Databricks runtime"
- [ ] `__init__.py` exports: `advise`, `advise_current_session`, `right_size`
- [ ] Unit tests cover: report generation, display rendering, CLI integration, error cases

---

## Verification

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/

# Integration check (mocked)
uv run dburnrate advise --run-id TEST12345  # Should print table with mocked data
```

---

## Handoff

```yaml
status: todo
```
