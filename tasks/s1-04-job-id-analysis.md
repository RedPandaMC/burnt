# Task: Job ID Analysis for Multiple Runs

---

## Metadata

```yaml
id: s1-04-job-id-analysis
status: todo
sprint: 1
priority: medium
agent: ~
blocked_by: [s1-03-advisor]
created_by: executor
```

---

## Context

### Goal

Implement `burnt.advise(job_id=...)` to analyze multiple runs from a Databricks Job and provide a more accurate cluster recommendation based on aggregated metrics.

This is a follow-up task requested during s1-03-advisor implementation, where we added a stub that raises NotImplementedError with message: "advise(job_id=...) is not yet implemented. Use --run-id or --statement-id instead. Job ID analysis for multiple runs will be added in a future release."

### Files to Read

```
src/burnt/advisor/session.py       # Current stub for job_id parameter
src/burnt/tables/queries.py        # Query history functions
src/burnt/core/models.py           # Models for aggregated metrics
DESIGN.md § "System Tables Reference"  # system.lakeflow.job_run_timeline structure
```

### Files to Modify

```
src/burnt/advisor/session.py       # Implement job_id analysis logic
tests/unit/test_advisor.py         # Add tests for job_id analysis
```

---

## Specification

### Job Analysis Logic

```python
def analyze_job_metrics(job_id: str, backend: Backend) -> AggregatedMetrics:
    """
    Analyze multiple runs from a Databricks Job to get aggregated metrics.
    
    1. Fetch all runs for the job from system.lakeflow.job_run_timeline
    2. Aggregate metrics: average duration, peak memory, spill bytes, etc.
    3. Calculate variability metrics (std dev, min/max)
    4. Return AggregatedMetrics for better cluster sizing
    """
```

### AggregatedMetrics Model

```python
class AggregatedMetrics(BaseModel):
    """Aggregated metrics from multiple job runs."""
    
    job_id: str
    num_runs: int
    avg_duration_ms: float
    avg_peak_memory_pct: float
    avg_peak_cpu_pct: float
    max_spill_bytes: int  # Worst-case spill
    duration_variability_pct: float  # std dev / avg
    memory_variability_pct: float
    last_run_metrics: dict[str, Any]  # Most recent run for baseline
```

### Enhanced Cluster Right-Sizing

When analyzing job_id:
- Use **worst-case metrics** (max spill, peak memory) for safety
- Consider **variability** for autoscaling recommendations
- Provide **confidence boost** from multiple runs vs single run

### CLI Integration

```bash
# Analyze a job with multiple runs
burnt advise --job-id abc123

# Output should mention "Based on 5 runs" vs "Based on single run"
```

---

## Acceptance Criteria

- [ ] `burnt.advise(job_id="...")` fetches metrics from `system.lakeflow.job_run_timeline`
- [ ] Returns `AdvisoryReport` with aggregated metrics from multiple runs
- [ ] Includes variability analysis for autoscaling recommendations
- [ ] Uses worst-case metrics (max spill, peak memory) for safe sizing
- [ ] Provides confidence boost indicator: "High confidence from 5 runs"
- [ ] Graceful error when job not found or no runs available
- [ ] Unit tests cover: job metric aggregation, variability calculation, error cases
- [ ] Integration with existing `advise()` function structure

---

## Verification

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

---

## Handoff

```yaml
status: todo
```