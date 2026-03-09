# CLI Workflows and CI/CD Integration

> Comprehensive guide for using burnt in command-line workflows and CI/CD pipelines.

---

## Table of Contents

1. [Interactive Advising](#interactive-advising)
2. [Cost-Aware Linting](#cost-aware-linting)
3. [Pre-Execution Estimation](#pre-execution-estimation)
4. [CI/CD Integration](#cicd-integration)
5. [Output Formats](#output-formats)

---

## Interactive Advising (The Translator)

The primary workflow for Data Engineers migrating code from interactive development to production orchestration.

If you ran a workload on an expensive All-Purpose cluster during development, `burnt` can analyze the actual Spark execution metrics and recommend an optimized Jobs cluster configuration.

```bash
# Retrieve optimization advice for a specific notebook/job run
uv run burnt advise --run-id 1234567890
```

*Example Output:*
```
  Compute Migration Analysis
┌──────────────────┬───────────┬──────────┬────────────────────┐
│ Compute Type     │ Est. Cost │ Savings  │ Tradeoff           │
├──────────────────┼───────────┼──────────┤────────────────────┤
│ All-Purpose      │ $45.12    │ baseline │ (Your test run)    │
│ Jobs Compute     │ $18.25    │ -60%     │ Recommended        │
│ SQL Serverless   │ $28.50    │ -37%     │ Fastest cold start │
└──────────────────┴───────────┴──────────┴────────────────────┘
💡 Advice: Peak memory utilization was 14%. Downsizing from DS4_v2 to DS3_v2 will save an additional 50% without impacting runtime.
```

---

## Cost-Aware Linting

Detect inefficient code patterns that spike Databricks DBUs or cause out-of-memory errors on the driver.

```bash
# Lint an entire directory of SQL, PySpark, and Notebooks
uv run burnt lint ./src/pipelines/
```

*Example Output:*
```
⚠ daily_revenue.sql:12  ORDER BY without LIMIT forces global sort
✗ etl_pipeline.sql:45   collect() without limit() — will OOM on large tables
⚠ etl_pipeline.py:67    Python UDF has 10-100x overhead vs Pandas UDF
```

---

## Pre-Execution Estimation

Estimate the cost of a standalone file or direct query string.

```bash
# Estimate a single SQL file
uv run burnt estimate queries/daily_revenue.sql

# Estimate with specific cluster configuration
uv run burnt estimate queries/daily_revenue.sql \
  --instance-type Standard_DS4_v2 \
  --num-workers 4

# Direct SQL Input
uv run burnt estimate "SELECT * FROM sales"
```

---

## CI/CD Integration

`burnt` is designed to run in GitHub Actions, GitLab CI, and Azure DevOps to catch expensive code and misconfigurations before they are deployed.

### 1. The Linter Gate
Fail the build if critical anti-patterns (like unbounded `collect()`) are introduced.
```bash
uv run burnt lint ./notebooks/ --fail-on error
```

### 2. The Budget Gate
Fail the build if the static cost projection of a Databricks Asset Bundle (DAB) exceeds the team's budget.
```bash
uv run burnt estimate-job ./databricks.yml --max-budget 50.00
```

### 3. Output Formats for CI
All commands support JSON output for easy parsing with `jq`.
```bash
uv run burnt estimate "SELECT 1" --output json > estimate.json
```
