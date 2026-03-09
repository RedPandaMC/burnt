# Programmatic Workflows

> How to use `import burnt` as a Data Engineer's Best Friend inside Databricks Notebooks and Python pipelines.

---

## 1. Context-Aware "End of Notebook" Advisor

This is the primary workflow for interactive development. 

When you finish writing and testing a new pipeline in a Databricks Notebook, your code is running on an expensive **All-Purpose Cluster**. You need to know how to configure this pipeline when you deploy it as a scheduled Job.

Simply add this to the final cell of your notebook:

```python
import burnt

# Analyzes the queries you *just ran* in this active Spark session
advice = burnt.advise_current_session()

# Prints a rich HTML table in the notebook UI
advice.display()
```

`burnt` will automatically:
1. Connect to your active `SparkSession`.
2. Analyze the peak memory, disk spill, and duration of the queries you just executed.
3. Recommend an optimized **Jobs Compute** instance type (e.g., downsizing from `DS4_v2` to `DS3_v2`).
4. Calculate exactly how much money you will save per run by applying these changes.

---

## 2. Cost-Aware Unit Testing (`pytest`)

Data engineering teams write unit tests to ensure their data transformations are mathematically correct. With `burnt`, you can write unit tests to ensure your pipelines remain financially viable.

This bypasses the need to write complex bash wrappers in your CI/CD pipelines.

```python
# test_pipeline_costs.py
import pytest
import burnt

def test_daily_job_cost_is_under_budget():
    # Pass the actual file path the scheduler will run
    cost = burnt.estimate_file("src/jobs/daily_aggregation.py")
    
    # Fail the CI/CD pipeline if the code changes made it too expensive
    assert cost.estimated_cost_usd < 5.00, f"Cost regression! New cost: ${cost.estimated_cost_usd}"

def test_no_driver_oom_risks():
    # Ensure no developer introduced an unbounded .collect()
    issues = burnt.lint_file("src/jobs/daily_aggregation.py")
    errors = [i for i in issues if i.severity == "error"]
    
    assert len(errors) == 0, f"Anti-patterns found: {errors}"
```

---

## 3. The "Circuit Breaker" Pattern

If your pipeline dynamically generates massive SQL queries or DataFrames based on runtime parameters (like backfilling 5 years of data vs. 1 day of data), a static CI/CD gate cannot protect you. 

Use `burnt` as a dynamic circuit breaker right before execution:

```python
import burnt
import requests

def run_etl(start_date, end_date):
    # Dynamically generate the query
    sql = generate_massive_query(start_date, end_date)
    
    # Estimate the cost BEFORE running it
    estimate = burnt.estimate(sql)
    
    if estimate.estimated_cost_usd > 50.00:
        requests.post("slack_webhook_url", json={
            "text": f"🚨 Query blocked! Estimated cost: ${estimate.estimated_cost_usd}\nSQL: {sql[:100]}..."
        })
        raise Exception("Cost limit exceeded. Query aborted.")
        
    # Safe to run!
    spark.sql(sql)
```
