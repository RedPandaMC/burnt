# CLI Workflows and CI/CD Integration

> Comprehensive guide for using dburnrate in command-line workflows and CI/CD pipelines

---

## Table of Contents

1. [Basic CLI Usage](#basic-cli-usage)
2. [Batch Analysis Workflows](#batch-analysis-workflows)
3. [Self-Referential Estimation](#self-referential-estimation)
4. [CI/CD Integration](#cicd-integration)
5. [Output Formats](#output-formats)
6. [Troubleshooting](#troubleshooting)

---

## Basic CLI Usage

### Single File Estimation

Estimate cost of a single SQL file:

```bash
dburnrate estimate queries/daily_revenue.sql
```

Estimate cost with specific cluster configuration:

```bash
dburnrate estimate queries/daily_revenue.sql \
  --instance-type Standard_DS4_v2 \
  --num-workers 4
```

### Direct SQL Input

Estimate cost of SQL passed directly:

```bash
dburnrate estimate "SELECT customer_id, SUM(amount) FROM orders GROUP BY 1"
```

### Notebook Estimation

Estimate cost of a Jupyter notebook:

```bash
dburnrate estimate notebooks/analysis.ipynb
```

Or a Databricks `.dbc` archive:

```bash
dburnrate estimate notebooks/etl_pipeline.dbc
```

### What-if Scenarios

Compare Photon vs non-Photon costs:

```bash
dburnrate whatif queries/complex_join.sql --scenario photon
```

Serverless migration impact:

```bash
dburnrate whatif queries/etl_job.sql --scenario serverless --utilization 60
```

---

## Batch Analysis Workflows

### Glob Patterns

Analyze all SQL files in a directory:

```bash
dburnrate estimate-batch "queries/*.sql"
```

Analyze all notebooks recursively:

```bash
dburnrate estimate-batch "notebooks/**/*.ipynb"
```

### Directory Analysis

Analyze all supported files in a directory (recursive by default):

```bash
dburnrate estimate-batch ./queries/
```

Exclude specific patterns:

```bash
dburnrate estimate-batch ./queries/ --exclude "*_test.sql" --exclude "*.bak"
```

### Multiple Specific Files

Analyze specific files:

```bash
dburnrate estimate-batch query1.sql query2.py notebook.ipynb
```

### Export Results

Export to CSV:

```bash
dburnrate estimate-batch queries/ --format csv --output costs.csv
```

Export to JSON:

```bash
dburnrate estimate-batch queries/ --format json --output costs.json
```

Export summary only:

```bash
dburnrate estimate-batch queries/ --format json --summary-only --output summary.json
```

### Filtering and Sorting

Show only high-confidence estimates:

```bash
dburnrate estimate-batch queries/ --min-confidence high
```

Sort by cost (highest first):

```bash
dburnrate estimate-batch queries/ --sort-by cost --order desc
```

Limit number of results:

```bash
dburnrate estimate-batch queries/ --limit 10
```

### Parallel Processing

Enable parallel processing for large batches:

```bash
dburnrate estimate-batch queries/ --parallel 8
```

Adjust batch size:

```bash
dburnrate estimate-batch queries/ --parallel 8 --batch-size 50
```

---

## Self-Referential Estimation

### Basic Usage

Add to the bottom of any Python file to estimate its own cost:

```python
# my_analysis.py
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("analysis").getOrCreate()

# Your analysis code here...
df = spark.read.table("sales")
result = df.groupBy("region").agg({"amount": "sum"})
result.show()

# At the bottom - estimate cost of this file
import dburnrate
estimate = dburnrate.estimate_self()
print(f"This analysis would cost: ${estimate.cost_usd:.4f}")
```

### In Notebooks

In a Databricks or Jupyter notebook:

```python
# Cell 1-10: Your analysis code
# ...

# Final cell
import dburnrate
estimate = dburnrate.estimate_self()
print(f"Notebook estimated cost: ${estimate.cost_usd:.4f} ({estimate.confidence})")
```

### CLI Self-Estimation

Estimate cost of the current working file:

```bash
dburnrate estimate-self
```

Estimate a specific file:

```bash
dburnrate estimate-self --file ./my_analysis.py
```

With output format:

```bash
dburnrate estimate-self --format json
```

---

## CI/CD Integration

### GitHub Actions

#### Basic Cost Check

```yaml
# .github/workflows/cost-check.yml
name: Cost Estimation Check

on:
  pull_request:
    paths:
      - 'queries/**'
      - 'notebooks/**'

jobs:
  estimate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install dburnrate
        run: pip install dburnrate[sql]

      - name: Estimate costs
        run: |
          dburnrate estimate-batch queries/ \
            --format json \
            --output costs.json

      - name: Comment PR with costs
        uses: actions/github-script@v7
        with:
          script: |
            const fs = require('fs');
            const costs = JSON.parse(fs.readFileSync('costs.json', 'utf8'));
            
            const body = `## 💰 Cost Estimation Summary
            
            | Metric | Value |
            |--------|-------|
            | Total Estimated Cost | $${costs.summary.total_cost_usd.toFixed(4)} |
            | Number of Queries | ${costs.summary.file_count} |
            | Average Cost | $${costs.summary.average_cost_usd.toFixed(4)} |
            | Highest Cost File | ${costs.summary.highest_cost_file} |
            
            <details>
            <summary>View Details</summary>
            
            | File | Cost | Confidence |
            |------|------|------------|
            ${costs.files.map(f => `| ${f.path} | $${f.cost_usd.toFixed(4)} | ${f.confidence} |`).join('\n')}
            </details>
            `;
            
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: body
            });
```

#### Cost Regression Detection

```yaml
# .github/workflows/cost-regression.yml
name: Cost Regression Detection

on:
  pull_request:
    paths:
      - 'queries/**'

jobs:
  compare:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install dburnrate
        run: pip install dburnrate[sql]

      - name: Estimate costs on PR branch
        run: |
          dburnrate estimate-batch queries/ \
            --format json \
            --output pr-costs.json

      - name: Checkout main branch
        run: |
          git stash
          git checkout main

      - name: Estimate costs on main branch
        run: |
          dburnrate estimate-batch queries/ \
            --format json \
            --output main-costs.json

      - name: Compare costs
        run: |
          python << 'EOF'
          import json
          
          with open('pr-costs.json') as f:
              pr = json.load(f)
          with open('main-costs.json') as f:
              main = json.load(f)
          
          pr_total = pr['summary']['total_cost_usd']
          main_total = main['summary']['total_cost_usd']
          change = ((pr_total - main_total) / main_total) * 100
          
          print(f"Main branch cost: ${main_total:.4f}")
          print(f"PR branch cost: ${pr_total:.4f}")
          print(f"Change: {change:+.2f}%")
          
          if change > 20:
              print("::error::Cost increase exceeds 20% threshold!")
              exit(1)
          EOF
```

#### Budget Enforcement

```yaml
# .github/workflows/budget-check.yml
name: Budget Enforcement

on:
  pull_request:
    paths:
      - 'queries/**'

jobs:
  budget:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install dburnrate
        run: pip install dburnrate[sql]

      - name: Check budget
        run: |
          dburnrate estimate-batch queries/ \
            --format json \
            --output costs.json
          
          # Check if total cost exceeds $1.00
          python << 'EOF'
          import json
          import sys
          
          with open('costs.json') as f:
              costs = json.load(f)
          
          total = costs['summary']['total_cost_usd']
          budget = 1.00
          
          if total > budget:
              print(f"::error::Budget exceeded! ${total:.4f} > ${budget:.2f}")
              sys.exit(1)
          else:
              print(f"✅ Within budget: ${total:.4f} <= ${budget:.2f}")
          EOF
```

### GitLab CI

```yaml
# .gitlab-ci.yml
cost-estimation:
  stage: test
  image: python:3.12-slim
  before_script:
    - pip install dburnrate[sql]
  script:
    - dburnrate estimate-batch queries/ --format json --output costs.json
    - |
      python << 'EOF'
      import json
      with open('costs.json') as f:
          costs = json.load(f)
      print(f"Total estimated cost: ${costs['summary']['total_cost_usd']:.4f}")
      print(f"Files analyzed: {costs['summary']['file_count']}")
      EOF
  artifacts:
    reports:
      dotenv: costs.env
    paths:
      - costs.json
  only:
    - merge_requests
    - main
```

### Pre-commit Hook

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: cost-estimation
        name: Estimate Query Costs
        entry: dburnrate estimate-batch
        language: system
        files: \.(sql|py|ipynb)$
        args: ['--format', 'json', '--quiet']
```

Or using a custom script:

```bash
#!/bin/bash
# .pre-commit-cost-check.sh

# Get list of staged SQL/Python files
files=$(git diff --cached --name-only --diff-filter=ACM | grep -E '\.(sql|py|ipynb)$')

if [ -z "$files" ]; then
    exit 0
fi

# Estimate costs
echo "Estimating costs for changed files..."
dburnrate estimate-batch $files --format json --output .pre-commit-costs.json

# Check if any high-cost queries
python << 'EOF'
import json
import sys

with open('.pre-commit-costs.json') as f:
    costs = json.load(f)

high_cost_files = [
    f for f in costs['files']
    if f['cost_usd'] > 0.10  # $0.10 threshold
]

if high_cost_files:
    print("⚠️  High-cost queries detected:")
    for f in high_cost_files:
        print(f"  - {f['path']}: ${f['cost_usd']:.4f}")
    print("\nConsider reviewing these queries before committing.")
    # Don't block commit, just warn
EOF

exit 0
```

### Azure DevOps

```yaml
# azure-pipelines.yml
trigger:
  paths:
    include:
      - queries/*
      - notebooks/*

pool:
  vmImage: 'ubuntu-latest'

steps:
- task: UsePythonVersion@0
  inputs:
    versionSpec: '3.12'
  displayName: 'Use Python 3.12'

- script: |
    pip install dburnrate[sql]
  displayName: 'Install dburnrate'

- script: |
    dburnrate estimate-batch queries/ \
      --format json \
      --output $(Build.ArtifactStagingDirectory)/costs.json
  displayName: 'Estimate costs'

- task: PublishBuildArtifacts@1
  inputs:
    pathToPublish: '$(Build.ArtifactStagingDirectory)/costs.json'
    artifactName: 'cost-estimates'
```

---

## Output Formats

### JSON Format

Full output with all details:

```json
{
  "summary": {
    "total_cost_usd": 1.2345,
    "total_dbu": 2.469,
    "file_count": 10,
    "average_cost_usd": 0.1234,
    "highest_cost_file": "queries/complex_etl.sql",
    "lowest_cost_file": "queries/simple_select.sql",
    "by_confidence": {
      "high": 6,
      "medium": 3,
      "low": 1
    }
  },
  "files": [
    {
      "path": "queries/complex_etl.sql",
      "cost_usd": 0.5000,
      "dbu": 1.0,
      "confidence": "medium",
      "signal": "static",
      "operations": ["MERGE INTO", "JOIN", "GROUP BY"],
      "warnings": ["cross_join"]
    }
  ],
  "generated_at": "2026-03-07T12:00:00Z"
}
```

### CSV Format

```csv
path,cost_usd,dbu,confidence,signal,operations,warnings
queries/complex_etl.sql,0.5000,1.0,medium,static,"MERGE INTO,JOIN,GROUP BY",cross_join
queries/simple_select.sql,0.0100,0.02,high,static,"SELECT",
```

### Table Format (Default)

```
Cost Estimation Summary
┌─────────────────────┬────────────┐
│ Metric              │ Value      │
├─────────────────────┼────────────┤
│ Total Cost          │ $1.2345    │
│ Total DBU           │ 2.469      │
│ Files Analyzed      │ 10         │
│ Average Cost        │ $0.1234    │
│ Highest Cost File   │ complex... │
└─────────────────────┴────────────┘

Individual Estimates
┌──────────────────────┬──────────┬───────┬────────────┬────────┐
│ File                 │ Cost     │ DBU   │ Confidence │ Signal │
├──────────────────────┼──────────┼───────┼────────────┼────────┤
│ complex_etl.sql      │ $0.5000  │ 1.00  │ medium     │ static │
│ simple_select.sql    │ $0.0100  │ 0.02  │ high       │ static │
│ ...                  │ ...      │ ...   │ ...        │ ...    │
└──────────────────────┴──────────┴───────┴────────────┴────────┘
```

---

## Troubleshooting

### Common Issues

#### "No files found matching pattern"

```bash
# Make sure glob patterns are quoted
dburnrate estimate-batch "queries/*.sql"  # ✅ Works
dburnrate estimate-batch queries/*.sql     # ❌ Shell expands before dburnrate sees it
```

#### "Failed to parse SQL"

```bash
# Check SQL syntax
dburnrate estimate query.sql --verbose

# Try with explicit dialect
dburnrate estimate query.sql --dialect databricks
```

#### "No workspace configured"

For hybrid estimation with EXPLAIN and historical data:

```bash
export DBURNRATE_WORKSPACE_URL=https://adb-xxx.azuredatabricks.net
export DBURNRATE_TOKEN=dapi...

dburnrate estimate query.sql --warehouse-id sql-warehouse-abc
```

### Performance Tips

1. **Use parallel processing** for large batches (>100 files)
2. **Cache workspace connections** - set environment variables once
3. **Filter by confidence** to focus on actionable results
4. **Use --summary-only** when you only need totals

### Debug Mode

```bash
# Enable verbose logging
export DBURNRATE_DEBUG=1
dburnrate estimate query.sql --verbose

# Or use --debug flag
dburnrate estimate query.sql --debug
```

---

## Best Practices

### 1. Cost Budgets

Set per-PR cost budgets:

```yaml
# In CI config
- name: Check cost budget
  run: |
    dburnrate estimate-batch queries/ --format json --output costs.json
    python << 'EOF'
    import json
    with open('costs.json') as f:
        costs = json.load(f)
    
    BUDGET = 5.00  # $5.00 per PR
    if costs['summary']['total_cost_usd'] > BUDGET:
        print(f"❌ Budget exceeded: ${costs['summary']['total_cost_usd']:.2f} > ${BUDGET:.2f}")
        exit(1)
    EOF
```

### 2. Progressive Rollout

Start with static analysis only, then add:
1. Workspace connection for EXPLAIN data
2. Historical fingerprinting for recurring queries
3. Delta metadata for scan sizes

### 3. Documentation

Document cost expectations:

```markdown
## Cost Expectations

This repository contains queries with the following cost characteristics:

| Query Type | Avg Cost | Max Cost |
|------------|----------|----------|
| Simple SELECT | $0.01 | $0.05 |
| Aggregation | $0.05 | $0.20 |
| ETL Pipeline | $0.50 | $2.00 |

Run `dburnrate estimate-batch queries/` to see current estimates.
```

---

## Examples

### Full CI/CD Pipeline

```yaml
# .github/workflows/full-pipeline.yml
name: Full Cost Pipeline

on: [pull_request]

jobs:
  analyze:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      
      - name: Install
        run: pip install dburnrate[sql]
      
      - name: Analyze
        run: |
          # Estimate all queries
          dburnrate estimate-batch queries/ \
            --format json \
            --output costs.json \
            --parallel 4
          
          # Check budget
          python << 'EOF'
          import json
          import sys
          
          with open('costs.json') as f:
              costs = json.load(f)
          
          total = costs['summary']['total_cost_usd']
          print(f"Total cost: ${total:.4f}")
          
          if total > 10.00:
              print("::error::Exceeds $10 budget!")
              sys.exit(1)
          EOF
      
      - name: Report
        uses: actions/github-script@v7
        with:
          script: |
            const fs = require('fs');
            const costs = JSON.parse(fs.readFileSync('costs.json'));
            
            await github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: `💰 Total estimated cost: $${costs.summary.total_cost_usd.toFixed(4)}`
            });
```

---

## Support

For issues and feature requests, see the [GitHub repository](https://github.com/anomalyco/dburnrate).

For workflow questions, refer to [AGENTS.md](../AGENTS.md) for development guidelines.
