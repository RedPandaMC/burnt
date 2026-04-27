# tasks/archive/

These tasks completed the **pre-pivot (crystal ball) design** — a Databricks-first workspace monitoring and cost drift product.

They were completed correctly for that design but **do not apply to the current architecture**, which is:
- CLI-first notebook quality and cost analysis
- sparkMeasure-based runtime capture
- Static lint as the primary value, Databricks as optional enrichment

## Archived Files

| File | What it did |
|------|-------------|
| `01-tag-attribution.md.completed` | Cost attribution by Databricks resource tags |
| `02-idle-cluster-detection.md.completed` | Detect clusters running with no active jobs |
| `03-cost-drift.md.completed` | Week-over-week cost drift alerting |
| `04-job-report.md.completed` | Per-job cost breakdown report |
| `05-pipeline-report.md.completed` | DLT pipeline cost report |
| `06-watch-orchestration.md.completed` | `burnt.watch()` orchestration core |
| `07-alert-dispatch.md.completed` | Slack/Teams/webhook alert dispatch |
| `08-monitoring-template.md.completed` | Deployable Databricks monitoring notebook |

These features may return as part of `burnt[databricks]` (Phase 4) but will be redesigned to fit the new architecture.
