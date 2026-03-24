# Phase 4: Monitoring & Alerts

> `burnt.watch()` complete. Tags, idle, drift, job/pipeline reports. `.alert()` dispatch. Monitoring template.

**Duration:** 3 weeks
**Depends on:** Phase 3
**Gate:** All watch functions correct with mocked system tables. Alerts send. Template executes.

---

## Tasks

### P4-01: Tag Attribution (Week 14)

`system.billing.usage` grouped by tag. Clusters API for cluster tags. DLT SKUs included. Untagged bucket. Sorted by cost.

### P4-02: Idle Cluster Detection (Week 14)

`system.compute.node_timeline`, `avg(cpu) < threshold`. All-Purpose only. Wasted cost. Auto-termination recommendation.

### P4-03: Cost Drift (Week 14–15)

Baseline: 30-day median per job/pipeline. Drift: `(recent - baseline) / baseline`. Root cause hints from table growth. DLT: per-table changes from `pipeline_event_log`.

### P4-04: Job Report (Week 15)

30 runs from `job_run_timeline`. Per-run cost. Trend classification. Significant change annotations (>20% jump).

### P4-05: Pipeline Report (Week 15)

30 updates from `pipeline_event_log`. Per-table cost trend. Dominant cost table.

### P4-06: `burnt.watch()` Orchestration (Week 15)

Parameters control what runs: `tag_key`, `drift_threshold`, `idle_threshold`, `budget`, `days`, `job_id`, `pipeline_id`. Reads defaults from `[watch]` in `burnt.toml`. Returns `WatchResult` with `.by_tag`, `.idle`, `.drift`, `.jobs`, `.pipelines` properties.

### P4-07: Alert Dispatch (Week 16)

`.alert(slack=, teams=, webhook=, delta=)`. No args → reads `[alert]` from `burnt.toml`.

Slack: `slack-sdk` Block Kit if installed, simple webhook POST fallback. Title → metric → top 5 → workspace link. DLT per-table in message.

Teams: Adaptive Card JSON via webhook.

Webhook: custom `payload_fn` or default JSON.

Delta: structured write via Spark or Statement Execution API.

### P4-08: Monitoring Template (Week 16)

`templates/burnt_monitor.py`. Databricks notebook with widgets: thresholds, webhook URL, Delta table. Calls `burnt.watch()` + `.alert()`. Single-node Jobs Compute, < $0.10/run. Deployment docs.

---

## Gate

- [ ] Tag attribution with DLT costs
- [ ] Idle clusters: All-Purpose only, wasted cost
- [ ] Drift: jobs + DLT, root cause hints
- [ ] Job report: 30-run trend
- [ ] Pipeline report: per-table trend
- [ ] `burnt.watch()` reads defaults from burnt.toml
- [ ] `.alert()` reads channel from burnt.toml when no args
- [ ] Slack + Teams + webhook + Delta work
- [ ] Template deployable, < $0.10/run
