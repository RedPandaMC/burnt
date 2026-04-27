```yaml
id: PX-06-tasks-cleanup
status: done
phase: X
priority: medium
agent: claude-sonnet-4-6
completed_by: claude-sonnet-4-6
created_by: planner
```

## Implementation

### Changes Made

- `tasks/archive/` — created; moved 8 old P4 completed files (tag-attribution, idle-cluster-detection, cost-drift, job-report, pipeline-report, watch-orchestration, alert-dispatch, monitoring-template)
- `tasks/PX/` — created; 6 new task files (01 through 06)
- `tasks/P2/08-explain-enrichment.md` — rewritten with acceptance criteria and background
- `tasks/P2/10-cost-estimation.md` — rewritten with formula and correlation algorithm
- `tasks/P3/06-cli-implementation.md` — rewritten; marks the old path as broken, adds SARIF and event-log criteria
- `tasks/P3/08-performance-tuning.md` — rewritten with concrete benchmark targets and script spec
- `tasks/P5/01-e2e-tests.md` — rewritten with specific fixture specs and assertion requirements
- `tasks/P5/06-ci-examples.md` — rewritten as full CI deliverable (3 files + docs)
- `tasks/README.md` — rewritten with new phase table
- `tasks/archive/README.md` — created with explanation

### Verification Results

- All task files have explicit acceptance criteria checkboxes
- No task has `completed_by` set with `status: todo` (contradiction resolved)
- `tasks/README.md` phase table matches directory contents
