# Task: R1 - Parquet Throughput Benchmarks

---

## Metadata

```yaml
id: r1-parquet-throughput-benchmarks
status: todo
sprint: 0
priority: normal
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Determine empirical baselines for Parquet scan, shuffle, and join throughput per instance type. These baselines are required to ground the static and hybrid estimation models with real-world constants, particularly when modeling execution duration.

### Files to Read

```
DESIGN.md § "Research Backlog"
src/burnt/estimators/static.py
```

### Action Items

1. Run standard TPC-DS/TPC-H queries focused on I/O.
2. Benchmark standard D/E/F series instances on Azure.
3. Record throughput limits (MB/s or GB/s) for scanning from cloud storage and shuffling data between nodes.
4. Update the `throughput_bps` and `shuffle_overhead_s` constants in the static estimation logic with these empirically gathered figures.

---

## Acceptance Criteria

- [ ] Benchmarking scripts created and run on Databricks.
- [ ] Throughput constants documented in `DESIGN.md` or a `docs/research/` document.
- [ ] Constants updated in `src/burnt/estimators/static.py`.

---

## Handoff

```yaml
status: todo
```