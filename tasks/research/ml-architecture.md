# Research: ML Architecture v4 — Three-Layer Cost Prediction

---

## Metadata

```yaml
id: r7-ml-architecture-v4
status: complete
priority: critical
created: 2026-03-14
supersedes: [r7-ml-regression-pivot, s5-02-classification-model]
decision: Three-layer architecture (Base Model + Transfer Function + Calibration)
```

---

## Architecture Decision

### Problem

A model trained on historical `system.query.history` data must predict cost for live notebook sessions on different clusters. The distributions shift: table sizes grow, cluster shapes differ, Spark versions change, caching states vary.

### Decision: Three-Layer Architecture

| Layer | Responsibility | Learns From | Update Frequency |
|-------|---------------|-------------|------------------|
| **Base Model** | Structural cost relationships (normalised ratios → DBU) | Offline training on <1K historical samples | Retrain monthly or when data doubles |
| **Transfer Function** | Source cluster → target cluster scaling | Physics (Amdahl's law, memory thresholds) | Deterministic + one tunable scalar |
| **Calibration** | Systematic bias correction | EMA on predicted-vs-actual | Per-deployment (when billing arrives) |

### Why Not Direct Learning

- Online fine-tuning requires labels (actual DBU) that don't exist until after deployment
- `HistGradientBoostingRegressor` doesn't support `partial_fit()`
- <1K samples insufficient for learning cluster-transfer relationships via ML
- Physics-based transfer is more robust than learned transfer at low sample counts

### Feature Strategy: Normalised Ratios

Train on cluster-invariant ratios, not raw metrics:
- `bytes_per_worker` instead of `total_bytes`
- `shuffle_ratio` (shuffle/read) instead of `shuffle_bytes`
- `spill_ratio` (spill/read) instead of `spill_bytes`
- `selectivity` (read/table_size) instead of `read_bytes`

This compresses the feature space so historical data transfers to current conditions.

### Metric Capture: sparkMeasure as Core

- sparkMeasure is a core dependency (not optional)
- Provides real-time stage-level metrics via Spark Listener
- Falls back to burnt native listener when JVM jar unavailable
- `system.query.history` used for training data + cross-validation

### Sprint 5 Task Breakdown

| Task | What | Blocked By |
|------|------|------------|
| `s5-00` | Listener infrastructure (sparkMeasure + native fallback) | s1-01 |
| `s5-01` | Feature extraction (3-tier: AST + table stats + observed metrics) | s5-00, s3-01 |
| `s5-02` | Transfer function (source → target cluster scaling) | s5-01 |
| `s5-03` | Cost regression model (HistGBR on normalised features) | s5-01, s2-03 |
| `s5-04` | Cost projection (per-run → daily/monthly/yearly + VM) | s5-02, s5-03 |
| `s5-05` | Prophet workspace budget forecasting (optional, separate concern) | — |

---

## References

- Baldacci & Golfarelli (2019), "A Cost Model for Spark SQL", IEEE TKDE — R² = 0.966
- sparkMeasure (LucaCanali/sparkMeasure) — Spark Listener-based metrics
- Amdahl's Law — parallel scaling efficiency ~0.7-0.85 for shuffle-heavy Spark
- FinOps Foundation — <20% forecast variance target
