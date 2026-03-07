# Task: Add AWS and GCP Databricks pricing

---

## Metadata

```yaml
id: p4-04-aws-gcp-pricing
status: todo
phase: 4
priority: medium
agent: kimi
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Extend `src/dburnrate/core/pricing.py` to support AWS and GCP Databricks pricing alongside the existing Azure data. Add cloud auto-detection from workspace URL and `--cloud` CLI flag. The estimators should select the correct DBU rate table based on cloud.

### Files to read

```
# Required
src/dburnrate/core/pricing.py
src/dburnrate/core/models.py        # ClusterConfig
src/dburnrate/core/config.py        # Settings
src/dburnrate/cli/main.py
tests/unit/core/test_pricing.py
```

### Background

Current pricing is Azure-only (`AZURE_DBU_RATES`, `AZURE_INSTANCE_DBU`). Add:

**AWS** (us-east-1 reference prices, USD/DBU):
- Jobs Compute: $0.20, All-Purpose: $0.55, SQL Serverless: $0.22, Photon multiplier: 2.9×

**GCP** (us-central1 reference prices, USD/DBU):
- Jobs Compute: $0.19, All-Purpose: $0.52, SQL Serverless: $0.21, Photon multiplier: 2.5×

Cloud detection from workspace URL:
- `*.azuredatabricks.net` → Azure
- `*.cloud.databricks.com` → AWS
- `*.gcp.databricks.com` → GCP

Add `CLOUD_DBU_RATES: dict[str, dict[str, Decimal]]` as the unified structure.
Add `detect_cloud(workspace_url: str) -> str` returning `"azure" | "aws" | "gcp" | "unknown"`.

---

## Acceptance Criteria

- [ ] `pricing.py` has `AWS_DBU_RATES`, `GCP_DBU_RATES`, `CLOUD_DBU_RATES` dicts
- [ ] `detect_cloud(url) -> str` implemented with regex matching
- [ ] `get_dbu_rate(sku, cloud="azure") -> Decimal` updated to accept cloud param
- [ ] `ClusterConfig` gets optional `cloud: str = "azure"` field
- [ ] CLI `estimate` command gets `--cloud` flag (auto-detected from `--workspace-url` if not set)
- [ ] Unit tests in `tests/unit/core/test_pricing_multicloud.py`
- [ ] All existing tests pass
- [ ] `uv run ruff check src/ tests/` exits 0

---

## Verification

```bash
uv run pytest -m unit -v tests/unit/core/test_pricing_multicloud.py
uv run pytest -m unit -v 2>&1 | tail -3
uv run ruff check src/ tests/
```

---

## Handoff

### Result

[Executor: fill in after completion]

### Blocked reason

[If blocked, explain here]
