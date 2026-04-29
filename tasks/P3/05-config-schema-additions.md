# Task: Config schema additions — pricing backend + system-table paths

---

## Metadata

```yaml
id: P3-05-config-schema-additions
status: todo
phase: 3
priority: high
agent: ~
blocked_by: [P3-01, P3-02]
created_by: planner
```

---

## Context

### Goal

Add two new configuration sections to the `burnt.toml` / `[tool.burnt]` schema:

1. **`[burnt.pricing]`** — backend selection when multiple pricing extras are installed
2. **`[burnt.databricks.system_tables]`** — configurable system-table paths for orgs that mirror system tables into private schemas

Also add **`[burnt.onprem_spark]`** — per-unit cost rates for the `[onprem-spark]` pricing backend.

These are additive config changes: existing configs are unaffected; new keys are ignored when the relevant extra is not installed.

### Files to read

```
# Required
src/burnt/_config/
docs/configuration.md
docs/modular-architecture.md   §3, §3.5
pyproject.toml

# Reference
DESIGN.md §11 Configuration
src/burnt/core/exceptions.py
```

### Background

**`[burnt.pricing]`** (from modular-architecture.md §3):

```toml
[burnt.pricing]
backend = "azure-databricks"   # which PricingBackend to use when multiple extras installed
                               # options: "azure-databricks", "aws-databricks", "gcp-databricks",
                               #          "onprem-spark"
                               # default: auto (uses whichever single backend is installed;
                               #          errors if multiple installed and none configured)
```

Env-var override: `BURNT_PRICING_BACKEND`.

**`[burnt.databricks.system_tables]`** (exact keys from modular-architecture.md §3.5):

```toml
[burnt.databricks.system_tables]
query_history             = "system.query.history"
billing_usage             = "system.billing.usage"
list_prices               = "system.billing.list_prices"
information_schema_tables = "system.information_schema.tables"
compute_clusters          = "system.compute.clusters"
node_timeline             = "system.compute.node_timeline"
enabled                   = true   # master switch; false = never query system tables
```

Env-var overrides: `BURNT_DATABRICKS_SYSTEM_TABLES_QUERY_HISTORY`, etc.

Behaviour rules (must be implemented):
1. If a configured table is unreadable, log once at INFO and fall back silently (never error)
2. Paths resolved once per `burnt.check()` call, cached on result
3. `burnt doctor` reports which system tables are reachable

**`[burnt.onprem_spark]`**:

```toml
[burnt.onprem_spark]
cost_per_vcpu_hour   = 0.048   # $/vCPU-hour (required when [onprem-spark] is the backend)
cost_per_gb_hour     = 0.006   # $/GB-hour memory
cost_per_gb_shuffle  = 0.001   # $/GB shuffled
```

Env-var overrides: `BURNT_ONPREM_SPARK_COST_PER_VCPU_HOUR`, etc.

Use the existing Pydantic Settings models in `src/burnt/_config/` — extend them, don't replace them. Follow the same pattern as the existing `LintSettings` and `CacheSettings` classes.

---

## Acceptance Criteria

- [ ] `BurntConfig` (or equivalent Pydantic settings model) has a `pricing` sub-model with `backend: str | None = None`
- [ ] `BurntConfig` has a `databricks.system_tables` sub-model with the 6 path fields and `enabled: bool = True`
- [ ] `BurntConfig` has an `onprem_spark` sub-model with the 3 cost rate fields
- [ ] All new fields are readable from `burnt.toml`, `[tool.burnt]` in `pyproject.toml`, and env vars
- [ ] Invalid `backend` value (not one of the four named backends) raises `ConfigError` with a helpful message listing valid options
- [ ] `burnt doctor` output includes a "System tables" section reporting reachability for each configured path (when `[databricks]` is installed)
- [ ] `burnt init` config generator includes the new sections as commented-out examples
- [ ] `docs/configuration.md` updated with the new sections (this is the main user-facing doc)
- [ ] `uv run pytest -m unit -v` passes (add unit tests for config loading and validation)
- [ ] `uv run ruff check src/` passes

---

## Verification

### Commands

```bash
uv run pytest -m unit -v -k "config or settings or system_tables or pricing"
uv run ruff check src/ tests/
# Config loading smoke test
python -c "
from burnt._config import load_config
import os
os.environ['BURNT_PRICING_BACKEND'] = 'azure-databricks'
cfg = load_config()
assert cfg.pricing.backend == 'azure-databricks'
print('config ok')
"
```

### Integration Check

- [ ] A `burnt.toml` with `[burnt.databricks.system_tables] enabled = false` causes `burnt doctor` to report system tables as disabled
- [ ] A `burnt.toml` with `[burnt.pricing] backend = "onprem-spark"` causes `result.cost_estimate.backend == "onprem-spark"` (once P3-02 is implemented)

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked by P3-01 (extras must exist for the backend validation list to be authoritative) and P3-02 (PricingBackend protocol must exist before backend selection logic makes sense).
