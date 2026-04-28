# Configuration Reference

`burnt` is zero-config by default — every setting has a sensible default and
the tool works offline with no credentials. This reference covers the full
configuration surface for teams that need to customise behaviour.

---

## Configuration Files

`burnt` discovers configuration by walking upward from the current working
directory, stopping at the git root or `$HOME`. It checks each directory in
this order:

1. `.burnt.toml` — dedicated burnt config file (top-level keys)
2. `pyproject.toml` with a `[tool.burnt]` section

The first match wins. If neither is found, all defaults apply.

Run `burnt init` to create a config file interactively.

---

## `.burnt.toml` Format

```toml
[lint]
select         = ["ALL"]   # Run all rules (default). Or list specific IDs/prefixes/tags.
extend-select  = []        # Add rules on top of select without replacing it.
ignore         = []        # Rule patterns to suppress globally.
extend-ignore  = []        # Add rules to ignore on top of ignore list.
fail-on        = "error"   # Exit non-zero at this severity: info | warning | error
exclude        = []        # Glob patterns for paths to skip entirely.

# [lint.per-file-ignores]
# "migrations/*.sql"      = ["BQ*"]            # prefix pattern
# "notebooks/explore*.py" = ["performance"]    # tag pattern

[cache]
enabled     = true
ttl-seconds = 3600.0   # Cache TTL in seconds (default: 1 hour)

# Top-level settings
target-currency = "USD"    # USD | EUR
pricing-source  = "api"    # api | static
```

---

## `pyproject.toml` Equivalent

Identical keys, nested under `[tool.burnt]`:

```toml
[tool.burnt.lint]
select        = ["ALL"]
ignore        = ["BP001"]     # exact ID
extend-ignore = ["style"]     # tag
fail-on       = "warning"
exclude       = ["tests/**"]

[tool.burnt.lint.per-file-ignores]
"migrations/*.sql"      = ["BQ*", "SQ001"]
"notebooks/explore*.py" = ["performance", "BP008"]

[tool.burnt.cache]
enabled     = true
ttl-seconds = 3600.0
```

Note: TOML keys use kebab-case (`ttl-seconds`, `fail-on`, `extend-ignore`).
The Python `Settings` model accepts snake_case equivalents internally.
`burnt init` always writes kebab-case.

---

## Rule Selection Patterns

All `select`, `ignore`, `extend-select`, `extend-ignore`, and `per-file-ignores`
fields accept the same pattern types:

| Pattern | Example | What it matches |
|---------|---------|-----------------|
| Exact code | `BP008` | One specific rule |
| Prefix | `BP`, `BQ`, `SDP` | All rules whose code starts with that string |
| Tag | `performance`, `pyspark`, `style` | All rules with that tag |
| Special | `ALL` | Every registered rule |

Patterns are resolved in this order:

1. `select` replaces the default active set (default: `["ALL"]`)
2. `extend-select` adds rules on top of the active set
3. `ignore` removes rules from the active set
4. `extend-ignore` removes additional rules
5. Per-file ignores subtract from the file-specific active set
6. Inline comment suppressions suppress individual findings

### Examples

```toml
[lint]
# Only run SQL rules
select = ["BQ", "SQ", "BD"]

# Run everything except style rules
select = ["ALL"]
ignore = ["style"]

# Run everything, but skip performance rules in notebook files
select = ["ALL"]

[lint.per-file-ignores]
"notebooks/**/*.py" = ["performance", "pyspark"]
```

### Tag vocabulary

| Tag | Rules it covers |
|-----|-----------------|
| `performance` | BD002, BP010, BP013, BP016, BP021, BP032, BQ002–BQ004, SDP004, SQ001 |
| `pyspark` | All BP0xx PySpark rules |
| `sql` | All BQ, SQ, BD, SQL-language rules |
| `style` | BNT-C01, BNT-I01, BNT-N01, BP001, BP002, BP005, BP007 |
| `sdp` | All SDP rules + BP022 |
| `correctness` | BN003, BNT-C01, BQ001 |
| `memory` | BP008, BP011, BP030, BP031 |
| `driver-bound` | BP008, BP011 |
| `notebook` | BB001, BN001–BN003, BP005–BP007 |
| `delta` | BD001, BD002 |

---

## Comment-Based Suppression

Suppress findings without touching the config file using inline comments.

### Suppress a specific rule on one line

```python
df.collect()  # burnt: ignore[BP008]
```

The comment applies to the line it appears on.

### Suppress using a standalone comment above the statement

```python
# burnt: ignore[BP008]
df.collect()
```

A comment on its own line suppresses the finding on the **next** line.

### Suppress multiple rules or patterns

```python
df.collect()  # burnt: ignore[BP008, driver-bound]
```

### Suppress all rules on a line

```python
df.collect()  # burnt: ignore
```

### Suppress rules for the whole file

Place at the top of the file:

```python
# burnt: ignore-file[pyspark]
```

```python
# burnt: ignore-file[BP008, BP011]
```

```python
# burnt: ignore-file   # suppresses everything
```

All pattern types (exact code, prefix, tag) work inside `[...]`.

---

## Settings Reference

### `LintSettings`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `select` | `list[str]` | `["ALL"]` | Rules to enable. Accepts exact IDs, prefixes, tags, and `ALL`. |
| `extend-select` | `list[str]` | `[]` | Add rules on top of `select` without replacing it. |
| `ignore` | `list[str]` | `[]` | Rule patterns to suppress globally. Applied after `select`. |
| `extend-ignore` | `list[str]` | `[]` | Add patterns to `ignore` without replacing it. |
| `fail-on` | `str` | `"error"` | Exit with code 1 when any issue at or above this severity is found. Values: `info`, `warning`, `error`. |
| `exclude` | `list[str]` | `[]` | Glob patterns matched against file paths. Matching files are skipped entirely. |
| `per-file-ignores` | `dict[str, list[str]]` | `{}` | Map of glob pattern → list of rule patterns to suppress for matching files. |

### `CacheSettings`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | `bool` | `true` | Whether the local cache at `.burnt/cache/` is active. |
| `ttl-seconds` | `float` | `3600.0` | How long cached results are considered fresh (seconds). |

### Top-Level Settings

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `workspace-url` | `str \| None` | `None` | Databricks workspace URL. Overridden by `BURNT_WORKSPACE_URL`. Requires `pip install burnt[databricks]`. |
| `token` | `str \| None` | `None` | PAT token. Overridden by `BURNT_TOKEN`. Requires `pip install burnt[databricks]`. |
| `target-currency` | `str` | `"USD"` | Default currency for cost estimates. `USD` or `EUR`. |
| `pricing-source` | `str` | `"static"` | How to fetch pricing data. `static` uses shipped rate tables; `api` queries live cloud pricing APIs (requires the relevant `[*-databricks]` extra). |

---

## CLI Flags

All `burnt check` flags that affect rule selection use the same pattern syntax as the config file.

| Flag | Description |
|------|-------------|
| `--select PATTERN` | Override config `select`. Accepts exact ID, prefix, tag, or `ALL`. Repeatable. |
| `--ignore PATTERN` | Suppress rules matching pattern. Repeatable. |
| `--extend-select PATTERN` | Add rules on top of config `select`. Repeatable. |
| `--extend-ignore PATTERN` | Add rules to suppress on top of config `ignore`. Repeatable. |
| `--fail-on LEVEL` | Override `fail-on` for this run (`info` \| `warning` \| `error`). |
| `--output FORMAT` | Output format: `table` (default), `text`, `json`. |

CLI flags are merged with config: `--ignore` and `--extend-ignore` are treated
as additions to the config `extend-ignore` list. `--select` replaces the config
`select` for that run.

### Examples

```bash
# Suppress one rule
burnt check ./src/ --ignore BP008

# Suppress all PySpark rules
burnt check ./src/ --ignore pyspark

# Run only SQL rules
burnt check ./src/ --select BQ --select SQ

# Only fail on errors (not warnings)
burnt check ./src/ --fail-on error

# JSON output for CI
burnt check ./src/ --output json
```

---

## Environment Variables

### `BURNT_*` variables

| Variable | Maps to | Description |
|----------|---------|-------------|
| `BURNT_WORKSPACE_URL` | `settings.workspace_url` | Databricks workspace URL |
| `BURNT_TOKEN` | `settings.token` | Personal Access Token |
| `BURNT_TARGET_CURRENCY` | `settings.target_currency` | Default currency (`USD` / `EUR`) |
| `BURNT_PRICING_SOURCE` | `settings.pricing_source` | `api` or `static` |

### Databricks SDK variables

| Variable | Used when | Description |
|----------|-----------|-------------|
| `DATABRICKS_RUNTIME_VERSION` | Checked first | Set automatically inside Databricks clusters. When present, `SparkBackend` is used; no credentials needed. |
| `DATABRICKS_HOST` | External access | Workspace URL for `RestBackend`. |
| `DATABRICKS_TOKEN` | SDK auth | PAT for the Databricks SDK. |
| `DATABRICKS_CLIENT_ID` | SDK auth | OAuth client ID for service principals. |
| `DATABRICKS_CLIENT_SECRET` | SDK auth | OAuth client secret for service principals. |

---

## Settings Precedence

From highest to lowest priority:

1. **CLI flags** (`--select`, `--ignore`, `--fail-on`, `--output`) — override for that run
2. **Inline comments** (`# burnt: ignore[...]`) — suppress individual findings
3. **`BURNT_*` environment variables** — override config file values
4. **`.burnt.toml` or `pyproject.toml [tool.burnt]`** — project-level config
5. **Built-in defaults**

---

## Pricing Backend Selection

When multiple pricing-backend extras are installed, tell burnt which to use:

```toml
[burnt.pricing]
backend = "azure-databricks"   # azure-databricks | aws-databricks | gcp-databricks | onprem-spark
                               # default: auto (single installed backend wins; error if ambiguous)
```

**Env-var override:** `BURNT_PRICING_BACKEND=azure-databricks`

If only one pricing extra is installed, `backend` is optional — burnt uses it automatically. If
multiple are installed and `backend` is not configured, `burnt check` exits with a config error
listing the installed backends and asking you to pick one.

---

## On-Prem / Self-Hosted Spark Rates

For self-hosted Spark on Kubernetes, YARN, or standalone. Requires `pip install burnt[onprem-spark]`.
No cloud SDK dependency — dollar estimates come entirely from the rates you configure here.

```toml
[burnt.onprem_spark]
cost_per_vcpu_hour   = 0.048   # $/vCPU-hour  (required)
cost_per_gb_hour     = 0.006   # $/GB-hour memory  (required)
cost_per_gb_shuffle  = 0.001   # $/GB shuffled  (required)
```

**Env-var overrides:**

| Variable | Maps to |
|----------|---------|
| `BURNT_ONPREM_SPARK_COST_PER_VCPU_HOUR` | `onprem_spark.cost_per_vcpu_hour` |
| `BURNT_ONPREM_SPARK_COST_PER_GB_HOUR` | `onprem_spark.cost_per_gb_hour` |
| `BURNT_ONPREM_SPARK_COST_PER_GB_SHUFFLE` | `onprem_spark.cost_per_gb_shuffle` |

All three rates are required when `[onprem-spark]` is the active backend. `burnt check` exits
with a config error if any rate is missing.

---

## Databricks System Tables

*Requires `pip install burnt[databricks]`. System tables are an enrichment — burnt never errors if
they are unavailable; it logs once at INFO and falls back to other data sources.*

System-table paths can be overridden when your org mirrors them into private schemas, or when future
Unity Catalog versions relocate them.

```toml
[burnt.databricks.system_tables]
# Master switch — set false to skip system-table queries entirely even when [databricks] is installed.
enabled = true

# Defaults shown; override only what you need.
# query_history             = "system.query.history"
# billing_usage             = "system.billing.usage"
# list_prices               = "system.billing.list_prices"
# information_schema_tables = "system.information_schema.tables"
# compute_clusters          = "system.compute.clusters"
# node_timeline             = "system.compute.node_timeline"
```

**Env-var overrides** (follow existing `BURNT_*` prefix convention):

| Variable | Table |
|----------|-------|
| `BURNT_DATABRICKS_SYSTEM_TABLES_QUERY_HISTORY` | `query_history` |
| `BURNT_DATABRICKS_SYSTEM_TABLES_BILLING_USAGE` | `billing_usage` |
| `BURNT_DATABRICKS_SYSTEM_TABLES_LIST_PRICES` | `list_prices` |
| `BURNT_DATABRICKS_SYSTEM_TABLES_INFORMATION_SCHEMA_TABLES` | `information_schema_tables` |
| `BURNT_DATABRICKS_SYSTEM_TABLES_COMPUTE_CLUSTERS` | `compute_clusters` |
| `BURNT_DATABRICKS_SYSTEM_TABLES_NODE_TIMELINE` | `node_timeline` |

**What system tables are used for** (four narrow, bounded uses — no fleet-wide mining):

| Use | Table | Purpose |
|-----|-------|---------|
| DBU rate lookup | `list_prices` | Replaces shipped JSON of DBU rates for orgs that prefer live data |
| Table-size enrichment | `information_schema_tables` | Fills `estimated_input_bytes` from ground truth without per-table `DESCRIBE DETAIL` |
| Cluster profile resolution | `compute_clusters` | Resolves `cluster_id` in `burnt.toml` to node type + size |
| Last-run observed cost | `query_history` | Opt-in: show "your last actual run cost $X" in-notebook |

**Behaviour rules:**
1. If a configured table is unreadable (permissions error, doesn't exist), burnt logs once at `INFO` and falls back to its non-system-table path (shipped JSON / `DESCRIBE DETAIL` / SDK call). It never errors.
2. Paths are resolved once per `burnt.check()` call and cached on the result.
3. `burnt doctor` reports which system tables are reachable for the current config, so you can verify access before relying on enrichment.

---

## TableRegistry (Programmatic)

For programmatic use when you need to override table paths in code rather than config:

```python
from burnt.core.table_registry import TableRegistry
import burnt

registry = TableRegistry(
    billing_usage="governance.cost_management.v_billing_usage",
    query_history="governance.cost_management.v_query_history",
)

result = burnt.check(registry=registry)
```

| Key | Default table |
|-----|---------------|
| `billing_usage` | `system.billing.usage` |
| `billing_list_prices` | `system.billing.list_prices` |
| `query_history` | `system.query.history` |
| `compute_node_types` | `system.compute.node_types` |
| `compute_clusters` | `system.compute.clusters` |
| `compute_node_timeline` | `system.compute.node_timeline` |
| `lakeflow_jobs` | `system.lakeflow.jobs` |
| `lakeflow_job_run_timeline` | `system.lakeflow.job_run_timeline` |

The `[burnt.databricks.system_tables]` config section (above) is the preferred approach for
static configuration; `TableRegistry` is for dynamic overrides in application code.
