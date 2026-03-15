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
select   = ["ALL"]          # Run all rules; or list specific rule IDs
ignore   = []               # Rule IDs to suppress globally
fail-on  = "error"          # Exit non-zero at this severity: info | warning | error
exclude  = []               # Glob patterns for paths to skip entirely

[lint.per-file-ignores]
"migrations/*.sql" = ["select_star"]   # Per-file rule suppression

[cache]
enabled     = true
ttl-seconds = 3600.0        # Cache TTL in seconds (default: 1 hour)

# Top-level settings
target-currency = "USD"     # USD | EUR
pricing-source  = "api"     # api | static
```

---

## `pyproject.toml` Equivalent

Identical keys, nested under `[tool.burnt]`:

```toml
[tool.burnt]
target-currency = "USD"
pricing-source  = "api"

[tool.burnt.lint]
select   = ["ALL"]
ignore   = ["cross_join"]
fail-on  = "warning"
exclude  = ["tests/**"]

[tool.burnt.lint.per-file-ignores]
"migrations/*.sql"      = ["select_star"]
"notebooks/explore*.py" = ["toPandas", "collect_without_limit"]

[tool.burnt.cache]
enabled     = true
ttl-seconds = 3600.0
```

Note: TOML keys use kebab-case (`ttl-seconds`, `fail-on`). The Python
`Settings` model accepts snake_case equivalents (`ttl_seconds`, `fail_on`).
`burnt init` writes kebab-case.

---

## Settings Reference

### `LintSettings`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `select` | `list[str]` | `["ALL"]` | Rules to enable. `["ALL"]` enables every rule in the registry. Pass specific rule IDs to enable only those. |
| `ignore` | `list[str]` | `[]` | Rule IDs to suppress. Applied after `select`. |
| `fail-on` | `str` | `"error"` | Exit with code 1 when any issue at or above this severity is found. Values: `info`, `warning`, `error`. |
| `exclude` | `list[str]` | `[]` | Glob patterns matched against file paths. Matching files are skipped entirely. |
| `per-file-ignores` | `dict[str, list[str]]` | `{}` | Map of glob pattern → list of rule IDs to suppress only for matching files. |

### `CacheSettings`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | `bool` | `true` | Whether the local cache at `.burnt/cache/` is active. |
| `ttl-seconds` | `float` | `3600.0` | How long cached results are considered fresh (seconds). |

### Top-Level Settings

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `workspace-url` | `str \| None` | `None` | Databricks workspace URL for the REST client. Overridden by `BURNT_WORKSPACE_URL`. |
| `token` | `str \| None` | `None` | PAT token for the REST client. Overridden by `BURNT_TOKEN`. |
| `target-currency` | `str` | `"USD"` | Default currency for cost estimates. `USD` or `EUR`. |
| `pricing-source` | `str` | `"api"` | How to fetch exchange rates. `api` uses frankfurter.app; `static` uses hardcoded rates. |

---

## Environment Variables

### `BURNT_*` variables (Settings)

Read by the `Settings` pydantic-settings model with the prefix `BURNT_`.
These override the equivalent config file values.

| Variable | Maps to | Description |
|----------|---------|-------------|
| `BURNT_WORKSPACE_URL` | `settings.workspace_url` | Databricks workspace URL for the REST client |
| `BURNT_TOKEN` | `settings.token` | Personal Access Token for the REST client |
| `BURNT_TARGET_CURRENCY` | `settings.target_currency` | Default currency (`USD` / `EUR`) |
| `BURNT_PRICING_SOURCE` | `settings.pricing_source` | `api` or `static` |

### Databricks SDK variables (backend auto-detection)

Read by `auto_backend()` to detect the execution context. These follow the
standard [Databricks unified authentication](https://docs.databricks.com/en/dev-tools/auth/unified-auth.html) convention.

| Variable | Used when | Description |
|----------|-----------|-------------|
| `DATABRICKS_RUNTIME_VERSION` | Checked first | Set automatically inside Databricks clusters and notebooks. When present, `SparkBackend` is used; no credentials needed. |
| `DATABRICKS_HOST` | External access | Workspace URL. When present (without `DATABRICKS_RUNTIME_VERSION`), `RestBackend` is created via the Databricks SDK. |
| `DATABRICKS_TOKEN` | SDK auth | PAT for the Databricks SDK (legacy; OAuth preferred for production). |
| `DATABRICKS_CLIENT_ID` | SDK auth | OAuth client ID for service principals. |
| `DATABRICKS_CLIENT_SECRET` | SDK auth | OAuth client secret for service principals. |

When none of these are set, `auto_backend()` returns `None` and burnt operates
in offline / static-only mode.

---

## TableRegistry (Programmatic)

Enterprise environments often expose system tables through governance views
with custom names. Use `TableRegistry` to remap the tables burnt queries:

```python
from burnt.core.table_registry import TableRegistry
import burnt

# Override individual tables
registry = TableRegistry(
    billing_usage="governance.cost_management.v_billing_usage",
    query_history="governance.cost_management.v_query_history",
)

# Pass to any function that accepts registry=
estimate = burnt.estimate("SELECT ...", registry=registry)
```

### Available table keys

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

---

## Settings Precedence

From highest to lowest priority:

1. **CLI flags** (`--fail-on`, `--ignore-rule`, `--output`) — override everything for that run
2. **`BURNT_*` environment variables** — override config file values
3. **`.burnt.toml` or `pyproject.toml [tool.burnt]`** — project-level config
4. **Built-in defaults** (see tables above)

---

## Config File Discovery

`Settings.discover(cwd)` walks upward from the working directory:

- Checks `.burnt.toml` in the current directory first
- Then checks `pyproject.toml` for a `[tool.burnt]` section
- Moves to the parent directory and repeats
- Stops at the git root (directory containing `.git/`) or `$HOME`

The first config file found is used exclusively — there is no merging across
multiple config files in parent directories.
