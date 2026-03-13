# Task: burnt doctor — Environment Health Check Command

---

## Metadata

```yaml
id: s2-08-doctor-command
status: todo
phase: 2
priority: high
agent: ~
blocked_by: [s2-05a-cli-api-redesign]
created_by: planner
```

---

## Context

### Goal

Add a `burnt doctor` CLI command that shows the user exactly what burnt sees: Python
version, dependency status, credential status, connection reachability, which config
file is loaded and from which path, and cache state. This is the first command a user
should run when something seems wrong.

### Files to read

```
# Required
src/burnt/cli/main.py
src/burnt/core/config.py          ← Settings.discover() (implemented in s2-05a)
src/burnt/core/exceptions.py

# Reference
tasks/s2-05a-cli-api-redesign.md   ← Settings.discover() spec
```

### Background

Target output format:

```
burnt v0.1.0 environment check
────────────────────────────────────────────────
  Python            3.12.4          OK
  sqlglot           26.2.1          OK
  pydantic          2.7.0           OK
  pydantic-settings 2.3.0           OK
  rich              13.7.0          OK
────────────────────────────────────────────────
  DATABRICKS_HOST   SET             https://adb-123.azuredatabricks.net
  DATABRICKS_TOKEN  SET             (redacted)
  Connection test                   OK  workspace reachable
  system.query.history              OK  accessible
────────────────────────────────────────────────
  Config            /project/.burnt.toml
    workspace-url   https://adb-123.azuredatabricks.net
    lint.fail-on    error
    lint.select     ALL  (12 rules, 2 ignored)
    cache.ttl       300s
  Also found        /project/pyproject.toml [tool.burnt]  (lower priority)
────────────────────────────────────────────────
  Cache             .burnt/cache/  12 files  2.3 MB
────────────────────────────────────────────────
```

**When things are missing:**

```
  DATABRICKS_HOST   NOT SET         ⚠ advise() and live features unavailable
  DATABRICKS_TOKEN  NOT SET         ⚠ advise() and live features unavailable
  Connection test                   SKIP  (credentials not configured)
  system.query.history              SKIP  (credentials not configured)
```

```
  Config            NOT FOUND       ⚠ Using defaults. Run `burnt init` to create .burnt.toml
```

```
  Cache             .burnt/cache/   not found  (run `burnt check` to populate)
```

---

## Part 1: Command scaffold in CLI

In `src/burnt/cli/main.py`, add `doctor` as a Click command:

```python
@cli.command()
def doctor():
    """Check environment health: dependencies, credentials, config, cache."""
    # Implementation here
```

Note: s2-05a should add a stub for this. This task fills in the full implementation.

---

## Part 2: Checks to run

### 2a. Python version

- Use `sys.version_info`. Always shows version. Status: OK.

### 2b. Dependency check

For each required package, attempt `importlib.metadata.version(pkg)`:

| Package | Import name |
|---------|-------------|
| `sqlglot` | `sqlglot` |
| `pydantic` | `pydantic` |
| `pydantic-settings` | `pydantic_settings` |
| `rich` | `rich` |

Status: OK if found, MISSING if `PackageNotFoundError`.

### 2c. Credential check

Read env vars `DATABRICKS_HOST` (also check `DATABRICKS_WORKSPACE_URL`, `BURNT_WORKSPACE_URL`)
and `DATABRICKS_TOKEN` (also check `BURNT_TOKEN`).

Show `SET` + redacted value if present (show scheme + host, redact token after first 6 chars).
Show `NOT SET ⚠` with advisory message if absent.

### 2d. Connection test

Only attempt if both credentials are set. Make a lightweight HTTP request to
`{workspace_url}/api/2.0/clusters/list` with `?limit=1`. Timeout: 5 seconds.

- Success → `OK  workspace reachable`
- Timeout → `TIMEOUT  (5s)  check firewall/network`
- Auth error (401/403) → `AUTH ERROR  check token`
- Other error → `ERROR  <status_code>`

**Do not raise.** Catch all exceptions and show a user-friendly message.

### 2e. `system.query.history` access

Only if credentials set. Make a lightweight query to check access.
Success → `OK  accessible`. Auth/permission error → `NO ACCESS  check workspace permissions`.

**Do not raise.**

### 2f. Config file detection

Call `Settings.discover(cwd=Path.cwd())`. This returns `(path, settings)`.

Display the path. Show key settings:
- `workspace-url` (full value)
- `lint.fail-on`
- `lint.select` + `lint.ignore` — summarize as "ALL (N rules, M ignored)" or "N rules selected, M ignored"
- `cache.ttl-seconds`

If a second config file also exists (e.g., both `.burnt.toml` and `pyproject.toml [tool.burnt]`),
show it as "Also found ... (lower priority)".

If no config found: show `NOT FOUND ⚠` with hint to run `burnt init`.

### 2g. Cache status

Check if `.burnt/cache/` exists in CWD. If yes: count files and sum sizes.
If not found: show "not found" with hint.

---

## Part 3: Output format

Use `rich` if available, otherwise plain text with aligned columns.

- Use green color for `OK`
- Use yellow/orange for `WARN` or `NOT SET`
- Use red for `ERROR` or `MISSING`
- Redact token: show first 6 chars + `...` (e.g., `dapiAB...`)

Exit code: 0 always (doctor is informational, not a check command).

---

## Acceptance Criteria

- [ ] `burnt doctor` runs without error in any environment (no credentials, no config, no cache)
- [ ] Shows Python version and all dependency versions (or MISSING)
- [ ] Shows DATABRICKS_HOST and DATABRICKS_TOKEN status; token value redacted
- [ ] Shows connection test result (or SKIP if credentials not set)
- [ ] Shows which config file is loaded and its full path
- [ ] Shows key config values (workspace_url, lint.fail_on, rules count, cache ttl)
- [ ] Shows "Also found" if both `.burnt.toml` and `pyproject.toml [tool.burnt]` exist
- [ ] Shows "NOT FOUND" with hint if no config exists
- [ ] Shows cache directory file count and total size
- [ ] Exit code is always 0

---

## Verification

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/

# With no credentials or config
burnt doctor

# With .burnt.toml present
echo '[lint]
select = ["ALL"]
ignore = ["python_udf"]' > .burnt.toml
burnt doctor
rm .burnt.toml

# With pyproject.toml
cat >> pyproject.toml << 'EOF'
[tool.burnt]
workspace_url = "https://test"
EOF
burnt doctor
```

### Integration Check

- [ ] `burnt doctor` prints a complete environment report without raising
- [ ] Config path shown matches the actual file discovered on disk

---

## Handoff

### Result

```yaml
status: todo
```

### Blocked reason

Blocked on s2-05a (`Settings.discover()` must be implemented first).
