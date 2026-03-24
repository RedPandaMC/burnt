# Phase 3: Display & CLI

> `burnt.check().display()` in notebooks. `burnt check` from terminal. Three layouts. Config system. Graceful degradation. < 3 seconds.

**Duration:** 3 weeks
**Depends on:** Phase 2
**Gate:** Renders in mock notebook (3 modes). CLI correct. Config loading works. Degradation at all 4 access levels. < 3s.

---

## Tasks

### P3-01: Notebook HTML Renderer (Week 11)

Rich `Console(record=True)` → HTML → `displayHTML()`.

**Python:** Session banner → cost summary → operation list (node, line, cost, % bar) → findings (severity, code, cost impact) → recommendations + API JSON.

**SQL:** Session banner → statement list (SQL preview, cost) → table dependency chain → maintenance costs → findings → recommendations.

**DLT/SDP:** Pipeline summary (tier, total) → table DAG (tree, per-table cost, kind, expectations) → inner hotspots → findings → recommendations.

### P3-02: Terminal Renderer (Week 11)

Same Rich renderables, console output. Color badges. Unicode cost bars.

### P3-03: Export (Week 11)

`.json()`: mode, summary, per-file details. `.markdown()`: formatted for PR comments.

### P3-04: `burnt.check()` Wiring (Week 12)

Detect env → access level → read notebook (or file) → `burnt_engine.analyze_*()` → check mode → deserialize → enrich → estimate → session cost → recommend → CheckResult.

No path = current notebook (SparkSession). Path = file analysis (REST or Spark). Directory = per-file with per-file mode detection.

### P3-05: Config System (Week 12)

Config discovery: walk up from target path looking for `burnt.toml` → `.burnt.toml` → `pyproject.toml` with `[tool.burnt]` → `~/.config/burnt/burnt.toml`. First file found wins. `pyproject.toml` reads `[tool.burnt]` section and strips prefix — schema identical to `burnt.toml`.

`pydantic-settings` for typed config model. Sections: `[connection]`, `[tables]`, `[check]`, `[watch]`, `[alert]`, `[calibration]`. Env var mapping: `BURNT_CHECK__MAX_COST=50`.

`burnt check --init`: if `pyproject.toml` exists in cwd, ask whether to add `[tool.burnt]` section there or create standalone `burnt.toml`. If no `pyproject.toml`, create `burnt.toml`.

Priority: function args > `burnt.config()` > discovered config file > `~/.config/burnt/burnt.toml` > env vars > defaults.

### P3-06: CLI (Week 12–13)

`burnt check <path>` with flags: `--cluster yaml:target`, `--json`, `--markdown`, `--strict`, `--max-cost`, `--only`, `--skip`. `burnt check --explain [rule]`. `burnt check --init`. `burnt version`.

Reads `burnt.toml`. Flags override config. Exit codes: 0/1/2. < 200 lines.

### P3-07: Graceful Degradation (Week 13)

Every external call wrapped. DESCRIBE fails → heuristic. System tables blocked → monitoring disabled. Dynamic SQL → BN002 + partial graph. Widget names → try default. No creds → `ConnectionRequired`. Each access level: clear message.

### P3-08: Performance (Week 13)

Target < 3s on 50-cell notebook. < 50 MB memory. Batch DESCRIBE calls. Lazy imports.

---

## Gate

- [ ] `burnt.check()` renders in mock notebook (3 modes)
- [ ] Session cost banner
- [ ] Terminal output readable
- [ ] JSON/Markdown export valid
- [ ] Config system: burnt.toml + .burnt.toml + pyproject.toml [tool.burnt] + env vars, priority correct
- [ ] `burnt check --init` offers pyproject.toml integration when pyproject.toml exists
- [ ] CLI flags work, exit codes correct
- [ ] 4 access levels produce output with messages
- [ ] Dynamic SQL → partial analysis
- [ ] < 3s, < 50 MB
