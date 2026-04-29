# Task: --fix, --unsafe-fixes, --diff CLI flags (core)

---

## Metadata

```yaml
id: P3-04-cli-fix-diff-flags
status: todo
phase: 3
priority: high
agent: ~
blocked_by: [P2-03, P3-01]
created_by: planner
```

---

## Context

### Goal

Wire `--fix`, `--unsafe-fixes`, and `--diff <ref>` into `burnt check` as core CLI flags, no extra needed. These were previously discussed as `[fix]` and `[git]` extras but the April 2026 decision folded them into core — matching ruff's shape.

### Files to read

```
# Required
src/burnt/cli/main.py
src/burnt/_check/__init__.py
docs/writing-rules.md
docs/modular-architecture.md   §2.1 (folded-in CLI flags)

# Reference
DESIGN.md §12 CLI
```

### Background

**`--diff <ref>`**:
- Run `git diff --name-only <ref>...HEAD` via `subprocess.run`
- Filter the file list to only those burnt can parse (`.py`, `.sql`, `.ipynb`, `.dbc`)
- Pass the filtered list to `_check.run()` instead of the path argument
- No Python git library; no `[git]` extra
- If `git` is not on PATH → exit 2 with a clear message

**`--fix`**:
- After `_check.run()` collects findings, apply autofixes for all findings where `rule.fix` is not None
- Autofixes are implemented in the Rust engine via `tree-sitter::InputEdit` — no libcst, no Python-side AST manipulation
- The Rust engine returns a `Fix { replacement: String, range: TextRange }` alongside each fixable finding
- Python side applies the replacements in reverse-offset order to avoid position drift
- Show a summary of applied fixes: `"Fixed 3 issues in 2 files"`
- Rules declare autofixability via `[fix]` section in their TOML (see writing-rules.md)

**`--unsafe-fixes`**:
- Superset of `--fix` — also applies fixes marked `unsafe = true` in the rule TOML
- These are fixes that may change observable semantics (e.g. reordering, removing a call)
- Requires explicit opt-in; never applied automatically

**Rule TOML `[fix]` section** (new, additive to existing format):
```toml
[fix]
description = "Replace .collect() with .limit(n).collect()"
unsafe = false   # true means --unsafe-fixes is required
```

Writing-rules.md needs a new section documenting this format.

---

## Acceptance Criteria

- [ ] `burnt check ./notebook.py --diff main` only lints files changed since `main` branch
- [ ] `burnt check ./notebook.py --fix` applies all safe autofixes in-place and reports how many were applied
- [ ] `burnt check ./notebook.py --unsafe-fixes` also applies unsafe fixes
- [ ] `--diff` without git on PATH exits 2 with a message: `"'--diff' requires git to be installed and this directory to be a git repository"`
- [ ] Autofixes use the Rust engine's `Fix { replacement, range }` structs (not libcst, not regex)
- [ ] At least one existing rule has a `[fix]` section added to its TOML as a test case
- [ ] `writing-rules.md` has a new section documenting the `[fix]` TOML format
- [ ] `uv run pytest -m unit -v` passes (add unit tests for diff filtering and fix application)
- [ ] `uv run ruff check src/` passes

---

## Verification

### Commands

```bash
uv run pytest -m unit -v -k "fix or diff"
uv run ruff check src/ tests/
# Smoke test
burnt check ./tests/fixtures/notebooks/ --diff HEAD~1
burnt check ./tests/fixtures/notebooks/ --fix --dry-run   # if dry-run is implemented
```

### Integration Check

- [ ] `burnt check ./notebook.py --diff HEAD` runs without error in a git repo
- [ ] `burnt check ./notebook.py --fix` modifies a test fixture that has a known-fixable rule violation

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked by P2-03 (CLI must be rewired to `_check.run()` before flags can be added) and P3-01 (extras restructure confirms libcst is not used).
