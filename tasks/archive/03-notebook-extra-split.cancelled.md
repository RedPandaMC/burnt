> **CANCELLED** — April 2026 pivot. See tasks/README.md for rationale.

# Task: [notebook] extra split

---

## Metadata

```yaml
id: PN-03-notebook-extra-split
status: todo
phase: N
priority: medium
agent: ~
blocked_by: [PN-01-pyproject-extras-restructure]
created_by: planner
```

---

## Context

### Goal

Gate HTML rendering behind the `[notebook]` extra and add a `.dbc` archive parser to that same extra. Today `display/notebook.py` is always installed and pulls in Jinja2 as an implicit dependency, which adds weight to the core install. Splitting it out preserves the "core install is small" promise: `pip install burnt` gives you 84 rules + compute-seconds via a Rich terminal table, nothing more.

### Files to read

```
# Required
src/burnt/display/notebook.py
src/burnt/display/terminal.py
src/burnt/__init__.py
src/burnt/core/exceptions.py
pyproject.toml

# Reference
docs/modular-architecture.md   §2.3
DESIGN.md §9 Display
```

### Background

**Graceful degradation contract:**

- `result.to_html()` without `[notebook]` → raises `BurntError("'.to_html()' requires pip install burnt[notebook]")`
- `result.display()` in a Jupyter environment without `[notebook]` → falls back to the terminal renderer (Rich → text) silently
- `burnt check x.dbc` without `[notebook]` → exits 2 with `BurntError("'.dbc' archives require pip install burnt[notebook]")`
- `.ipynb` parsing continues to work in the core install (no change)

**DBC parser** (`src/burnt/parsers/dbc.py`):

`.dbc` is a ZIP archive. Each notebook inside is a JSON file. Cell content can be plain UTF-8, Base64-encoded, or gzip-compressed + Base64. The parser extracts each cell's source and language, then routes it to the standard notebook parsing pipeline. No new dependencies: stdlib `zipfile`, `json`, `gzip`, `base64` only.

```python
# Rough shape
def parse_dbc(path: str) -> list[NotebookCell]:
    with zipfile.ZipFile(path) as zf:
        for name in zf.namelist():
            if name.endswith(".json"):
                data = json.loads(zf.read(name))
                for cell in data.get("commands", []):
                    yield _decode_cell(cell)
```

**Import gating pattern** — follow the existing pattern in `src/burnt/runtime/__init__.py`:

```python
try:
    from burnt.display._notebook_impl import NotebookRenderer
except ImportError:
    NotebookRenderer = None  # type: ignore[assignment,misc]
```

Then raise `BurntError` at call time if `NotebookRenderer is None`.

---

## Acceptance Criteria

- [ ] `src/burnt/display/notebook.py` is gated: importing it without `jinja2` installed raises `ImportError`, not a syntax error
- [ ] `result.to_html()` raises `BurntError` with a helpful message when `[notebook]` is not installed
- [ ] `result.display()` in a Jupyter kernel falls back to terminal renderer (no exception) when `[notebook]` is not installed
- [ ] `src/burnt/parsers/dbc.py` exists and handles plain UTF-8, Base64, and gzip+Base64 cell content
- [ ] `burnt check notebook.dbc` works when `[notebook]` is installed; exits 2 with a clear message when it is not
- [ ] Core install (`pip install burnt`) has no Jinja2 dependency
- [ ] `uv run pytest -m unit -v` passes
- [ ] `uv run ruff check src/` passes

---

## Verification

### Commands

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
# In a venv without [notebook]:
python -c "from burnt.core.models import CheckResult; r = CheckResult.__new__(CheckResult); r.to_html()"
# Should raise BurntError
```

### Integration Check

- [ ] `pip install burnt` (no notebook extra) → `burnt check ./notebook.py` produces terminal output, no import errors
- [ ] `pip install burnt[notebook]` → `result.to_html()` returns an HTML string

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked by PN-01 (the `[notebook]` extra must exist in pyproject.toml before this gating makes sense).
