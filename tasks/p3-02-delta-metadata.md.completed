# Task: Implement Delta log metadata reader

---

## Metadata

```yaml
id: p3-02-delta-metadata
status: done
phase: 3
priority: medium
agent: claude-sonnet-4-6
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Implement `src/dburnrate/parsers/delta.py` that reads Delta Lake `_delta_log` JSON files to extract table statistics (total size, row count, file count, per-file stats). This provides exact scan-size data without executing the query, feeding into the hybrid estimator. Also wrap `DESCRIBE DETAIL` SQL output parsing.

### Files to read

```
# Required
src/dburnrate/core/models.py
src/dburnrate/core/exceptions.py
src/dburnrate/parsers/sql.py      (style reference)

# Reference
RESEARCH.md   # Section on Delta _delta_log
```

### Background

Delta `_delta_log/` contains JSON files (`0000...json`) with `add` and `remove` actions:

```json
{"add": {"path": "part-00001.parquet", "size": 1234567, "stats": "{\"numRecords\": 1000, \"minValues\": {...}, \"maxValues\": {...}, \"nullCount\": {...}}"}}
```

Key fields:
- `add.size` — file size in bytes
- `add.stats` (JSON string) — `numRecords`, `minValues`, `maxValues`, `nullCount`
- `remove` actions: file was deleted (don't count toward current size)

`DESCRIBE DETAIL` output (SQL response) contains:
- `location`, `numFiles`, `sizeInBytes`, `partitionColumns`

Implementation notes:
- Read `_delta_log/*.json` files, parse `add`/`remove` to get current file set
- Current files = all `add` paths minus all `remove` paths (by path)
- Sum `size` of current files for total table size
- Parse `stats` JSON string for per-file record counts
- Return `DeltaTableInfo` Pydantic model

---

## Acceptance Criteria

- [ ] `src/dburnrate/parsers/delta.py` exists
- [ ] `DeltaTableInfo` Pydantic model in `src/dburnrate/core/models.py`:
  - `total_size_bytes: int`, `num_files: int`, `num_records: int | None`
  - `partition_columns: list[str]`, `location: str`
- [ ] `read_delta_log(log_dir: Path) -> DeltaTableInfo` implemented
- [ ] `parse_describe_detail(rows: list[dict]) -> DeltaTableInfo` implemented
- [ ] Correctly handles `remove` actions (deleted files not counted)
- [ ] Handles missing `stats` field gracefully (`num_records = None`)
- [ ] `ParseError` raised if `_delta_log` directory not found or no JSON files
- [ ] Unit tests in `tests/unit/parsers/test_delta.py` using `tmp_path` fixture
- [ ] Tests cover: basic log, log with removes, missing stats, empty log
- [ ] `uv run pytest -m unit -v` passes
- [ ] `uv run ruff check src/ tests/` exits 0

---

## Verification

### Commands (run all, in order)

```bash
uv run pytest -m unit -v tests/unit/parsers/test_delta.py -v
uv run pytest -m unit -v 2>&1 | tail -3
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

---

## Handoff

### Result

Implementation complete. All acceptance criteria met:

- `DeltaTableInfo` model added to `src/dburnrate/core/models.py` with fields: `location`, `total_size_bytes`, `num_files`, `num_records`, `partition_columns`
- `src/dburnrate/parsers/delta.py` created with `read_delta_log()`, `parse_describe_detail()`, and `_parse_partition_columns()`
- `tests/unit/parsers/test_delta.py` created with 18 tests covering: basic add, add+remove, stats summing, partial stats (num_records=None), missing directory, empty directory, malformed JSON lines, multiple log files, `parse_describe_detail` normal/empty/missing-fields, `_parse_partition_columns` list/string/empty-string/None/empty-list cases
- Removed `from __future__ import annotations` to avoid ruff TC003 false positive (Path is used at runtime)
- Test results: 18/18 passed for new tests; 210/210 passed for full unit suite
- `uv run ruff check src/ tests/` exits 0
- `uv run ruff format --check src/ tests/` exits 0

### Blocked reason

N/A
