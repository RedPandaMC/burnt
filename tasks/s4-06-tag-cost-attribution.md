# Task: Tag-Based Cost Attribution

---

## Metadata

```yaml
id: s4-06-tag-cost-attribution
status: todo
phase: 4
priority: high
agent: ~
blocked_by: [s4-01-error-handling, s4-04-table-registry]
created_by: planner
```

---

## Context

### Goal

Add `burnt.cost_by_tag()` that aggregates Databricks billing data by a specified cluster
tag key, enabling team-level or project-level cost attribution. The `system.billing.usage`
table has a `custom_tags` map column. This feature makes burnt useful for FinOps teams
who need chargeback reports. The untagged spend percentage is explicitly surfaced as an
actionable finding.

### Files to read

```
# Required
src/burnt/tables/billing.py
src/burnt/core/table_registry.py
src/burnt/core/pricing.py
src/burnt/core/models.py
src/burnt/__init__.py

# Reference
DESIGN.md
tasks/s4-04-table-registry.md
```

### Background

**Module location:** `src/burnt/tables/tags.py`

**Data source:** `system.billing.usage` joined with `system.billing.list_prices`:
- `custom_tags` is a `MAP<STRING, STRING>` column
- Filter: `custom_tags[tag_key] = tag_value` or group by `custom_tags[tag_key]`
- Use `list_prices` for USD amounts (not just DBU counts — rates differ by SKU)

**`TagAttribution` model:**
```python
@dataclass
class TagAttribution:
    tag_key: str
    date_range: tuple[date, date]
    granularity: str                    # "daily" | "weekly" | "monthly" | "total"
    rows: list[TagAttributionRow]
    untagged_cost_usd: float
    untagged_pct: float
    total_cost_usd: float

@dataclass
class TagAttributionRow:
    tag_value: str                      # e.g. "data-eng"
    period: date | None                 # None for granularity="total"
    cost_usd: float
    dbu: float
    sku_breakdown: dict[str, float]     # {"JOBS_COMPUTE": 8000, "ALL_PURPOSE": 4000}
    pct_of_total: float
```

**API surface:**
```python
report = burnt.cost_by_tag(
    tag_key="team",
    days=30,
    granularity="daily",      # daily | weekly | monthly | total
    group_by=["sku_name"],    # optional secondary grouping
    registry=None,            # optional TableRegistry for enterprise view mapping
)
report.display()
report.to_json()  # → dict for BI tool ingestion
```

**Untagged coverage reporting:**

Tag coverage is often incomplete — not every cluster gets tagged, especially dev/interactive
clusters. Include an "untagged" bucket showing the percentage of spend with no value for
the specified tag key.

**Example output:**
```
Team Attribution (last 30 days)
─────────────────────────────────
  data-eng        $12,340  (42%)
  ml-platform      $8,720  (30%)
  analytics        $4,100  (14%)
  ⚠ untagged       $4,180  (14%)  ← 14% of spend has no team tag
```

---

## Acceptance Criteria

- [ ] `src/burnt/tables/tags.py` exists with `cost_by_tag()` function
- [ ] `TagAttribution` and `TagAttributionRow` models in `src/burnt/core/models.py`
- [ ] Queries use actual USD amounts (via `list_prices` join), not just DBU counts
- [ ] Supports `granularity` parameter: `"daily"`, `"weekly"`, `"monthly"`, `"total"`
- [ ] Untagged spend computed and included in result as separate row
- [ ] `TagAttribution.display()` shows table with tag values, costs, percentages, untagged warning
- [ ] `TagAttribution.to_json()` returns serializable dict
- [ ] `TableRegistry` used for system table path resolution
- [ ] `burnt.cost_by_tag()` exported from `src/burnt/__init__.py`
- [ ] Unit tests cover: basic aggregation, granularity options, untagged bucket, to_json
- [ ] All existing tests still pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

### Commands

```bash
uv run pytest -m unit -v -k "tag_attribution or cost_by_tag"
uv run ruff check src/ tests/
```

### Integration Check

- [ ] With mock billing data containing `custom_tags={"team": "data-eng"}` for 80% of rows and null for 20%: `cost_by_tag(tag_key="team")` shows `untagged_pct ≈ 0.20`.

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked on s4-01 (error handling) and s4-04 (table registry for billing path resolution).
