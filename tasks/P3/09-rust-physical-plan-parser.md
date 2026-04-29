```yaml
id: P3-09-rust-physical-plan-parser
status: todo
phase: 3
priority: medium
agent: ~
blocked_by: [P2-02]
created_by: planner
```

## Context

### Goal

Parse the Spark physical query plan JSON from the REST API `/sql/{id}` endpoint inside
the `burnt-engine` Rust crate, and emit annotated CostGraph nodes that describe
what Catalyst actually chose to execute (joins, shuffles, scans, aggregations).

### Background

After P2-02 (REST session client), `state.collected` contains raw stage data. But the
`/api/v1/applications/{app_id}/sql` endpoint also returns query execution plans as a
JSON tree (`physicalPlanDescription`). Parsing this in Rust and cross-linking to the
CostGraph nodes gives significantly richer cost attribution than stage metrics alone.

**Plan tree structure (simplified):**
```json
{
  "id": 42,
  "description": "== Physical Plan ==\nProject ...",
  "nodes": [
    {"nodeId": 1, "nodeName": "Sort", "metrics": [...], "children": [2]},
    {"nodeId": 2, "nodeName": "Exchange", "metrics": [...], "children": [3]}
  ]
}
```

**What the parser should emit per node:**
- `node_id`, `node_name`, `parent_id`
- Key metrics: `number of output rows`, `data size`, `shuffle write size`
- A mapping from `nodeId` → `CostGraph node id` (matched by rule code + source location)

### Files to modify

```
src/burnt-engine/src/plan_parser.rs   (new)
src/burnt-engine/src/lib.rs           (expose parse_physical_plan via PyO3)
src/burnt/core/session_cost.py        (call parse_physical_plan on SQL result bodies)
tests/fixtures/plans/                 (sample plan JSON fixtures)
```

---

## Acceptance Criteria

- [ ] `parse_physical_plan(json_str) -> list[PlanNode]` is callable from Python via PyO3
- [ ] `PlanNode` has fields: `node_id: int`, `node_name: str`, `parent_id: int | None`,
  `metrics: dict[str, Any]`
- [ ] CostGraph nodes for shuffle operations are annotated with `shuffle_write_bytes` from
  the matched plan node
- [ ] Parsing an unknown or empty plan string returns `[]` (no panic, no error)
- [ ] Unit tests in Rust cover: sort node, exchange node, scan node, empty input
- [ ] `cargo test plan_parser` passes

## Verification

```bash
cargo test -p burnt-engine plan_parser
uv run pytest tests/unit/test_plan_parser.py -v
```
