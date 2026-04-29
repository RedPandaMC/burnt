```yaml
id: P3-06-rule-count-reconciliation
status: todo
phase: 3
priority: medium
agent: ~
blocked_by: []
created_by: planner
```

## Context

### Goal

Make `burnt rules | wc -l` match what README and docs claim. Currently the filesystem
has 43 Tier 1 TOML rule files, but Tier 2 and Tier 3 rules live only in Rust source
(no TOML stubs), so they are invisible to `burnt rules`. The fix: give every Tier 2 and
Tier 3 rule a TOML stub so the TOML directory is the single source of truth for rule
count and metadata.

### Background

- Tier 1 rules: already have TOML + tree-sitter query files.
- Tier 2/3 rules: implemented in Rust, no TOML. They appear in `burnt check` output but
  not in `burnt rules`.
- `burnt rules` currently reads from the TOML directory only.
- Target: every rule has a TOML stub (even if `query = ""` for Tier 2/3).

### Files to modify

```
src/burnt-engine/rules/**/*.toml   (add stubs for Tier 2/3 rules)
docs/anti-pattern-rules.md         (update if count changes from 43)
README.md                           (update if count changes)
```

---

## Acceptance Criteria

- [ ] `burnt rules | wc -l` matches the count stated in README.md
- [ ] Every rule ID in `burnt check` output is also in `burnt rules` output
- [ ] `docs/anti-pattern-rules.md` and `README.md` both show the same number
- [ ] The reconciled count is documented in a comment at the top of `docs/anti-pattern-rules.md`

## Verification

```bash
burnt rules | wc -l
burnt check tests/fixtures/ 2>&1 | grep -oP '\b[A-Z]+\d+\b' | sort -u
# Both lists should match
```
