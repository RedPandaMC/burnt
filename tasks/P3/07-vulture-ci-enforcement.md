```yaml
id: P3-07-vulture-ci-enforcement
status: todo
phase: 3
priority: low
agent: ~
blocked_by: [P2-01b]
created_by: planner
```

## Context

### Goal

Add `vulture` to CI so deleted dead code stays deleted. After the April 2026 pivot
removed watch/, alerts/, intelligence/ (P2-01b), vulture will catch any re-introduction
of unused imports or dead functions automatically.

### Files to modify

```
.github/workflows/ci.yml    (add vulture step)
pyproject.toml              (vulture is already in lint group; add config if needed)
```

### Background

`vulture` is already in the `lint` dependency group in `pyproject.toml`. It just isn't
wired to CI. The tool already has a `[tool.vulture]` section pending or can use the
default config.

Add it as a non-blocking step initially (warn-only) then promote to blocking after the
first clean run confirms no false positives.

---

## Acceptance Criteria

- [ ] CI runs `uv run vulture src/ --min-confidence 80`
- [ ] The step fails on dead imports or functions with confidence ≥ 80%
- [ ] Any legitimate false positives are suppressed with `# noqa: vulture` comments, not
  by lowering the threshold
- [ ] `uv run vulture src/ --min-confidence 80` passes locally on a clean checkout

## Verification

```bash
uv run vulture src/ --min-confidence 80
```
