# Task: PricingBackend protocol extraction

---

## Metadata

```yaml
id: P3-02-pricing-backend-protocol
status: todo
phase: 3
priority: high
agent: ~
blocked_by: [P3-01]
created_by: planner
```

---

## Context

### Goal

Extract a formal `PricingBackend` protocol to `src/burnt/core/pricing.py` per `docs/modular-architecture.md` §3. This creates the explicit boundary that makes "Cost Compiler for Spark" an architectural claim rather than just a tagline. Without this protocol, pricing is co-mingled with Databricks-specific code paths and there is no extensible seam for cloud-specific extras to plug into.

Additionally, stub out the `src/burnt/cloud/` directory tree so Phase 4 (and future cloud extras) have a clear home. Move any existing `DatabricksPricingBackend` implementation stub out of core into `src/burnt/cloud/azure_databricks/` (stub only — actual pricing data files are added in P4).

### Files to read

```
# Required
src/burnt/core/
src/burnt/databricks/
src/burnt/runtime/backend.py
docs/modular-architecture.md   §3, §2.4

# Reference
DESIGN.md §4 Architecture, §10 Pricing Backends
```

### Background

**Protocol shape** (from modular-architecture.md §3):

```python
# src/burnt/core/pricing.py
from typing import Protocol
from burnt.core.models import CostGraph, CostEstimate

class PricingBackend(Protocol):
    name: str                                        # "azure-databricks", "onprem-spark", ...
    def map(self, graph: CostGraph) -> CostEstimate: # compute-seconds → $$
        ...
```

Core ships nothing pricing-shaped by default. Without a pricing extra, `result.cost_estimate.usd` is `None` and only compute-seconds are reported.

**Directory stubs** to create (empty `__init__.py` only — implementations come in P4):

```
src/burnt/cloud/
├── __init__.py
├── azure_databricks/
│   └── __init__.py      # will contain: AzureDatabricksPricingBackend
├── aws_databricks/
│   └── __init__.py      # will contain: AwsDatabricksPricingBackend
├── gcp_databricks/
│   └── __init__.py      # will contain: GcpDatabricksPricingBackend
└── onprem_spark/
    └── __init__.py      # will contain: OnPremSparkPricingBackend
```

The `[databricks]` extra (`src/burnt/databricks/`) is the workspace API client + system-table reader. It does NOT implement `PricingBackend`. Any existing `DatabricksPricingBackend` stub in core should be moved to `src/burnt/cloud/azure_databricks/__init__.py` as a stub with a `NotImplementedError` body.

The `runtime/` auto-detect logic picks a backend based on what's installed and what credentials are present. When multiple backends are installed, the user selects via `burnt.toml` (`[burnt.pricing] backend = "azure-databricks"`).

---

## Acceptance Criteria

- [ ] `src/burnt/core/pricing.py` exists and defines `PricingBackend` as a `Protocol` with `name: str` and `map(graph: CostGraph) -> CostEstimate`
- [ ] `src/burnt/cloud/__init__.py` exists (may be empty)
- [ ] `src/burnt/cloud/{azure_databricks,aws_databricks,gcp_databricks,onprem_spark}/__init__.py` exist (stubs)
- [ ] No `PricingBackend` implementation lives in `src/burnt/core/` or `src/burnt/databricks/`
- [ ] `from burnt.core.pricing import PricingBackend` works in a clean install
- [ ] `uv run pytest -m unit -v` passes
- [ ] `uv run ruff check src/` passes

---

## Verification

### Commands

```bash
uv run python -c "from burnt.core.pricing import PricingBackend; print(PricingBackend)"
uv run python -c "import burnt.cloud.azure_databricks; print('cloud stubs ok')"
uv run pytest -m unit -v
uv run ruff check src/ tests/
```

### Integration Check

- [ ] `burnt check ./examples/notebook.py` still works end-to-end (pricing protocol addition is additive — no regressions)

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked by P3-01 (pyproject restructure) — extras must be in place before the cloud package dirs can be properly gated.
