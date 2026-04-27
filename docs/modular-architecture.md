# burnt — Modular Facets Architecture

> Architecture document. Realigns the package's identity and module/extras boundaries so subsequent task work (PX, P2–P6) executes against a coherent shape. Companion to `DESIGN.md` (technical spec) and `README.md` (user-facing pitch).

---

## Context

burnt's April 2026 pivot removed the "crystal-ball" pre-execution surface (`advise`, `tutorial`, `simulate`, the recommendations engine, the feedback-loop calibrator) and re-anchored the package on three modes: CLI lint, in-notebook coaching, CI gate. The Rust engine + sparkMeasure runtime + optional Databricks `$$` backend is the spine.

The pivot was correct, but the **identity that ships in `README.md`/`DESIGN.md` is "Cost Compiler for Databricks"**, while the technical reality is mostly Spark-generic with Databricks as one (currently the only) cost backend. Data engineers running Spark anywhere — EMR, Glue, Dataproc, on-prem, Databricks — have the same need: see what each pipeline costs, before and after running it. The package today is closer to serving that audience than its README admits.

This document re-states burnt as a **modular facets package** under a Spark-generic identity, codifies which extras exist (and what they're for), and names the future extras as named placeholders.

Decisions recorded here so the next session doesn't re-litigate:

- **Identity:** Spark-generic, Databricks is one backend among several planned.
- **New facet:** `[notebook]` (HTML rendering split out + `.dbc` archive parsing added).
- **Promoted to core:** `sparkmeasure` (was `[spark]`) — runtime metric capture is fundamental to the "honest confidence" pitch.
- **Folded into core CLI:** `--fix` / `--unsafe-fixes` (was `[fix]`) and `--diff <ref>` (was `[git]`) — match ruff's shape.
- **Removed:** `[sql]` (Rust engine replaces sqlglot), `[alerts]` (out of mission — orchestration concern), and the `[chargeback]` / `[sample]` / `[bench]` / `[catalog]` candidates (not good fits).
- **Pricing-backend naming:** `[azure-databricks]` / `[aws-databricks]` / `[gcp-databricks]` / `[onprem-spark]`. Every `[*-databricks]` extra auto-pulls `[databricks]` (workspace API + system tables); `[onprem-spark]` is self-contained.
- **System tables:** four bounded uses (DBU rate lookup, table-size enrichment, cluster profile resolution, opt-in last-run cost), with config-driven path overrides for orgs that mirror system tables. See §3.5.

---

## 1. Re-stated Identity

**Tagline (proposed):** *"Cost Compiler for Spark. Per-operation, per-table, per-dollar."*

**One-liner:** burnt helps data engineers build cost-effective Spark pipelines and see what each pipeline costs — statically before it runs, with live metrics while it runs, and as a CI gate before it ships.

**Audience:** anyone running production Spark — Databricks notebooks today, EMR/Glue/Dataproc/on-prem next.

**What stays Databricks-shaped (and is honest about it):**

- DLT/SDP rules (`SDP*`) and Delta rules (`BD*`) only fire when DLT/Delta usage is detected. They're shipped in the core lint catalog but are no-ops on non-Databricks code.
- Dollar mapping today comes from the Databricks `$$` backend only. Compute-seconds is the universal unit; dollars are a backend mapping layer that any cloud-specific extra can implement.

**What this re-statement does not change:**

- The pivot stands. No reintroduction of `advise`/`simulate`/recommendations engine/feedback loop.
- "Compute-seconds over dollars" remains the core unit.
- "Honest confidence" (observed ≠ estimated) remains a design principle.

---

## 2. Facets Architecture

### 2.1 Always-installed core (`pip install burnt`)

| Component | Path | Role |
|---|---|---|
| Rust lint engine + tree-sitter | `src/burnt-engine/` (PyO3 wheel) | 84 rules, CostGraph construction. Always works, no creds. |
| Check orchestration | `src/burnt/_check/` | `check()` entry, CheckResult model, finding aggregation |
| Config | `src/burnt/_config/` | `burnt.toml` / `[tool.burnt]` discovery |
| Data models | `src/burnt/core/` | `Finding`, `CheckResult`, `CostEstimate`, `CostGraph`, `CostNode` |
| CLI | `src/burnt/cli/main.py` | `check`, `rules`, `init`, `doctor`, `cache` |
| Terminal display | `src/burnt/display/terminal.py` | Rich tables (CLI default) |
| Export | `src/burnt/display/export.py` | JSON, Markdown, SARIF 2.1.0 |
| Parsers (text formats) | `src/burnt/parsers/` (Python wrapper of Rust) | `.py`, `.sql`, `.ipynb` |
| Instance/scaling catalog | `src/burnt/catalog/` | Backend-agnostic lookup tables (kept generic; cloud-specific data lives in the cloud extras) |

**Core dependencies:** `pydantic`, `pydantic-settings`, `typer`, `rich`, `pyyaml`, `tabulate`, **`sparkmeasure>=2.0`** (promoted from the old `[spark]` extra — runtime metric capture is core to the "honest confidence" story and ships always).

**Core CLI capabilities (folded in, no extras needed):**

- `burnt check --fix` / `burnt check --unsafe-fixes` — autofix subset of lint findings in place, ruff-style. Each rule declares whether it's autofixable; `--unsafe-fixes` opts into fixes that may change semantics. No `[fix]` extra; `LibCST` for Python rewrites is a core dep.
- `burnt check --diff <ref>` — diff-aware lint, only files changed since `<ref>`. Shells out to `git diff --name-only`; no Python git library, no `[git]` extra.

**Core promise:** zero credentials, zero network, zero cloud SDK. `burnt check ./notebook.py` produces all 84 lint findings + a CostGraph in compute-seconds. This is the contract that makes burnt safe to install in any environment.

### 2.2 Existing extras — kept, retasked, or removed

| Extra | Status | Role under new identity |
|---|---|---|
| `[sql]` | **Removed.** | `sqlglot` is replaced by the Rust engine's tree-sitter SQL coverage. No optional SQL parser is needed; what tree-sitter doesn't catch, the engine grows native support for. One fewer dep, one fewer failure mode. |
| `[spark]` | **Removed (promoted to core).** | `sparkmeasure>=2.0` ships as a core dependency. Runtime metric capture is fundamental to the "honest confidence" pitch — gating it behind an extra was wrong. |
| `[alerts]` | **Removed.** | Out of mission. burnt is a linter + cost analyser; result dispatch (Slack, webhooks, email) is an orchestration concern that lives in CI, dbt, Airflow, or the user's own scripts consuming `result.to_json()`. No `slack-sdk` dependency. |
| `[databricks]` | **Kept, repositioned as base.** `databricks-sdk>=0.50,<1`, `requests`. | Workspace API client + system-table reader (see §3.5). Does **not** ship a `PricingBackend` itself; pricing data lives in the cloud-specific `[*-databricks]` extras (DBU rates differ across Azure/AWS/GCP). Auto-installed as a transitive dep of any `[*-databricks]` extra — users install `[azure-databricks]` and get `[databricks]` for free. |
| `[all]` | meta | `burnt[notebook,databricks,azure-databricks,aws-databricks,gcp-databricks,onprem-spark]`. |

### 2.3 New facet: `[notebook]` — HTML rendering + `.dbc` archive support

Why a separate extra: HTML rendering carries template/asset weight that CLI users don't need, and `.dbc` parsing requires a ZIP/encoding pipeline that's dead weight for `.py`/`.sql`-only users. Splitting it out preserves the "core install is small" promise from `DESIGN.md`.

**Contents:**

| Piece | Path (proposed) | Purpose |
|---|---|---|
| HTML renderer (move from core) | `src/burnt/display/notebook.py` | Already exists in core; this extra owns its dependencies (Jinja2 + minimal CSS). Core retains a stub that raises `NotAvailableError("pip install burnt[notebook]")`. |
| `.dbc` parser (new) | `src/burnt/parsers/dbc.py` | ZIP archive → JSON notebooks. Supports plain UTF-8, Base64, and gzip-compressed cell content. |
| Cell-magic router | already in `parsers/` | Routes `%sql` → SQL parser, `%python` → Python AST/tree-sitter, `%scala` → unsupported (skip with note). Lives in core; `[notebook]` only adds `.dbc` upstream. |

**Dependencies:** `jinja2>=3,<4` (templating). `.dbc` uses stdlib `zipfile`, `json`, `gzip`, `base64` — no new deps.

**Public API:**

```python
result = burnt.check("notebook.dbc")     # works only if [notebook] installed for .dbc
result.display()                          # in Jupyter: HTML rendering only if [notebook] installed
result.to_html()                          # raises NotAvailableError without [notebook]
```

**Graceful degradation:**

- `.dbc` without `[notebook]` → `BurntError("'.dbc' archives require pip install burnt[notebook]")`. CLI exits 2 (config/parse error).
- `.ipynb` continues to work in the core install.
- Notebook-rendered `display()` in Jupyter falls back to the terminal renderer (Rich → text) if `[notebook]` is absent.

### 2.4 Future extras — named placeholders only

Two groups. The pricing backends are **structural** — required by the Spark-generic identity choice. The rest are **candidate facets** chosen for buildability and direct alignment with "help DEs build cost-effective Spark pipelines + cost transparency".

#### Pricing backends (structural)

Pricing is split per cloud × runtime so each combination ships exactly the data and SDKs it needs. Every `[*-databricks]` extra **auto-pulls `[databricks]`** as a transitive dependency (workspace API + system tables); `[onprem-spark]` is fully self-contained.

| Extra | Auto-pulls | Role | Buildability anchor |
|---|---|---|---|
| `[azure-databricks]` | `[databricks]` | `PricingBackend` for Databricks on Azure. Azure DBU rates × Azure VM SKU prices × cluster profile lookup. | DBU rates published by Databricks (small JSON refreshed in CI) + Azure Retail Prices API (public, no auth) for VM SKU prices. Optional `azure-identity` only if the workspace requires AAD auth. |
| `[aws-databricks]` | `[databricks]` | `PricingBackend` for Databricks on AWS. AWS DBU rates × EC2 instance prices × cluster profile lookup. | DBU JSON + AWS Pricing API (`boto3` `pricing` client) or shipped EC2 price tables refreshed in CI. |
| `[gcp-databricks]` | `[databricks]` | `PricingBackend` for Databricks on GCP. GCP DBU rates × GCE machine type prices. | DBU JSON + GCP Cloud Billing Catalog API (`google-cloud-billing`). |
| `[onprem-spark]` | nothing | `PricingBackend` for self-hosted Spark on Kubernetes / YARN / standalone, driven by user-supplied `$/vCPU-hour`, `$/GB-hour`, `$/GB-shuffle` in `burnt.toml`. | Pure config + arithmetic. Zero dependencies. Highest reach for the non-Databricks Spark population. |
| (future) `[aws-emr]`, `[aws-glue]`, `[gcp-dataproc]` | nothing | Native cloud Spark runtimes outside Databricks. | Same pricing-API shape as the `[*-databricks]` extras minus the DBU layer. Reserved as named slots. |

The naming pattern (`<cloud>-<runtime>`) makes it explicit what each extra knows about and prevents the ambiguity of a single `[aws]` that conflates EMR / Glue / Databricks-on-AWS pricing.

#### Candidate facets

Six prior candidates are retired:

- `[fix]` — folded into `burnt check --fix` / `--unsafe-fixes` (core). See §2.1.
- `[git]` — folded into `burnt check --diff <ref>` (core). See §2.1.
- `[chargeback]`, `[sample]`, `[bench]`, `[catalog]` — dropped as bad fits (chargeback drifts toward fleet-FinOps; sample/bench overlap the runtime story without adding a clean signal; catalog requires too many divergent adapters for one extra).

Remaining candidates (explicitly named, design deferred):

| Extra | What it does | Buildable because… | New signal | Pivot-safe? |
|---|---|---|---|---|
| `[iceberg]` | Reads Apache Iceberg table metadata (manifest lists, partition specs, snapshot history) directly from object storage to fill `estimated_input_bytes` and partition counts on CostGraph nodes whose `tables_referenced` resolve to Iceberg tables. | `pyiceberg` (Apache project, Python-native, stable) reads metadata without a Spark/Trino runtime. | Ground-truth table size + partition layout for the Iceberg user base — closes the same gap a future `[delta]` would close for Delta. | Yes — descriptive metadata read. |
| `[mlflow]` | When an MLflow run is active, attaches CheckResult's compute-seconds and top findings as MLflow tags + metrics on the run, so ML experiments record cost alongside accuracy. Optional: log SARIF as a run artifact. | `mlflow` SDK exposes `log_metric` / `set_tag` / `log_artifact`. CheckResult already serialises. | Surfaces cost to ML platform teams in the tool they're already in. | Yes — passive logging of observed values. |

Anything not on this list (e.g. `[snowflake]`/`[bigquery]` pricing, `[secret-scan]`, hosted server UI, fleet FinOps dashboards, scheduling/budget enforcement, retrospective system-table mining) is **explicitly off-mission** — burnt is Spark-shaped and per-pipeline; those belong in different tools.

---

## 3. Cross-cutting: the `PricingBackend` protocol

Today's `runtime/` already has a `Backend` protocol, but pricing is co-mingled with the Databricks-specific code paths. The Spark-generic identity needs an explicit boundary.

**Conceptual shape:**

```python
# src/burnt/core/pricing.py
class PricingBackend(Protocol):
    name: str                                          # "azure-databricks", "aws-databricks",
                                                       # "gcp-databricks", "onprem-spark", ...
    def map(self, graph: CostGraph) -> CostEstimate:   # compute-seconds → $$
        ...
```

- Core ships nothing pricing-shaped by default — without an extra, `result.cost_estimate.usd` is `None` and only compute-seconds are reported. (`[onprem-spark]` is a tiny extra rather than a core dependency because it still wants its own protocol implementation file and config schema.)
- `[databricks]` ships the workspace API client + system-table reader — used by every `[*-databricks]` backend, but **does not itself implement `PricingBackend`** (no DBU rate data lives there).
- `[azure-databricks]` / `[aws-databricks]` / `[gcp-databricks]` each ship their own `PricingBackend` plus their cloud's DBU-rate data and VM-price source.
- `[onprem-spark]` ships a `PricingBackend` driven entirely by `burnt.toml` rates.
- `runtime/` (auto-detect) picks a backend based on what's installed and what credentials/configuration are present. Multiple backends installed → user-selected via `burnt.toml` (`[burnt.pricing] backend = "azure-databricks"`).

This re-housing is what makes "Cost Compiler for Spark" a real architectural claim rather than just a tagline change.

---

## 3.5 Databricks system tables — what they're for and how they're configured

### What they're used for (four narrow, named uses — nothing fleet-wide)

System tables (Unity Catalog `system.*` schemas) become useful **only when `[databricks]` is installed** (the workspace API + SQL warehouse access live there). Each `[*-databricks]` pricing backend uses them for one or more of:

| Use | System table | Why it matters in burnt |
|---|---|---|
| **Live DBU rate lookup** | `system.billing.list_prices` | Replaces the shipped JSON of DBU rates (which goes stale) for orgs that prefer authoritative data. Shipped JSON remains the offline default. |
| **Real table size for `estimated_input_bytes`** | `system.information_schema.tables` (and where available, table-storage-statistics extensions) | Fills CostGraph nodes' input-bytes from ground truth without running `DESCRIBE DETAIL` per table — single batched SQL query. |
| **Cluster profile resolution** | `system.compute.clusters` | When `burnt.toml` has only a `cluster_id`, look up node type + size to feed the pricing math. Avoids `databricks-sdk` `clusters.get` round-trip per cluster. |
| **Last-run observed cost** | `system.query.history` (filtered by current user + recent time window) | Optional, opt-in: after `result = burnt.check(...)`, an in-notebook helper can show "your last actual run of this notebook cost $X" — pure observation, no prediction. This is the runtime-side completion of the "honest confidence" story for Databricks users. |

What system tables are explicitly **not** used for: org-wide retrospective workload mining, pattern detection across thousands of queries, cross-user cost attribution, fleet FinOps. Those are the surfaces the April 2026 pivot deleted; they don't come back through the system-tables door.

### Configurability via `burnt.toml`

System table paths can shift in two real-world cases: (1) orgs replicate them into a private schema for governance/cost reasons (`prod_observability.query_history`); (2) future Unity Catalog versions may relocate or namespace them. Both warrant config-driven paths.

Proposed shape (additive to the existing config schema, fits the current `[databricks]` table style):

```toml
[burnt.databricks.system_tables]
# Defaults shown; override per table for orgs that mirror system tables elsewhere.
query_history             = "system.query.history"
billing_usage             = "system.billing.usage"
list_prices               = "system.billing.list_prices"
information_schema_tables = "system.information_schema.tables"
compute_clusters          = "system.compute.clusters"
node_timeline             = "system.compute.node_timeline"

# Master switch — if false, burnt never queries system tables even when [databricks] is
# installed. Falls back to shipped JSON pricing + DESCRIBE DETAIL.
enabled = true
```

Env-var overrides follow the existing config priority order (`BURNT_DATABRICKS_SYSTEM_TABLES_QUERY_HISTORY=...`), so CI environments can repoint paths without editing `burnt.toml`.

Behaviour rules:

1. If a configured table is unreadable (permissions, doesn't exist), burnt logs once at `INFO` and falls back to its non-system-table path (shipped JSON / `DESCRIBE DETAIL` / SDK API call). It never errors — system tables are an **enrichment**, not a requirement.
2. The set of paths is resolved once per `burnt.check()` call and cached on the result.
3. `burnt doctor` reports which system tables are reachable for the current config, so users can verify access before relying on enrichment.

---

## 4. Install matrix (the user-visible contract)

| Install | Lint | Compute-seconds | Live runtime (sparkMeasure) | Dollars | System-table enrichment | HTML / `.dbc` |
|---|---|---|---|---|---|---|
| `pip install burnt` | ✅ 84 rules + `--fix`/`--unsafe-fixes` + `--diff` | ✅ | ✅ (core) | ❌ | ❌ | ❌ |
| `+ [onprem-spark]` | ✅ | ✅ | ✅ | ✅ from user-supplied `$/vCPU-hour` etc. | ❌ | ❌ |
| `+ [databricks]` (alone) | ✅ | ✅ | ✅ | ❌ — pricing data lives in cloud-specific extras | ✅ | ❌ |
| `+ [azure-databricks]` (auto-pulls `[databricks]`) | ✅ | ✅ | ✅ | ✅ Azure DBU + VM | ✅ | ❌ |
| `+ [aws-databricks]` (auto-pulls `[databricks]`) | ✅ | ✅ | ✅ | ✅ AWS DBU + EC2 | ✅ | ❌ |
| `+ [gcp-databricks]` (auto-pulls `[databricks]`) | ✅ | ✅ | ✅ | ✅ GCP DBU + GCE | ✅ | ❌ |
| `+ [notebook]` | ✅ | ✅ | ✅ | per other extras | per other extras | ✅ |
| `[all]` | ✅ | ✅ | ✅ | ✅ (whichever pricing backend is selected in `burnt.toml`) | ✅ | ✅ |

This matrix is the single source of truth for what users get from each combination — it should be embedded in the README and `DESIGN.md` once codified.

---

## 5. Critical files that document/codify this architecture (when work begins)

The following are the surfaces that future task work will touch when this re-statement is acted on:

- `README.md` — tagline, three-modes section, install matrix, access-levels table. Drop mentions of `[sql]` / `[alerts]`. Promote sparkMeasure to core.
- `DESIGN.md` — §1 Product, §2 Philosophy, §3 Environments, §4 Architecture, §10 Databricks Optional Module (rename to "Pricing Backends"), §11 Configuration (add backend selection, system-table path overrides per §3.5, on-prem rates for `[onprem-spark]`).
- `pyproject.toml` — remove `[sql]`, `[spark]`, `[alerts]`. Add `sparkmeasure>=2.0` and `LibCST` to core deps. Add `[notebook]`, `[azure-databricks]`, `[aws-databricks]`, `[gcp-databricks]`, `[onprem-spark]`. `[databricks]` becomes a transitive of every `[*-databricks]` extra. Update `[all]` accordingly.
- `src/burnt/core/` — new `pricing.py` (`PricingBackend` protocol).
- `src/burnt/databricks/` — workspace API client + system-table reader (no `PricingBackend` here). The current `DatabricksPricingBackend` migrates out into the cloud-specific extras.
- `src/burnt/cloud/azure_databricks/`, `aws_databricks/`, `gcp_databricks/`, `onprem_spark/` — one `PricingBackend` per package, each gated by its extra.
- `src/burnt/display/notebook.py` — gated by `[notebook]`; core stub raises `NotAvailableError`.
- `src/burnt/parsers/` — new `dbc.py` gated by `[notebook]`.
- `src/burnt/_check/` — wire `--fix`, `--unsafe-fixes`, `--diff` flags into the core CLI (folded in from former `[fix]` / `[git]` extras).
- `src/burnt/_config/` — schema additions for `[burnt.databricks.system_tables]` and `[burnt.pricing] backend = "..."`.
- `tasks/` — new tasks under PX (or a new PN phase) for: (a) pricing-backend protocol extraction, (b) cloud-pricing extra split, (c) `[notebook]` extra split, (d) sparkMeasure promotion, (e) `[sql]`/`[alerts]` removal, (f) docs alignment.

Reuse: existing lazy-import patterns in `src/burnt/runtime/__init__.py`, `src/burnt/databricks/__init__.py`, and the `__getattr__` deferral in core `__init__.py` already demonstrate the right shape — new extras follow the same pattern, no new infrastructure needed.

---

## 6. Verification — how we'll know this architecture is real (when codified)

Architecture-level checks:

1. **Core install isolation:** in a clean venv, `pip install burnt && burnt check ./examples/notebook.py` produces 84 rules' findings + a CostGraph in compute-seconds, no network calls, no cloud SDK imports. `pip show burnt` lists only core deps (pydantic, pydantic-settings, typer, rich, pyyaml, tabulate, sparkmeasure, LibCST).
2. **Folded-in CLI flags work in core:** `burnt check --fix` rewrites a known-fixable rule's match site; `burnt check --unsafe-fixes` additionally applies semantic-shift fixes; `burnt check --diff main` only lints files reported by `git diff --name-only main...HEAD`. None of these require an extra.
3. **`*-databricks` auto-pull works:** `pip install burnt[azure-databricks]` results in `databricks-sdk` being importable (transitive of `[databricks]`). Same for `aws-databricks`, `gcp-databricks`. Installing `[onprem-spark]` does **not** pull `databricks-sdk`.
4. **Each extra is independently installable, no silent pulls:** `pip install burnt[notebook]` adds Jinja2 only; `pip install burnt[databricks]` adds `databricks-sdk` + `requests` only; `pip install burnt[azure-databricks]` adds Azure-pricing data + the `[databricks]` deps and nothing else.
5. **Graceful absence:** without `[notebook]`, `burnt check x.dbc` exits 2 with `pip install burnt[notebook]`; without any pricing extra, `result.cost_estimate.usd` is `None` and only compute-seconds are reported; without `[databricks]`, system-table enrichment is silently skipped.
6. **Backend swap by config:** with `[azure-databricks]` and `[onprem-spark]` both installed, `[burnt.pricing] backend = "onprem-spark"` in `burnt.toml` produces `result.cost_estimate.backend == "onprem-spark"`. Switching to `"azure-databricks"` switches the backend without re-installing.
7. **System-table path override:** setting `[burnt.databricks.system_tables] query_history = "prod_observability.query_history"` causes burnt to query that table; pointing at a non-existent table logs once at INFO and falls back without erroring (per §3.5 rule 1).
8. **Install-matrix doc parity:** the README install matrix is the same matrix the test suite asserts against — one truth, both surfaces.

These are the gates that distinguish "we said it's modular" from "it actually is".

---

## 7. Out of scope for this document (explicit, so the next round doesn't drift)

- No design for `[azure-databricks]`, `[aws-databricks]`, `[gcp-databricks]`, `[onprem-spark]`, `[iceberg]`, `[mlflow]` beyond reserving the names and (for the pricing backends) the auto-pull rule on `[databricks]`.
- No design for the future `[aws-emr]`, `[aws-glue]`, `[gcp-dataproc]` slots beyond the naming pattern.
- No re-litigation of the April 2026 pivot. `advise`/`simulate`/recommendations/feedback-loop stay deleted.
- No re-introduction of the dropped extras (`[sql]`, `[spark]`, `[alerts]`, `[chargeback]`, `[sample]`, `[bench]`, `[catalog]`, `[fix]`, `[git]`). Their decisions are recorded in §2.2 and §2.4 above.
- No fleet/historical FinOps in burnt itself — that remains a separate-product concern. System-table use is bounded to the four narrow uses listed in §3.5.
- No new lint rules. The 84-rule catalog is unchanged by this document.
