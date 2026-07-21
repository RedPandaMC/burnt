# Holistic Repository Review — `burnt`

**Date:** 2026-07-21
**Branch reviewed:** `claude/burnt-holistic-review-1uxs1l` (from `main` @ `6bb15cf`)
**Scope:** Entire repository — Rust engine, Python package, CI/CD, security, performance, documentation, tests, and issue tracker — plus the two companion repositories that inform its direction:
- `Mallard` (exemplar CI + VitePress docs site)
- `tree-sitter-sql-extended` (the in-house multi-dialect SQL grammar intended to become burnt's parsing backend)

**Companion document:** `reviews/rust-engine-review.md` (2026-05-19, 110 findings R-001..R-110). This review does not repeat that file; it verifies which of its themes still hold, and covers everything that review did not (Python, CI, docs, tests, packaging, licensing, issue tracker, and the parsing-backend strategy).

---

## Executive summary

`burnt` is in **good architectural health and poor operational health**. The Rust engine is genuinely well-designed (pyo3-free domain core, a grammar-agnostic `AstShape` contract, a codegen'd rule registry with generated per-rule tests, zero `unsafe`, zero TODO markers). The Python layer has a disciplined lazy-import firewall around the engine and a clean security posture.

But the project has **no CI whatsoever**, ships **contradictory license metadata**, **crashes on its own declared minimum Python version**, reports the **wrong version number**, and its documentation describes flags, workflows, and rule counts that do not exist. The issue tracker (20 open issues) is largely stale: many issues reference infrastructure (`.github/workflows/ci.yml`) or commands that were never created or have since been removed, and one 110-finding mega-issue (#86) is too large to be actionable.

**The single most consequential finding** is about the parsing backend: burnt believes it migrated to the in-house grammar, but it did not.

| Area | Verdict |
|---|---|
| Parsing backend | **Wrong grammar in production** — DerekStride's ANSI crate, not `tree-sitter-sql-extended` (§1) |
| CI/CD | **Absent entirely** — no `.github/` directory (§4) |
| Python correctness | 5 shipping bugs incl. license metadata and a 3.10 crash (§3) |
| Rust engine | Sound; dead legacy subsystem + error-swallowing + perf nits (§2) |
| Security | Clean posture; tooling declared but never executed (§5) |
| Performance | No pathologies; several cheap wins (§6) |
| Docs | Internally contradictory (43 vs 110 vs 111 rules); overclaims features (§7) |
| Tests | Good unit core; zero integration tests; key CLI paths untested (§8) |
| Issues | 20 open; recommend full reboot (§9) |

---

## 1. Parsing infrastructure — the headline finding

### 1.1 burnt is not using the in-house grammar

`src/burnt-engine/Cargo.toml:18` declares `tree-sitter-sequel = "0.3"`, resolved in `Cargo.lock` to `tree-sitter-sequel 0.3.11` from **crates.io**. Verified via the crates.io API (2026-07-21):

- crate `tree-sitter-sequel`, max version 0.3.11
- repository: `https://github.com/derekstride/tree-sitter-sql.git`
- owner: `DerekStride`

So the SQL builder (`src/burnt-engine/src/graph/sql.rs:384,462`) parses with **DerekStride's grammar** — a permissive, broadly ANSI-flavored SQL grammar with no Spark or Databricks dialect awareness. The in-house `tree-sitter-sql-extended` repo declares the *same crate name and version* in its own `Cargo.toml` (`tree-sitter-sequel` 0.3.11), which is a name collision, not a publication: DerekStride owns that name on crates.io, so `tree-sitter-sql-extended`'s `publish.yml` cargo-publish job can never succeed under it. (The closed migration issue #87 conflated the two, describing the dependency as "covers Databricks SQL dialect" — it does not.)

**Consequence:** every Spark/Databricks-specific construct — `LATERAL VIEW`, `QUALIFY`, `PIVOT`/`UNPIVOT`, `CREATE TABLE … USING/OPTIONS`, `OPTIMIZE … ZORDER BY`, `VACUUM`, `CLUSTER/DISTRIBUTE/SORT BY`, time travel, `SELECT * EXCEPT`, Unity Catalog DDL — is at best an `ERROR` node and at worst silently mis-shaped, invisibly degrading graph construction and every SQL rule.

### 1.2 What `tree-sitter-sql-extended` offers

The in-house grammar is purpose-built for burnt (per its README) and is architecturally right: a strict ANSI base plus 22 dialect grammars composed by inheritance following real genealogy (`databricks → spark → hive → base`). The `spark` dialect covers QUALIFY, PIVOT, time travel, SQL scripting, Iceberg, VARIANT, `CREATE TABLE USING`, Spark 4.0 features; `databricks` adds Delta (`OPTIMIZE ZORDER`, `VACUUM`, `RESTORE`), Unity Catalog DDL, and `GRANT`. Its CI is mature (sharded corpus tests, a feature-coverage scorecard with regression gating, multi-registry publish automation, VitePress docs).

### 1.3 What blocks the migration (upstream)

1. **Crate name** — must be renamed (e.g. `tree-sitter-sql-extended`) before it can ever be published; until then a git dependency works, but the crate name inside its `Cargo.toml` still collides in any dependency tree that also pulls DerekStride's crate.
2. **No per-dialect Rust bindings** — `bindings/rust/lib.rs` exposes only the base `LANGUAGE` (`tree_sitter_sql`); no `.rs` bindings exist under `spark/` or `databricks/`, and `Cargo.toml`'s `include` list (`"src/*"` etc.) excludes dialect directories, so a published crate cannot even ship Spark sources. The generated `spark/src/parser.c` / `databricks/src/parser.c` exist with predictable symbols (`tree_sitter_spark_sql`, `tree_sitter_databricks_sql`), so feature-gated `LanguageFn` consts are a contained change.

### 1.4 Migration blast radius in burnt (good news)

All grammar coupling lives in **one file**: `src/burnt-engine/src/graph/sql.rs` (~892 lines). It touches the grammar exclusively through ~30 hardcoded node-kind/field-name string literals (`"object_reference"`, `"relation"`, `"keyword_merge"`, `"create_query"`, field `"database"`, `"predicate"`, …). The rule-facing contract — `resolved/ast_shape.rs` (`SqlStatementNode`, `SqlExpr`, `ScopeFacts`) — is grammar-agnostic and `#[non_exhaustive]`, so rules do not change.

The one structural piece to plan around: the `extra_nodes: &[Node]` plumbing threaded through the whole SQL build path (`sql.rs:36-53, 93-110, 224, 348, 476-509`) exists solely to recover from DerekStride-grammar-specific `ERROR`-sibling emission for `JOIN` without `ON`. Under the new grammar this workaround should be deleted, not ported.

Also unlocked post-migration: `SqlStatementKind::Explain` (`resolved/ast_shape.rs:133`, declared but never constructed) and embedded `spark.sql("…")` parsing via `Parser::set_included_ranges` (one tree spanning Python + SQL with byte-accurate positions — resolves old finding R-003).

---

## 2. Rust engine (`src/burnt-engine`)

### 2.1 What is good (and should be preserved)

- **pyo3 firewall**: only `lib.rs`, `types.rs`, `resolved/python.rs`, `session/mod.rs`, `plan_parser.rs` know about pyo3; the domain core tests without a Python interpreter. Heavy work releases the GIL via `py.allow_threads` (`lib.rs:91,140,186,195`).
- **`resolved` design**: the tree-sitter `Tree` is dropped after build; `AstShape`/`ScopeFacts` are the single query surface (`resolved/ast_shape.rs:1-13`). This is precisely why the parser migration is tractable.
- **Rule registry codegen**: `build.rs` compiles ~130 rule TOMLs into a static registry and generates per-rule pass/fail tests; DSL predicate names are validated (warn-level) at build time.
- **Test layering**: module tests nearly everywhere, `insta` snapshots, crate-level integration test, three external rule suites wired via `[[test]]`.
- No `unsafe`, no process spawning, no env access, tuned release profile (`opt-level=3, lto=true, codegen-units=1, strip=true`).

### 2.2 Findings

| # | Severity | Finding | Location |
|---|---|---|---|
| RE-1 | High | **Rule errors silently swallowed**: `rules::run(...).unwrap_or_default()` converts engine failures to "zero findings" with no diagnostic, inconsistently with `analyze_source` which propagates | `lib.rs:171` |
| RE-2 | High | **Invalid rule DSL skipped at runtime** with `eprintln!` instead of failing the build — a typo in a rule pattern ships as a rule that never fires | `rules/graph_pipeline.rs:47,55`; `build.rs` warns only |
| RE-3 | High | **Substring-cascade operation classifier**: `handle_spark_call` classifies via `call_text.contains(...)` over raw call text — matches inside string literals/comments, `.count(` anywhere, etc. The tree-sitter AST is available but unused for this | `graph/python.rs:180-316` |
| RE-4 | Medium | **Dead legacy subsystem**: `RuleEngine`, `Rule` trait, `AnalysisCtx`, `RuleMeta`, `LanguageFilter` are never called from the pyo3 surface or tests — an entire superseded engine carried as weight | `rules/mod.rs:73-120`, `rules/rule.rs` |
| RE-5 | Medium | **Unconstructed types/variants**: `SqlStatementKind::Explain`, `ScalingBehavior::Quadratic`, `AstNode::SqlExpression`, `SdpSignal`, `PythonParseResult`, `SqlFragment`; stale doc-ref to removed `TableRef::from_object_name` | `resolved/ast_shape.rs:133`, `types.rs:68,198-223` |
| RE-6 | Medium | **O(n²) node attachment**: builders locate nodes by linear `iter_mut().find(id)` per attach; the id encodes the index (`node_{len+1}`) so an index lookup is trivial | `graph/sql.rs:880`, `graph/python.rs:402,411` |
| RE-7 | Medium | **pyo3 0.22 deprecated `_bound` APIs** (`get_type_bound`, `PyDict::new_bound`, `import_bound`) block upgrade to pyo3 ≥ 0.23 | `lib.rs:232`, `plan_parser.rs:138`, `resolved/python.rs:508` |
| RE-8 | Medium | **No `[lints]` table, no MSRV, no `rust-toolchain.toml`**; allows are scattered as file-level attributes. `unsafe_code = "forbid"` would codify the already-clean posture for free | `Cargo.toml` |
| RE-9 | Medium | **`.cargo/config.toml` hardcodes `python3.12` linker flags** (`-lpython3.12`, `-L/usr/lib/x86_64-linux-gnu`) — environment-specific, contradicts the abi3 portability goal | `.cargo/config.toml` |
| RE-10 | Low | Panic across FFI in `metrics` getter (`.expect(...)` under GIL — realistically infallible, still a hard panic) | `resolved/python.rs:44` |
| RE-11 | Low | Catalog URL interpolates the table FQN into the path unencoded; no URL validation on caller-supplied `base_url` | `catalog/databricks.rs:64-66` |
| RE-12 | Low | **6 stale `.snap.new` pending insta snapshots** — unaccepted snapshot output for a rule tier the CHANGELOG marks as deleted | `tests/unit/rules/snapshots/*.snap.new` |
| RE-13 | Info | Version drift: engine crate `0.2.0` vs package `0.3.0` | `src/burnt-engine/Cargo.toml:4` |
| RE-14 | Info | Rust test files live inside the Python test tree, wired by relative `[[test]]` paths — fragile cross-tree layout | `Cargo.toml:36-50` → `tests/unit/rules/*.rs` |

Positive security notes on the Rust side: DoS caps in the plan parser (`MAX_PLAN_NODES = 100_000`, `plan_parser.rs:22,171`), 10s HTTP timeout (`session/rest_client.rs:24`), mutex-poisoning handled gracefully (`catalog/databricks.rs:59,86`), linear-time `regex` crate (no ReDoS).

---

## 3. Python package (`src/burnt`)

### 3.1 What is good

- **Lazy engine-import firewall** with explicit comments and single-import-site discipline for `_resolve_graph` (`_check/__init__.py:184-188`, `graph/enrich.py:18-23`); graceful `ImportError` degradation so pure-Python installs work.
- Modern, consistent typing style; strict mypy config + pydantic plugin; thoughtful ruff selection; `filterwarnings = ["error"]` and `xfail_strict` in pytest.
- Zero TODO/FIXME markers across `src/` and `tests/`.

### 3.2 Shipping bugs

| # | Severity | Finding | Location |
|---|---|---|---|
| PY-1 | Critical | **License contradiction**: `pyproject.toml:6` says `GPL-3.0-or-later`; the `LICENSE` file is **MPL-2.0**; README badge says MPL-2.0. Package metadata is legally wrong. *(Decision: MPL-2.0 is intended; fix pyproject + Rust crate `license` field.)* | `pyproject.toml:6`, `LICENSE:1`, `README.md:14` |
| PY-2 | Critical | **`StrEnum` import crashes on the declared 3.10 floor** (`StrEnum` is 3.11+; `requires-python = ">=3.10"`). Any import of `burnt.parsers.antipatterns` on 3.10 raises. *(Decision: floor moves to 3.12 / abi3-py312.)* | `parsers/antipatterns.py:6,9` vs `pyproject.toml:7` |
| PY-3 | High | **`ClusterProfile.from_databricks_json` calls `ClusterConfig.from_databricks_json`, which does not exist** → `AttributeError` when exercised | `core/models.py:66-79` (call at :69; `ClusterConfig` defines only `to_dab`) |
| PY-4 | High | **Wrong version reported**: `__version__ = "0.2.0"` while pyproject is `0.3.0` — `burnt --version` and `burnt.version()` lie. Single-source via `importlib.metadata` | `src/burnt/__init__.py:36`, `cli/main.py:35` |
| PY-5 | High | **Dead cost-gate API**: `max_cost` is threaded into `_check.run` but never read; `CostBudgetExceeded` is exported but never raised — the documented cost gate is vaporware | `_check/__init__.py:72`, `__init__.py:24` |

### 3.3 Structural issues

- **Duplicate `Severity`** (`core/enums.py:13` as `str, Enum`; `parsers/antipatterns.py:9` as `StrEnum`) and **duplicate, divergent `PyEstimate`** (`core/models.py:91` vs `graph/estimate.py:82`) — `CheckResult.estimate: Any` holds either depending on code path.
- **Legacy parallel analysis path**: `parsers/antipatterns.detect_antipatterns` is unused by `src/` (tests only) — the Python twin of the dead Rust `RuleEngine`.
- **API vocabulary split**: `burnt.check()` speaks `skip`/`only`/`max_cost`, the CLI speaks `select`/`ignore`/`extend-select`/`fail-on` — two mental models for one product.
- **No `.pyi` stubs for `burnt._engine`**: every engine object is `Any`; code duck-types with `getattr(f, "code", ...)` (`_check/__init__.py:99-108`, `graph/estimate.py:190-235`). Strict mypy is blind exactly at the most important boundary, and `py.typed` overstates the guarantee.
- `tests/integration/conftest.py:39` imports **nonexistent `burnt.tables.connection`**.
- `enrich_dlt` returns empty (`graph/enrich.py:131-137`) while the README markets DLT pipeline mode.
- Mutable class-level defaults on pydantic models (`costs: dict = {}` etc., `core/models.py:101-104`) — safe under pydantic v2 but an anti-pattern to copy.

---

## 4. CI/CD — absent

There is **no `.github/` directory**: no workflows, no Dependabot, no CodeQL, no issue/PR templates, no release automation. Consequences:

- Nothing runs `cargo test` / `pytest` / `ruff` / `mypy` on push or PR.
- The entire `lint` dependency group (`bandit`, `pip-audit`, `vulture`, `xenon`, `interrogate` — `pyproject.toml:52-59`) is **never executed by anything** (no CI, no pre-commit config, no Makefile/justfile/tox).
- The 80% coverage gate (`fail_under = 80`, `pyproject.toml:82-102`) is enforced by nothing.
- **No wheel-building automation** — for a pyo3/maturin project this is release-blocking: users cannot `pip install burnt` without a local Rust toolchain until a maturin wheel matrix + publish workflow exists.
- Several old issues (#43, #56, #57…) presuppose `.github/workflows/ci.yml` exists.

**Template: Mallard.** Directly transplantable patterns from `Mallard/.github/`: every action SHA-pinned with a version comment + Dependabot refreshing pins across ecosystems; path-filtered workflows with YAML anchors; a composite `setup-toolchain` action; Codecov upload with `codecov.yml` thresholds; a dedicated `security.yml` (dependency-review, CodeQL, scanner SARIF uploads under distinct categories, weekly cron); docs build validated on PRs and deployed to Pages on main; architectural boundaries enforced by lint config (Mallard uses `no-restricted-imports`; burnt's analog is clippy `-D warnings` + import-boundary tests).

---

## 5. Security

**Posture: clean.** Both sides avoid the classic failure modes:

- No `unsafe`, no `subprocess`/`os.system`/`shell=True`, no `eval`/`exec`/`pickle`, `yaml.dump` only.
- All 11 `requests.*` call sites carry explicit `timeout=`; no `verify=False`.
- Token redaction in `doctor` output (`cli/main.py:768`); credentials come from env/pydantic-settings, none committed; the opencode GitHub PAT is a file reference (`opencode.jsonc`), not an embedded secret.
- Rust: DoS caps, bounded HTTP, no ReDoS surface (rules are trusted + `regex` crate is linear-time).

**Hardening items (minor):**

| # | Finding | Location |
|---|---|---|
| SEC-1 | GCP API key passed as URL query parameter (`?key={api_key}`) — can leak into proxy/access logs; move to header | `providers/gcp_databricks/catalog.py:137,148` |
| SEC-2 | Table FQN interpolated unencoded into catalog URL path; no URL validation/allowlist on caller-supplied base URLs | `catalog/databricks.rs:64-66` |
| SEC-3 | Disk cache written with default umask; harmless content today, but set restrictive perms as hygiene | `providers/cache.py:69-75` |
| SEC-4 | Hand-rolled line parser rewrites the TOML ignore list — brittle; use a real TOML writer | `cli/main.py:596-641` |
| SEC-5 | Security scanners (bandit/pip-audit/cargo-audit) declared but never wired to any execution (see §4) | `pyproject.toml:52-59` |

---

## 6. Performance / optimization

No pathologies; the engine is fast by construction (Rust, GIL released, rayon in the session collector). Cheap wins, in value order:

1. **O(n²) node attachment** in both graph builders → index-based lookup (RE-6).
2. **Second tree-sitter parse** in `Graph::from_python` solely to rebuild an `ImportMap` the builder already has (documented in old #88's spec) — remove with the method-chain dispatch work.
3. String node-ids cloned freely; interning or numeric ids would cut allocation across `populate_dag_facts` (`resolved/scope_facts.rs:80`, documented O(V·E) — fine at ≤200 nodes).
4. Clone-heavy pyo3 getters (`PyResolvedGraph::graph()` clones the whole graph per access — old R-018, still present).
5. Python cold-start is protected by the lazy-import firewall; keep it (any future eager engine import would regress CLI startup).

---

## 7. Documentation

### 7.1 Internal contradictions (worst offenders)

| Claim | Where | Reality |
|---|---|---|
| "43 lint rules across 6 categories" | `DESIGN.md:17,49,62`, `src/burnt/__init__.py:5` | 111 rule TOMLs under `src/burnt-engine/rules/` |
| "All 110 rules migrated" / "110 rules" | `CHANGELOG.md:43`, `README.md:55,125,131` | 111 |
| `--fix`, `--unsafe-fixes`, `--diff`, `--max-cost`, `--event-log` flags | `README.md:60-61,84,133`, `DESIGN.md:13,20,79` | None exist in `cli/main.py:72-110` |
| DLT pipeline mode | `README.md:100-109` | `enrich_dlt` is an empty stub (`graph/enrich.py:131-137`) |
| MPL-2.0 badge | `README.md:14` | pyproject says GPL-3.0-or-later (badge is right; metadata wrong) |
| `tasks/`-directory PLANNER/EXECUTOR workflow | `AGENTS.md:9-27,42-57` | No `tasks/` directory exists |
| SARIF + Code Scanning CI example for users | `README.md:88-97` | The repo itself has no CI |

### 7.2 Gaps

- **No docs site** (Mallard and tree-sitter-sql-extended both have VitePress + Pages; burnt has 5 engine-focused files in `docs/`).
- **No Python API reference** for the public `burnt.check()`/`config()`/`start_session()` surface.
- **No CONTRIBUTING.md**, no docs-as-tested-contract (Mallard pins `settings.md`/`commands.md` to `package.json` via an integration test — the same pattern would keep burnt's rule counts and CLI flag docs honest, generated from the rule TOMLs).
- `writing-rules.md` still lacks the graph-DSL tier documentation (old R-096 — all 111 rules use it).

---

## 8. Tests

- **Unit core is decent**: 14 Python test files (~2.4k LOC), plan-parser fixtures, mock backend, plus the Rust suites and generated per-rule tests.
- **Zero integration tests**: `tests/integration/` contains only a conftest — which itself imports a module that doesn't exist (PY §3.3).
- **Untested, CLI-hot modules**: `core/suppression.py` (the `# burnt: ignore` engine), `core/rule_filter.py` (rule selection), `display/terminal.py`, `parsers/notebooks.py`; CLI `init`/`rules`/`cache`/`pricing` subcommands.
- **`hypothesis` is a dead dev-dependency** — declared (`pyproject.toml:46`), never imported.
- 6 unaccepted `.snap.new` snapshots (RE-12) mean some Rust snapshot tests are failing/unreviewed.
- The 80% coverage and interrogate gates are configured but unenforced (§4).

---

## 9. Issue tracker assessment

20 open issues at review time. Categories:

- **Stale phase-plan batch** (#23, 40, 43, 48, 49, 55, 56, 57, 58, 59, 61, 63, 64, 69, 70): written against a May roadmap; many presuppose CI that doesn't exist, commands that were removed, or acceptance criteria that partially shipped. Individually salvageable content, collectively misleading.
- **Mega-issue #86**: 110 review findings in 9 work streams — a table of contents, not an actionable unit of work.
- **Recent engine issues #88/#89/#90**: accurate and well-specified; content carried into the reboot.
- **Placeholder #76**: "Docs: Add better docs / To add description".
- Closed #87 ("migrate to tree-sitter-sequel") was marked completed but rested on the false premise that the crates.io crate covers Databricks SQL (§1.1) — the *actual* intended migration (to `tree-sitter-sql-extended`) has not happened.

**Disposition (executed alongside this review):** all 20 closed as superseded, replaced by a structured set — six epics (Parsing migration / CI-CD / Correctness / Engine quality / Docs / Tests+security) with scoped child issues, plus two upstream issues in `tree-sitter-sql-extended` (crate rename; per-dialect Rust bindings) that gate the parsing epic.

---

## Appendix: verification commands used

```bash
# crates.io provenance of the current SQL grammar dependency
curl -s -A "review" https://crates.io/api/v1/crates/tree-sitter-sequel        # → repo derekstride/tree-sitter-sql
curl -s -A "review" https://crates.io/api/v1/crates/tree-sitter-sequel/owners # → DerekStride

# CI absence
ls -a /home/user/burnt          # no .github

# Grammar usage sites
grep -rn 'tree_sitter_sequel' src/burnt-engine/src   # graph/sql.rs:384,462 only

# Rule TOML count
find src/burnt-engine/rules -name '*.toml' | wc -l   # 111 (+ non-rule TOMLs excluded)
```
