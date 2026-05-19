# Rust Engine Code Review — `burnt-engine`

**Date:** 2026-05-19
**Branch reviewed:** `claude/review-rust-engine-nDV61` (at commit `f70adde`)
**Scope:** Entire Rust engine at `src/burnt-engine/` — 45 `.rs` files, ~14,500 LOC
**Reviewer:** Automated deep-dive review (multi-agent code analysis + `cargo clippy`/`cargo doc`/`cargo tree`)

---

## Executive Summary

`burnt-engine` is in **good baseline health** — no `unsafe` blocks, no `TODO`/`FIXME` markers, `cargo clippy -- -D warnings` is enforced, and the public API is reasonably well-scoped through PyO3. The codebase is modular, with clear separation between graph builders, the rules engine, the DSL evaluator, and the runtime fusion layer.

However, the review surfaced **a set of structural and correctness concerns that warrant attention** before further scaling:

| Severity     | Count | Headline themes                                                                 |
|--------------|------:|---------------------------------------------------------------------------------|
| **Critical** |     6 | **Migrate SQL builder from `sqlparser` to `tree-sitter-sequel`**; SQL builder loses derived-subquery references; orphaned shuffle nodes; missing PyO3 typed exceptions |
| **High**     |    24 | Pervasive `.unwrap()` / `.expect()` on tree-sitter walks; clone-heavy hot paths; missing input validation in catalog/HTTP layer; graph DSL tier entirely undocumented in `writing-rules.md` |
| **Medium**   |    33 | Silent error swallowing in `build.rs`; case-sensitivity in table-ref dedup; clippy `pedantic` lint surface (~600 warnings); incomplete error-type unification; DSL docs/code drift |
| **Low**      |    35 | Idiom polish (`format!` string interpolation, `Option` combinators); naming inconsistencies; test-only panics; rule-count comment drift |
| **Info**     |    11 | Documentation cross-links, profile tuning, future-proofing notes                |
| **Total**    |   109 |                                                                                 |

The four user-requested focal areas are summarised in their own sections at the end:

1. **Graph logic soundness** — the SQL and Python builders diverge on important semantics (alias tracking, derived subqueries, shuffle wiring), and `types.rs` allows internally inconsistent `TableRef` states.
2. **Dependency hygiene** — two declared dependencies (`tree-sitter-sequel`, `streaming-iterator`) are not used; PyO3 is on `0.22` without `abi3` (rebuild per Python version); `thiserror = 2.0` adoption is partial — most public APIs still return `Result<_, String>`.
3. **Python-layer interface** — error mapping to Python is inconsistent (`PyRuntimeError` vs `PyIOError` with no domain-specific exception type); several `#[pyfunction]` items lack docstrings; `PyResolvedGraph::graph()` clones the entire graph on every getter.
4. **DSL syntax & docs** — the grammar is sound but operand/arity validation happens at match time (typos silently produce zero matches); `pred_shares_receiver` is registered twice; predicate metadata exists only as inline comments. **Documentation cross-reference surfaced 13 distinct gaps**, the most consequential being that `writing-rules.md` does not document the graph DSL tier at all even though all 111 active rules use it.

---

## Methodology

This review combined:

- **Static code analysis** across 8 tiers (entry & codegen, graph builders, DSL engine, runtime fusion, I/O surface, graph soundness, Python interface, DSL docs) — each tier had every relevant file read in full.
- **`cargo clippy --lib --no-deps -- -W clippy::pedantic -W clippy::nursery`** — surfaced ~600 lints currently masked by the project's `-D warnings` baseline.
- **`cargo doc --no-deps --document-private-items`** — surfaced 4 broken intra-doc links.
- **`cargo tree --duplicates`** — confirmed 4 transitive duplicate chains (`getrandom 0.2/0.4`, `memchr 2.8` reached two ways, others).
- **`cargo audit`** — not available in environment; recommend running in CI.

Each finding carries:

- **ID** — stable, of the form `R-NNN`.
- **Location** — `path:line` (or line range).
- **Category** — `panic`, `error-handling`, `security`, `perf`, `dead-code`, `idiom`, `consistency`, `pyo3`, `build`, `graph-soundness`, `deps`, `dsl-syntax`, `dsl-docs`, `tests`.
- **Severity** — `Critical` / `High` / `Medium` / `Low` / `Info`.
- **Impact** — what could go wrong / why this matters.
- **Recommendation** — concrete next step.
- **Snippet** — included only where essential to disambiguate.

---

## Per-module Summary

| Module                          | LOC   | Key concerns                                                            |
|---------------------------------|------:|-------------------------------------------------------------------------|
| `lib.rs` (PyO3 entry)           |   295 | Error mapping inconsistency; missing docstrings; redundant graph rebuilds |
| `types.rs`                      |   874 | `TableRef` allows invalid state combinations; case-sensitivity unspecified |
| `detect.rs`                     |    61 | Bounds-check gap in `is_dlt_decorator`; double-scan of source           |
| `plan_parser.rs`                |   319 | `.unwrap()` on JSON `.find()`; no plan-size bounds; test-only assertions |
| `json_py.rs`                    |   ~70 | Silent f64 fallback hides invariant violations                          |
| `build.rs`                      |   320 | Silent TOML parse swallowing; no DSL syntax validation at build time; registry clones on every load |
| `catalog/`                      |   ~150 | URL encoding gap; token-in-error-message risk; cache scoped to instance |
| `graph/python.rs`               |  1004 | High `.unwrap()` density on tree-sitter walks; SQL exec nodes never created |
| `graph/sql.rs`                  |   905 | **Recommended for full rewrite onto tree-sitter-sequel (R-109)**; orphaned shuffle nodes; derived subqueries dropped; case-sensitive dedup |
| `graph/sdp.rs`                  |   547 | Raw string parsing for DLT args; case-sensitive lookup; SDP source-type overwrite |
| `semantic/mod.rs`               |   ~80 | `push_scope`/`pop_scope`/`get_bindings` are dead — scope stack never used |
| `resolved/merge.rs`             |   505 | Dangling-overlay risk; multiple stages-per-exec silently picks first    |
| `resolved/python.rs`            |   611 | Heavy cloning per getter (graph, table_specs, metrics)                 |
| `rules/graph_dsl/lexer.rs`      |   560 | Sound; minor reserved-keyword note                                       |
| `rules/graph_dsl/parser.rs`     |   540 | One-token lookahead is fragile; no parse-time predicate validation     |
| `rules/graph_dsl/predicate.rs`  |  1795 | **Duplicate `shares-receiver` registration**; clone-heavy quantifiers; regex compiled per call |
| `rules/graph_dsl/matcher.rs`    |   964 | Head-kind validation only at match time (typos = silent no-match)       |
| `rules/graph_pipeline.rs`       |   381 | `OnceLock`-cached pattern map — sound but no invalidation hook         |
| `session/rest_client.rs`        |    57 | No explicit TLS policy; silent builder fallback; no response-size limit |
| `session/mod.rs`                |   295 | Token-bearing reqwest errors propagated raw; URL not validated as `url::Url` |
| `ingestion/files.rs`            |   223 | No path canonicalisation; no file-size cap; symlink-following          |
| `ingestion/dabs.rs`             |   ~30 | Stub returning `"Not implemented"`                                      |
| `parse/notebooks.rs`            |   ~340 | Multi-byte newline byte-offset assumption; cell-line cloning            |
| `parse/import_map.rs`           |   516 | Three `#[allow(dead_code)]` fields/variants; unbounded AST recursion   |

---

## Findings — by severity

### Critical

#### R-109. Migrate the SQL builder from `sqlparser` to tree-sitter (via `tree-sitter-sequel`)
- **File:** `src/burnt-engine/src/graph/sql.rs` (entire file, 905 LOC); `src/burnt-engine/Cargo.toml`
- **Category:** graph-soundness / consistency / deps
- **Severity:** Critical
- **Note on naming:** The crate `tree-sitter-sequel` (already in `Cargo.toml`, currently unused) is published from `github.com/DerekStride/tree-sitter-sql` — the GitHub repo and crates.io names differ. The grammar covers Databricks SQL dialect features. No new dependency is needed for this migration.
- **Impact:** The SQL builder is the only major parsing surface that doesn't use tree-sitter — the Python builder (`graph/python.rs`) and the SDP builder (`graph/sdp.rs` for embedded SQL fragments) both use tree-sitter. The split causes:
  1. **No error recovery on SQL.** `sqlparser` bails on the first parse error, returning `Err(...)` which `graph/sql.rs:18-21` then silently converts to an empty `Vec` (R-024). A single typo in a file produces zero findings. Tree-sitter parses partial/malformed input and yields a best-effort tree, so the rest of the file still produces findings.
  2. **Embedded SQL in Python is unreachable.** `spark.sql("…")` content is a string from tree-sitter's perspective; to analyse it today the engine would need to spin up a *second* parser (`sqlparser`) with different node types, different position info, and different error semantics. With a single tree-sitter pipeline, the embedded string can be re-parsed with `Parser::set_included_ranges` into the *same* tree, sharing position offsets — directly unblocking R-003.
  3. **Inconsistent position info.** `sqlparser` line numbers are statement-index-relative (R-2-1 of the Tier B agent: "SQL builder sets `line_number = statement_index + 1`, which is not the actual source line"). Tree-sitter gives byte-accurate row/column for every node, matching the Python builder.
  4. **Concrete-syntax DSL alignment.** The graph DSL `ast/*` patterns assume a tree-sitter-shaped AST (field names, child kinds). SQL nodes today take a different shape, which is why rules referencing `:method-chain`, `:arg/N`, etc. only work on Python.
  5. **Dependency duplication.** `tree-sitter-sequel = "0.3"` is already a declared dependency in `Cargo.toml` and currently goes unused (this was originally flagged as a removal candidate — see R-053, now superseded). The package was clearly intended for adoption.
- **Recommendation:**
  1. Build a `graph/sql_ts.rs` next to the existing `graph/sql.rs` that traverses tree-sitter-sequel concrete syntax and produces the same `Graph`/`Vec<Finding>` pair.
  2. Mirror the visitor functions one statement at a time (CTE, SELECT, JOIN, GROUP BY, INSERT, MERGE, CREATE TABLE / VIEW / STREAMING TABLE) — write a fixture file per construct and pin output with `insta`.
  3. Once feature-parity tests pass, switch `Graph::from_sql` to call the new path, delete `graph/sql.rs`, drop `sqlparser` from `Cargo.toml`. Keep DatabricksDialect-specific cases (e.g. `EXCEPT` semantics) as a thin normalisation pass.
  4. Wire embedded `spark.sql("…")` strings into the SQL tree using `Parser::set_included_ranges` so a single tree spans Python + SQL — this resolves R-003 as a side-effect.
  5. Validate against the existing rule corpus (`tests/unit/rules/test_*_rules.rs`) — these are the regression net.
- **Cost / risk:** ~1–2 engineer-weeks for the migration; `tree-sitter-sequel` does not cover every Databricks SQL extension out of the box (verify against the rule corpus). Migration is staged via the side-by-side approach above, so risk is contained.

#### R-001. SQL `TableFactor::Derived` subqueries silently drop their inner table references
- **File:** `src/burnt-engine/src/graph/sql.rs:349-351`
- **Category:** graph-soundness
- **Severity:** Critical
- **Impact:** `FROM (SELECT col FROM inner_tbl) AS sub` produces zero edges referencing `inner_tbl`. Rules that depend on table-reference completeness (e.g. cost estimation by table, lineage) silently undercount.
- **Recommendation:** Recurse into `TableFactor::Derived { subquery, .. }` by calling `collect_table_refs_from_query(subquery)` — symmetrical with `Cte` handling.

#### R-002. JOIN shuffle nodes are created but never edged into the graph
- **File:** `src/burnt-engine/src/graph/sql.rs:386-397`
- **Category:** graph-soundness
- **Severity:** Critical
- **Impact:** `process_join()` constructs a shuffle node with `let _shuffle_node_id = self.create_node(...)`; the underscore prefix indicates the node is then dropped. Comment claims it'll be wired "later in process_query" but `process_query` only handles GROUP BY shuffles. Any rule that traces the shuffle stage of a JOIN cannot fire.
- **Recommendation:** Either (a) connect the join shuffle through `data_flow` edges before returning, or (b) remove the orphaned node creation. If joins should not produce shuffle nodes, delete the dead code.

#### R-003. `spark.sql()` calls are classified but never created as graph nodes
- **File:** `src/burnt-engine/src/graph/python.rs:118-129`
- **Category:** graph-soundness
- **Severity:** Critical
- **Impact:** `handle_spark_call` reports `OperationKind::Read` for `spark.sql(...)`, but the surrounding code returns before `create_node()` runs (per the agent's read; verify on the line range). Effectively every embedded SQL call is invisible to rules until the SQL builder re-parses the string — which is a separate code path.
- **Recommendation:** Create a node for `spark.sql` at the call site so its `tables_referenced` attach and lineage rules can target it. Confirm against current behaviour and add a regression test.

#### R-004. PyO3 layer surfaces all engine errors as `PyRuntimeError` — no typed exception hierarchy
- **File:** `src/burnt-engine/src/lib.rs:79,110,206,220,226,233`
- **Category:** pyo3
- **Severity:** Critical
- **Impact:** Python callers cannot distinguish *parse failure* from *file-not-found* from *rule-evaluation panic* — all of them surface as `PyRuntimeError` or `PyIOError`. Downstream tooling (lints, CI, the `burnt` CLI) has no structured way to react.
- **Recommendation:** Define `class BurntEngineError(Exception)` and a small set of typed subclasses (`ParseError`, `RuleError`, `CatalogError`, `IoError`) using `create_exception!`. Map Rust error variants in a single `From<EngineError> for PyErr` impl.

#### R-005. `build.rs` silently drops malformed rule TOMLs via `.ok()?` / `.flatten()`
- **File:** `src/burnt-engine/build.rs:39,50-58`
- **Category:** build
- **Severity:** Critical
- **Impact:** A rule author who introduces a malformed TOML (typo, broken graph DSL block, missing `id`) gets **zero build error** — the rule just disappears from the registry. Combined with no parse-time validation of the embedded DSL (R-021), a rule can fail to load in production without ever being flagged.
- **Recommendation:** Replace `.ok()?` with `match` and `panic!` / `cargo:warning=` when a TOML in the rules tree fails to parse. Fail the build on any malformed rule — that's the whole point of moving rules to compile time.

---

### High

#### R-006. Tree-sitter `utf8_text(...).unwrap_or("")` pattern silently corrupts data
- **File:** `src/burnt-engine/src/graph/python.rs:79,379-388,407-426,482-506,706-722` (representative — pattern repeats)
- **Category:** panic / data-loss
- **Severity:** High
- **Impact:** Replacing failed UTF-8 extraction with `""` propagates empty identifiers, type names, and string-literal values downstream. The bindings map and method-chain reconstruction then receive silently-wrong data. In production this looks like rule false-negatives, not a panic — much harder to diagnose.
- **Recommendation:** Define a helper `fn text<'a>(n: Node<'_>, src: &'a str) -> Option<&'a str>` and propagate `None` upward (or return `Result<_, GraphBuildError>`). Reserve `.unwrap_or("")` only for places where empty is semantically meaningful.

#### R-007. `graph/python.rs` panics on tree-sitter grammar/parse failure with `expect()`
- **File:** `src/burnt-engine/src/graph/python.rs:37,40`
- **Category:** panic
- **Severity:** High
- **Impact:** `.expect("tree-sitter-python grammar failed to load")` and `.expect("tree-sitter failed to parse")` panic the whole process, killing the parent `analyze_directory` rayon worker pool.
- **Recommendation:** Return `Result<(Vec<Node>, Vec<Edge>, Vec<Finding>), GraphBuildError>` from the Python builder entry point. Surface parse failures as findings with a `parse_failure` rule code.

#### R-008. `plan_parser.rs` panics on `.find()` over external Spark JSON
- **File:** `src/burnt-engine/src/plan_parser.rs:137,274,276`
- **Category:** panic / security
- **Severity:** High
- **Impact:** Untrusted Spark plan JSON drives `.unwrap()` on `find()` results. A malformed plan from a non-malicious mis-version causes a panic across the FFI boundary; a malicious response can DoS the analyser.
- **Recommendation:** Replace `.unwrap()` with `.ok_or(PlanParseError::MissingNode { ... })?`. Pair with R-009 to also bound input size.

#### R-009. No size/depth limit on Spark plan node arrays
- **File:** `src/burnt-engine/src/plan_parser.rs:190-215`, `src/burnt-engine/src/resolved/python.rs:528-565`
- **Category:** security
- **Severity:** High
- **Impact:** `Vec::with_capacity(arr.len())` without bounds; a plan with 10⁹ nodes triggers OOM. Spark plans from production are typically <10k nodes; a 10⁶ cap is conservative.
- **Recommendation:** Add `const MAX_PLAN_NODES: usize = 100_000; if arr.len() > MAX_PLAN_NODES { return Err(...) }`. Same for `metricValues`. Document the limit.

#### R-010. `session/rest_client.rs` builds with `Client::builder()` then silently drops the configured client on failure
- **File:** `src/burnt-engine/src/session/rest_client.rs:24-28`
- **Category:** security / error-handling
- **Severity:** High
- **Impact:** `.build().unwrap_or_else(|_| Client::new())` — if the builder fails, the fallback `Client::new()` has **no timeout** and **default TLS**. Hung requests then block indefinitely; security policy of the configured client (cipher selection, root certs) is lost.
- **Recommendation:** Return `Result<Self, EngineError>` from `RestClient::new` and let callers handle failure. There is no scenario in which `Client::builder()` failing should silently downgrade security.

#### R-011. No explicit TLS verification policy in `RestClient`
- **File:** `src/burnt-engine/src/session/rest_client.rs:24-28`
- **Category:** security
- **Severity:** High
- **Impact:** rustls defaults to validating certs, so this is **safe today** — but the policy is implicit. A future contributor adding `danger_accept_invalid_certs(true)` for local debugging won't see a guard against it.
- **Recommendation:** Add `.use_rustls_tls().https_only(true)` and a doc comment stating the policy. Reject http schemes for production catalog URLs.

#### R-012. Reqwest errors propagated raw — may leak auth tokens in upstream logs
- **File:** `src/burnt-engine/src/session/mod.rs:195`
- **Category:** security
- **Severity:** High
- **Impact:** `Err(e)` from `reqwest` carries the request URL (which can include a query-string token) and sometimes the response body. Both go into `partial_errors`, eventually surfacing in Python logs.
- **Recommendation:** Map `reqwest::Error` → `EngineError::Http { status, endpoint: stripped_path }` and never include full URLs. Strip query strings; truncate response bodies to status-line text only.

#### R-013. Catalog URL constructed by `format!` — no URL encoding on `table_fqn`
- **File:** `src/burnt-engine/src/catalog/databricks.rs:64-67`
- **Category:** security
- **Severity:** High
- **Impact:** A table name containing `..`, `/`, or URL-special characters is concatenated directly into the REST URL. Combined with case-sensitivity assumptions elsewhere, this is a path-confusion surface.
- **Recommendation:** Use `urlencoding::encode_segment` or build URLs via `reqwest::Url::join`. Validate that `table_fqn` matches `^[A-Za-z0-9_.]+$` (Unity Catalog naming rules).

#### R-014. Ingestion does not canonicalise paths or guard against symlink traversal
- **File:** `src/burnt-engine/src/ingestion/files.rs:14-25`
- **Category:** security
- **Severity:** High
- **Impact:** `analyze_directory(path)` walks symlinks. A `.lnk`-style symlink in a project directory could escape the project root. Less severe in a local-CLI context, but the engine is positioned to run inside services.
- **Recommendation:** `fs::canonicalize(path)?` once at entry, then enforce that every walked file's canonical path stays under the root. Use `walkdir` with `follow_links(false)`.

#### R-015. Ingestion has no file-size cap
- **File:** `src/burnt-engine/src/ingestion/files.rs:21-22`
- **Category:** security
- **Severity:** High
- **Impact:** `fs::read_to_string()` will happily slurp a 10 GB file into RAM.
- **Recommendation:** `fs::metadata()` first; reject anything > a documented cap (e.g. 32 MiB for `.py`/`.sql`, larger for `.ipynb`). Surface as a `Finding`, don't panic.

#### R-016. Variable shadowing overwrites bindings — downstream edges point to wrong node
- **File:** `src/burnt-engine/src/graph/python.rs:91-94`
- **Category:** graph-soundness
- **Severity:** High
- **Impact:** When the Python builder sees `df = ...; df = ...`, the second assignment overwrites `bindings["df"]` and previously-created edges that used `df`'s old binding now resolve incorrectly. Common pattern in real notebooks.
- **Recommendation:** Track binding history per scope; resolve references by lookup at the time the reference is encountered, not by later mutation. Or: store the node_id at edge-creation time, not the variable name.

#### R-017. Table-reference dedup is case-sensitive; SQL identifiers are case-insensitive in Databricks
- **File:** `src/burnt-engine/src/graph/sql.rs:402-412`, `src/burnt-engine/src/graph/sdp.rs:218-226`
- **Category:** graph-soundness
- **Severity:** High
- **Impact:** `cat.SCH.t` and `cat.sch.T` produce two `TableRef` entries instead of one — duplicate edges, inflated cost estimates, and confusion in deduplication-dependent rules.
- **Recommendation:** Normalise `TableRef` keys to lowercase **before** insertion into the dedup map. Add a `TableRef::canonical_key()` method and use it everywhere a `TableRef` is hashed.

#### R-018. PyO3 `PyResolvedGraph::graph()` clones the entire graph on every getter call
- **File:** `src/burnt-engine/src/resolved/python.rs:262-264`
- **Category:** perf
- **Severity:** High
- **Impact:** Python access pattern `g.graph.nodes` triggers a full deep clone of nodes + edges + AST + scope. For 1000-node graphs this is measurable; for the worst real notebooks (~10k nodes), it dominates.
- **Recommendation:** Wrap the canonical graph in `Arc<Graph>` and return a cheap clone of the `Arc`. Or cache the `PyGraph` materialised form once and hand out clones of a `Py<PyGraph>`.

#### R-019. `predicate.rs` recompiles regexes on every predicate call
- **File:** `src/burnt-engine/src/rules/graph_dsl/predicate.rs:355-365`
- **Category:** perf
- **Severity:** High
- **Impact:** `pred_match()` calls `Regex::new(&rhs)` per evaluation. A rule running over N nodes with a stable regex pattern recompiles N times.
- **Recommendation:** Cache compiled regexes in a `OnceLock<RwLock<HashMap<String, Regex>>>` keyed by pattern string, or hoist regex compilation into the `CompiledRule` at registry-build time when the pattern is a literal.

#### R-020. `pred_when` kwarg parser is a hand-rolled `while i < args.len()` with no validation
- **File:** `src/burnt-engine/src/rules/graph_dsl/predicate.rs:1013-1042`
- **Category:** dsl-syntax / error-handling
- **Severity:** High
- **Impact:** Malformed `:key value` pairs silently slip through — an odd number of trailing args is just dropped. Rules with typos in `:key` names match more or less broadly than intended, with no diagnostic.
- **Recommendation:** Extract `fn parse_kwargs(args: &[PredArg]) -> Result<HashMap<String, CaptureValue>, DslError>` and require even-arity after the trigger. Reject unknown keys against the `#when` schema.

#### R-021. Head-kind and predicate names validated only at match time
- **File:** `src/burnt-engine/src/rules/graph_dsl/matcher.rs:582-593`, `src/burnt-engine/src/rules/graph_dsl/predicate.rs:255-261`
- **Category:** dsl-syntax
- **Severity:** High
- **Impact:** A typo like `(op:Trnasform ...)` or `#in-loop-x?` parses fine, then silently matches zero nodes / always returns false. Combined with R-005 (silent TOML drop), entire rules can be effectively disabled without anyone noticing.
- **Recommendation:** Build a static `KNOWN_OP_KINDS` set and a static predicate-name set in `build.rs`. Validate every rule's parsed pattern against both — fail the build on unknown identifiers.

#### R-022. Duplicate predicate registration: `shares-receiver` registered twice, second silently shadows the first
- **File:** `src/burnt-engine/src/rules/graph_dsl/predicate.rs:134,177`
- **Category:** consistency
- **Severity:** High
- **Impact:** A reader looking at the stub `pred_shares_receiver` at line 134 sees one definition; the real one at 177 wins. Maintenance hazard, and confirms that the registry has no collision check.
- **Recommendation:** Remove the stub. Add a debug assertion in the registry builder that names are unique. Better: make the registry a `phf::Map` so duplicates fail to compile.

#### R-023. `TableRef` allows internally inconsistent state (`is_temp_view && is_path_read`, `is_path_read && path.is_none()`)
- **File:** `src/burnt-engine/src/types.rs` (relevant fields are `pub`)
- **Category:** graph-soundness
- **Severity:** High
- **Impact:** Public fields plus no constructor invariant means builders can produce nonsense `TableRef`s. Downstream rules that switch on `is_path_read` may panic or silently mis-classify.
- **Recommendation:** Make fields private, expose `TableRef::new_named(...)` / `::new_temp(...)` / `::new_path(...)` constructors. Add a `debug_assert` `validate()` method.

#### R-024. SQL builder swallows parse errors and returns an empty graph with no signal
- **File:** `src/burnt-engine/src/graph/sql.rs:18-21,94-98`
- **Category:** error-handling
- **Severity:** High
- **Impact:** `let Ok(statements) = Parser::parse_sql(...) else { return Vec::new(); };` — a syntactically invalid SQL file produces *no findings* and *no warning*. Looks identical to a file with no issues.
- **Recommendation:** Track parse failures in a `Vec<Finding>` (the Python builder already does this) and return a `parse_failure` rule code (e.g. `BN999`) that surfaces in the report.

#### R-025. `analyze_source` rebuilds the graph that `check()` just built
- **File:** `src/burnt-engine/src/lib.rs:129-148,172-173`
- **Category:** perf
- **Severity:** High
- **Impact:** `analyze_source(src)` calls `check(src)` (parses, builds graph) and then calls `build_graph_and_pipeline(src)` (parses, builds graph again). For large notebooks this is double work in the hottest path.
- **Recommendation:** Refactor `check()` to return the built graph; or have `analyze_source` cache the parse tree and graph between calls.

#### R-026. `resolved/python.rs` clones table specs and metrics on every getter
- **File:** `src/burnt-engine/src/resolved/python.rs:39-46,287-291`
- **Category:** perf
- **Severity:** High
- **Impact:** `PyPlanSubtreeNode::metrics()` rebuilds the `PyDict` per call. `PyResolvedGraph::table_specs()` clones every spec including its `partition_columns` vector. Both compound when Python iterates these collections.
- **Recommendation:** Materialise once at construction and store as `Py<PyDict>` / `Vec<Py<PyTableSpec>>`. Use the same pattern as `PyPlanNode` does at `plan_parser.rs:105,121`.

#### R-027. SDP source-type assignment overwrites instead of accumulating
- **File:** `src/burnt-engine/src/graph/sdp.rs:201-275`
- **Category:** graph-soundness
- **Severity:** High
- **Impact:** A function containing both `sdp.read()` and `LIVE.x` references ends up with whichever source-type was assigned last. Rules keying on source type see only one of the two.
- **Recommendation:** Store `source_types: SmallVec<[SdpSourceType; 2]>` or a `bitflags` set. Update predicates that check source type to use `.contains()` rather than `==`.

#### R-028. SDP table-arg extraction uses raw string splitting instead of the AST
- **File:** `src/burnt-engine/src/graph/sdp.rs:209-228`
- **Category:** graph-soundness
- **Severity:** High
- **Impact:** `args_text.trim_matches(...).split(',').next()` will mis-parse anything fancier than `sdp.read('lit_name')` — string-formatted names, kwargs, list/dict args all break.
- **Recommendation:** Reuse `graph/python.rs::extract_call_ast()` and walk the actual AST. The pure-string path is a long-tail correctness debt.

#### R-096. `writing-rules.md` does not document the graph DSL tier — all 111 active rules use it
- **File:** `docs/writing-rules.md` (entire file)
- **Category:** dsl-docs
- **Severity:** High
- **Impact:** The rule-author guide describes only `[query]`, `[context]`, and `[dataflow]` detection tiers. The fourth tier, `[graph]`, is the *only* one actually used by all 111 currently-shipping rules. A new rule author following the docs writes the wrong kind of rule.
- **Recommendation:** Add a major section "Detection: Tier 4 — Graph DSL Patterns" covering: (a) `[graph]` block structure, (b) reference to `dsl-reference.md` for syntax, (c) a worked example (e.g. BN002 dynamic-SQL), (d) capture-name flow into `[graph.finding]` templates. Mark the legacy tiers as "deprecated, retained for reference."

---

### Medium

#### R-029. `cargo doc` has 4 broken intra-doc links
- **Files:**
  - `src/burnt-engine/src/resolved/scope_facts.rs:17` — `[ImportMap]` private
  - `src/burnt-engine/src/resolved/mod.rs:71` — `CatalogClient` not in scope
  - `src/burnt-engine/src/rules/graph_dsl/parser.rs:3` — `lexer` private
  - `src/burnt-engine/src/types.rs:452` — `CatalogClient` not in scope
- **Category:** consistency
- **Severity:** Medium
- **Impact:** Docs build is noisy; will fail under `RUSTDOCFLAGS=-D warnings` which is a common CI pattern.
- **Recommendation:** Make `lexer` re-exported from the DSL module, qualify `CatalogClient` with its full path, or change to plain back-ticks where the link is informational.

#### R-030. `cargo clippy -W clippy::pedantic` emits ~600 warnings — mostly `format!` modernisation
- **File:** entire crate
- **Category:** idiom
- **Severity:** Medium
- **Impact:** The top 5 lints account for the majority:
  - 44 × `unnecessary structure name repetition` (e.g. `Foo::Foo { ... }`)
  - 41 × `this could be a const fn`
  - 41 × `item in documentation is missing backticks`
  - 28 × `this method could have a #[must_use] attribute`
  - ~325 × `variables can be used directly in the format! string` (i.e. `format!("{}", x)` → `format!("{x}")`)
- **Recommendation:** Adopt `pedantic` selectively. The `format!` lint can be fixed crate-wide in one PR via `cargo clippy --fix`. The `const fn` and `must_use` additions tighten the public API without behaviour changes. Avoid `module_name_repetitions` (already excluded).

#### R-031. Clippy `unsafe_derive_deserialize` triggered on PyO3-generated unsafe code
- **File:** `src/burnt-engine/src/types.rs:315,338,347,363`
- **Category:** consistency
- **Severity:** Medium
- **Impact:** Lint is technically a false positive (PyO3 macros expand to `unsafe` for FFI; not user-written), but it appears 4× in the clippy output. Will distract reviewers in CI logs.
- **Recommendation:** `#[allow(clippy::unsafe_derive_deserialize)]` at the type-level on the affected enums, with a comment explaining the PyO3 interaction.

#### R-032. `lib.rs` uses `.unwrap_or_default()` to convert rule failures to empty findings
- **File:** `src/burnt-engine/src/lib.rs:158,189`
- **Category:** error-handling
- **Severity:** Medium
- **Impact:** Distinguishes "rules ran cleanly with 0 findings" from "rules failed and we ate the error" — but callers cannot tell which happened.
- **Recommendation:** Return `Result<Vec<Finding>, EngineError>` from the public function, or surface the error as a synthetic `engine_panic` finding.

#### R-033. `auth.and_then(|s| HeaderValue::from_str(s).ok())` silently drops invalid auth tokens
- **File:** `src/burnt-engine/src/session/rest_client.rs:29`
- **Category:** security / error-handling
- **Severity:** Medium
- **Impact:** A token with stray whitespace or a control character is dropped silently; the request goes unauthenticated; the server returns 401; the caller blames the token.
- **Recommendation:** Return `Result<Self, EngineError>` from `RestClient::new` and report `InvalidAuthHeader`. Never silently degrade authentication.

#### R-034. `RestClient` HTTP errors not classified — 401 / 403 / 5xx all look the same
- **File:** `src/burnt-engine/src/session/rest_client.rs:46-56`
- **Category:** error-handling
- **Severity:** Medium
- **Impact:** Operators can't distinguish auth failure from rate limit from cluster shutdown. Retry logic (when added) won't have signal.
- **Recommendation:** Inspect `response.status()` before `error_for_status()` and map to `EngineError::HttpAuth` / `::HttpRateLimit` / `::HttpTransient` / `::HttpClient`.

#### R-035. `RestClient` has no response-size limit
- **File:** `src/burnt-engine/src/session/rest_client.rs:36-44`
- **Category:** security
- **Severity:** Medium
- **Impact:** A misbehaving Spark master returning a 10 GB response will exhaust memory.
- **Recommendation:** Use `Response::bytes_stream()` with a size guard, or set `Client::builder().http2_max_frame_size(...)` and chunked reading.

#### R-036. Multiple stages with the same `sql_exec_id` silently picks the first (no tie-breaker)
- **File:** `src/burnt-engine/src/resolved/merge.rs:177-179`
- **Category:** graph-soundness
- **Severity:** Medium
- **Impact:** When Spark emits multiple stages for the same SQL execution, the overlay attaches metrics from an arbitrary one. Cost estimates become non-deterministic in re-runs.
- **Recommendation:** Pick by minimum `abs_diff(stage.source_line, node.line_number)`. Document the tie-break rule.

#### R-037. Overlay lookup may fail silently if graph mutated after index
- **File:** `src/burnt-engine/src/resolved/merge.rs:173,208`
- **Category:** graph-soundness
- **Severity:** Medium
- **Impact:** `if let Some(overlay) = overlays.get_mut(node_id)` — if the node was removed between indexing and lookup, the stage data is silently dropped.
- **Recommendation:** Use `debug_assert!(overlays.contains_key(node_id))` or convert to explicit error since every graph node should have an overlay entry per the construction at line 158.

#### R-038. Empty plan node list defaults `root` to node-id 0 — dangling reference
- **File:** `src/burnt-engine/src/resolved/merge.rs:196-202`
- **Category:** graph-soundness
- **Severity:** Medium
- **Impact:** When a plan bundle has nodes but no clear root (all nodes have parents — possible with cycles), `root` becomes 0, which may not exist. Consumers iterating from root traverse nothing or wrong nodes.
- **Recommendation:** Use `Option<NodeId>` or a sentinel `-1` and document the contract. Better: detect cycles at parse time and reject the plan.

#### R-039. Recursive CTEs aren't marked recursive — graph has unmarked cycles
- **File:** `src/burnt-engine/src/graph/sql.rs:29-54`
- **Category:** graph-soundness
- **Severity:** Medium
- **Impact:** `WITH RECURSIVE t AS (... t ...)` produces a self-referencing `TableRef`. Rules that walk dependencies can loop. No cycle detector exists.
- **Recommendation:** Add `is_recursive: bool` (or an `EdgeKind::Recursive`) when sqlparser tags the CTE recursive. Or: add a single cycle-detection pass at the end of `Graph::from_sql`.

#### R-040. Graph has no post-construction validation
- **File:** `src/burnt-engine/src/graph/mod.rs:25-70`
- **Category:** graph-soundness
- **Severity:** Medium
- **Impact:** Dangling edges (source/target id doesn't exist), orphaned nodes, duplicate ids — none detected. The graph builders trust themselves.
- **Recommendation:** Add `Graph::validate()` returning `Result<(), GraphInvariantError>`. Call it once at the end of each `from_*` constructor under `debug_assertions`.

#### R-041. `aliases` are not tracked in any builder — `t1.col` cannot be resolved back to `table_a`
- **File:** `src/burnt-engine/src/graph/{python,sql,sdp}.rs`
- **Category:** graph-soundness
- **Severity:** Medium
- **Impact:** Rules that reason about which column came from which physical table cannot do so when aliases are used.
- **Recommendation:** Extend `TableRef` with `alias: Option<String>` and populate from `TableFactor::Table { alias, .. }` in SQL.

#### R-042. Cell-line accumulation in `parse/notebooks.rs` clones every line twice
- **File:** `src/burnt-engine/src/parse/notebooks.rs:94,99-100`
- **Category:** perf
- **Severity:** Medium
- **Impact:** Each line is cloned into `current_cell_lines`, then cloned again in `join("\n")`. For notebooks with megabytes of code this is wasteful.
- **Recommendation:** Track byte ranges into the original buffer; reconstruct cell source by slicing. Or use `Cow<str>`.

#### R-043. UTF-8 byte-offset arithmetic assumes single-byte newlines and may diverge from tree-sitter offsets
- **File:** `src/burnt-engine/src/parse/notebooks.rs:66-67`
- **Category:** consistency
- **Severity:** Medium
- **Impact:** `current_byte_offset += line.len() as u32 + 1;` — `line.len()` is byte length (correct), `+1` for `\n` (works for `\n` but not `\r\n`). Windows-line-ending notebooks will produce off-by-one byte offsets vs tree-sitter.
- **Recommendation:** Read the file in binary, track newline width per line.

#### R-044. `parse/import_map.rs::collect_imports` recurses without depth bound
- **File:** `src/burnt-engine/src/parse/import_map.rs:310-332`
- **Category:** security
- **Severity:** Medium
- **Impact:** Pathologically deep AST (deeply nested decorators or unusual syntax) could overflow the stack on default thread sizes.
- **Recommendation:** Convert to iterative walk with an explicit stack, or pass a `depth: usize` and bail at 1000.

#### R-045. Decorator detection in `detect.rs::is_dlt_decorator` indexes `text[at_pos + 1..]` without bounds
- **File:** `src/burnt-engine/src/detect.rs:67-79`
- **Category:** panic
- **Severity:** Medium
- **Impact:** A decorator line `"@"` (just the at-sign, end of line) makes `at_pos + 1 == text.len()`, which is valid — but combined with `.find('@')` returning `usize::MAX` on no-match the arithmetic is suspicious.
- **Recommendation:** Restructure with `text.find('@').and_then(|i| text.get(i + 1..))` so unsafe indexing is impossible.

#### R-046. `parser.rs` predicate-vs-nested-pattern disambiguation depends on one-token lookahead
- **File:** `src/burnt-engine/src/rules/graph_dsl/parser.rs:147-157`
- **Category:** dsl-syntax
- **Severity:** Medium
- **Impact:** The decision rule is "if `tokens[pos+1]` is a `#hash`, it's a predicate, else it's a nested pattern." Works today but extending the grammar (e.g. adding bare-identifier predicates) silently breaks it.
- **Recommendation:** Move to explicit grammar productions, or document the lookahead invariant prominently in `parser.rs`.

#### R-047. Edge kinds are ad-hoc strings — no `EdgeKind` enum
- **File:** `src/burnt-engine/src/graph/sql.rs:231,273,291`, and elsewhere
- **Category:** consistency
- **Severity:** Medium
- **Impact:** `"data_flow"`, `"table_dependency"`, `"sdp_read"`, `"live_ref"` — typos compile, schema drifts silently across builders.
- **Recommendation:** Define `pub enum EdgeKind { DataFlow, TableDependency, SdpRead, LiveRef, ... }` in `types.rs` and use it everywhere.

#### R-048. Node-by-id lookup is linear in `graph/python.rs`
- **File:** `src/burnt-engine/src/graph/python.rs:310-314,318-321`
- **Category:** perf
- **Severity:** Medium
- **Impact:** `push_table_ref()` and `set_ast()` do O(n) linear scans on each call. For 1000-node graphs and ~10 ref-pushes per node, that's 10⁷ comparisons.
- **Recommendation:** Maintain `HashMap<String, usize>` alongside the `Vec` for O(1) lookup.

#### R-049. `DatabricksCatalogClient::cache` is per-instance — no cross-call dedup
- **File:** `src/burnt-engine/src/catalog/databricks.rs:57-87`
- **Category:** perf
- **Severity:** Medium
- **Impact:** Each `analyze_*` call with catalog enrichment builds a fresh client and a fresh cache; same tables are fetched repeatedly across files.
- **Recommendation:** Either pass the same `Arc<DatabricksCatalogClient>` across calls (requires API change), or back the cache with a process-global `OnceLock<Mutex<HashMap<...>>>`. Document TTL.

#### R-050. Mutex poisoning is silently absorbed
- **File:** `src/burnt-engine/src/catalog/databricks.rs:59,84`
- **Category:** consistency
- **Severity:** Medium
- **Impact:** `.lock().unwrap_or_else(|p| p.into_inner())` hides the panic that poisoned the mutex. The next caller proceeds with potentially-inconsistent cache state.
- **Recommendation:** Either use `parking_lot::Mutex` (panic-safe, no poisoning), or clear-poison + log the originating panic location.

#### R-051. Spark plan response schema not validated
- **File:** `src/burnt-engine/src/plan_parser.rs:190-215`
- **Category:** security / error-handling
- **Severity:** Medium
- **Impact:** Missing required fields are filled with defaults via `#[serde(default)]`. A malformed plan looks like an empty plan.
- **Recommendation:** Mark critical fields non-`default` and let serde fail; surface as an `EngineError::PlanSchema`.

#### R-052. Test code panics on missing fixture nodes rather than asserting helpfully
- **File:** `src/burnt-engine/src/plan_parser.rs:274,276`
- **Category:** tests
- **Severity:** Medium
- **Impact:** When the fixture format changes, tests panic with `unwrap()` rather than a descriptive failure.
- **Recommendation:** `.expect("expected ReusedExchange node in plan")` at minimum.

#### R-053. One declared dependency is unused: `streaming-iterator` (`tree-sitter-sequel` retained for R-109)
- **File:** `src/burnt-engine/Cargo.toml`
- **Category:** deps
- **Severity:** Medium
- **Impact:** `streaming-iterator` is declared but has zero references in the engine — adds compile time and wheel size for no value. `tree-sitter-sequel` is currently also unused **but is the SQL grammar from `github.com/DerekStride/tree-sitter-sql`** (crate-name ≠ repo-name) and should be adopted, not removed — see R-109 (Critical).
- **Recommendation:** Remove `streaming-iterator = "0.1"` from `[dependencies]`. Keep `tree-sitter-sequel`; track its adoption against R-109.

#### R-054. `thiserror = 2.0` adoption is partial — most public APIs still return `Result<_, String>`
- **File:** `src/burnt-engine/src/lib.rs`, `src/burnt-engine/src/ingestion/files.rs`, `src/burnt-engine/src/ingestion/dabs.rs`, `src/burnt-engine/src/rules/mod.rs`
- **Category:** error-handling / deps
- **Severity:** Medium
- **Impact:** `String` errors lose context (no source chain), are not type-safe, and complicate FFI mapping to typed Python exceptions (R-004).
- **Recommendation:** Introduce a top-level `error.rs` with `pub enum EngineError { ... }` using `#[derive(thiserror::Error)]`. Migrate public surfaces first; let internal `Result<_, String>` stay until they bubble up.

#### R-055. PyO3 is at `0.22` without `abi3` — wheels rebuild per Python minor version
- **File:** `src/burnt-engine/Cargo.toml`, `pyproject.toml`
- **Category:** deps
- **Severity:** Medium
- **Impact:** Each new Python minor release (3.13 → 3.14, etc.) requires a fresh wheel build matrix. With `abi3` enabled, a single `cp310-abi3` wheel works for 3.10+.
- **Recommendation:** Add `features = ["extension-module", "abi3-py310"]` to `pyo3`. Verify public surface is `abi3`-compatible (no PyType internals).

#### R-056. `maturin` config does not declare `abi3`
- **File:** `pyproject.toml`
- **Category:** deps
- **Severity:** Medium
- **Impact:** Goes with R-055; without `[tool.maturin].python-source` and the `abi3` annotation, the wheel-build matrix is per-Python.
- **Recommendation:** Add the corresponding maturin config block once R-055 is applied.

#### R-057. `notebooks.rs` extension matching is case-sensitive — clippy flags 5 sites
- **File:** `src/burnt-engine/src/parse/notebooks.rs:17,23,24,29`
- **Category:** consistency
- **Severity:** Medium
- **Impact:** `.IPYNB`, `.PY`, `.SQL` files are skipped. Real notebooks (especially Windows-authored) sometimes have uppercase extensions.
- **Recommendation:** `path.to_lowercase().ends_with(".ipynb")` or use `Path::extension().and_then(|e| e.to_str()).map(str::to_ascii_lowercase)`.

#### R-097. `:method-chain` property is implemented and tested but not in the `dsl-reference.md` Properties table
- **File:** `docs/dsl-reference.md:107-122`; cross-ref `src/burnt-engine/src/rules/graph_dsl/parser.rs:522-539`
- **Category:** dsl-docs
- **Severity:** Medium
- **Impact:** `(ast/Call :method-chain ["spark" "sql"] ...)` is a real, working pattern (see parser tests) but is undiscoverable from the docs. Rule authors will reach for less-precise alternatives (regex over source).
- **Recommendation:** Add `:method-chain` to the `ast/Call` Properties table with value type "List of String" and an example.

#### R-098. Grammar BNF in `dsl-reference.md` does not show the `@name.line` / `@name.column` field-suffix syntax
- **File:** `docs/dsl-reference.md:9-24` (Grammar) vs. `docs/dsl-reference.md:335-346` (Finding Overrides) and `src/burnt-engine/src/rules/graph_pipeline.rs:1067-1080`
- **Category:** dsl-docs
- **Severity:** Medium
- **Impact:** The grammar declares `capture-ref := "@" IDENT`, but `[graph.finding].line` accepts `@call.line` and `@call.column` — a feature the BNF does not describe.
- **Recommendation:** Either extend the BNF (`capture-ref := "@" IDENT ("." ("line" | "column"))?`) or add a clearly-marked "Field-suffix syntax (`[graph.finding]` only)" subsection.

#### R-099. `#column-of` is documented as returning the column number but is a no-op stub
- **File:** `docs/dsl-reference.md:231` vs. `src/burnt-engine/src/rules/graph_dsl/predicate.rs:952-956`
- **Category:** dsl-docs
- **Severity:** Medium
- **Impact:** Rules invoking `(#column-of @n)` will silently fail (return `Nil`), with the docs giving the author confidence the predicate works.
- **Recommendation:** Either implement `#column-of` (requires `AstNode` column metadata) or remove from `dsl-reference.md` until implementation lands. Mark in docs as "Not yet implemented — see issue #N."

#### R-100. Parser-test syntax for `:method-chain` is suggestive of a list-valued property feature that isn't documented anywhere else
- **File:** `src/burnt-engine/src/rules/graph_dsl/parser.rs:522-539`
- **Category:** dsl-docs
- **Severity:** Medium
- **Impact:** Either the feature is intended (then docs are incomplete — see R-097) or the test asserts on an aspirational/incomplete feature (then the test misleads readers). In either case there is drift.
- **Recommendation:** Decide which it is. If intended, document. If aspirational, gate behind a `#[ignore]`-style marker or remove until ready.

---

### Low

#### R-058. `semantic/mod.rs` scope stack is dead code — `push_scope`/`pop_scope`/`get_bindings` never called
- **File:** `src/burnt-engine/src/semantic/mod.rs:49-64,100-103`
- **Category:** dead-code
- **Severity:** Low
- **Impact:** `#[allow(dead_code)]` masks an unused subsystem. The bindings map is flat in practice — scope hierarchy is constructed but never consulted.
- **Recommendation:** Either remove the methods and the `scopes: Vec<...>` field, or implement scope-aware binding resolution (which the Python builder needs anyway — see R-016).

#### R-059. `parse/import_map.rs` has 3 `#[allow(dead_code)]` items (`DecoratorKind`, `local_name`, `member`)
- **File:** `src/burnt-engine/src/parse/import_map.rs:74,94,99`
- **Category:** dead-code
- **Severity:** Low
- **Impact:** Carries data per binding for no current consumer. Allocates strings per import.
- **Recommendation:** Remove the fields if they aren't planned, or wire a TODO with an issue link if they are.

#### R-060. `lib.rs` clones `cg.findings` into the result tuple when it could move
- **File:** `src/burnt-engine/src/lib.rs:144`
- **Category:** perf
- **Severity:** Low
- **Impact:** Small but on the hot path.
- **Recommendation:** Replace `cg.findings.clone()` with `std::mem::take(&mut cg.findings)` or restructure to consume `cg` directly.

#### R-061. `lib.rs` mixes `.to_string()` and `String::from()` inconsistently
- **File:** `src/burnt-engine/src/lib.rs:39,181`
- **Category:** idiom
- **Severity:** Low
- **Impact:** Cosmetic; clippy's `pedantic::useless_conversion` doesn't catch all of these.
- **Recommendation:** Pick one (`.to_owned()` is the closest to "no semantics, just a copy") and standardise.

#### R-062. `lib.rs` allocates `PathBuf` per cell during ingestion
- **File:** `src/burnt-engine/src/lib.rs:169`
- **Category:** perf
- **Severity:** Low
- **Impact:** For analyse_directory with many files, this is unnecessary.
- **Recommendation:** Reference the source path; `Arc<Path>` if shared.

#### R-063. `json_py.rs` silently coerces invalid f64 to 0.0
- **File:** `src/burnt-engine/src/json_py.rs:26`
- **Category:** error-handling
- **Severity:** Low
- **Impact:** `serde_json::Number::as_f64()` is always `Some` for valid JSON — the `.unwrap_or(0.0)` is unreachable. But the fallback hides any future invariant violation.
- **Recommendation:** Use `.expect("serde_json::Number always convertible to f64")` to fail loudly if invariant breaks.

#### R-064. `build.rs` lacks per-file `rerun-if-changed` for rule TOMLs
- **File:** `src/burnt-engine/build.rs:25-26`
- **Category:** build
- **Severity:** Low
- **Impact:** Modifying a single rule TOML may not always re-run the build script on all platforms; directory-watching is not granular on every filesystem.
- **Recommendation:** Emit `println!("cargo:rerun-if-changed={path}")` for each discovered TOML during the walk.

#### R-065. `build.rs` does not currently validate `[graph].detect` DSL syntax — see R-021
- **File:** `src/burnt-engine/build.rs:122-129`
- **Category:** build
- **Severity:** Low (already covered by R-021 at higher severity)

#### R-066. Inconsistent node-id prefixes across builders (`node_N`, `sql_node_N`, `sdp_table_N`)
- **File:** `src/burnt-engine/src/graph/python.rs:275`, `src/burnt-engine/src/graph/sql.rs:430`, `src/burnt-engine/src/graph/sdp.rs`
- **Category:** consistency
- **Severity:** Low
- **Impact:** A merged graph (multiple files) risks id collisions if Python and SQL builders are mixed and the source-mode prefix is dropped.
- **Recommendation:** Adopt a single scheme (`py:N`, `sql:N`, `sdp:N`) or a UUID prefix per builder invocation.

#### R-067. AST shape attached coarsely in SQL builder vs per-node in Python builder
- **File:** `src/burnt-engine/src/graph/sql.rs:108-112`
- **Category:** consistency
- **Severity:** Low
- **Impact:** Rules that introspect AST get different granularity by language. Predicates written for Python may not have node-local AST when run against SQL.
- **Recommendation:** Document the design choice in the module rustdoc; consider per-node AST in SQL when statements have multiple operations.

#### R-068. `predicate.rs::evaluate_inner_with_it` clones the full `CaptureMap` per quantifier item
- **File:** `src/burnt-engine/src/rules/graph_dsl/predicate.rs:531-536`
- **Category:** perf
- **Severity:** Low
- **Impact:** For `#all` / `#any` predicates over large lists, the clone-per-item cost is multiplicative.
- **Recommendation:** Use a `MatchCtx::with_it_override` that borrows the parent capture map and adds a single override layer; commit on success.

#### R-069. `predicate.rs` has multiple stub predicates that always return `Bool(false)`
- **File:** `src/burnt-engine/src/rules/graph_dsl/predicate.rs:165,1172-1174,952-956`
- **Category:** dead-code
- **Severity:** Low
- **Impact:** `fires-rule` and `column_of` are placeholders. Rules using them get false-on-everything with no diagnostic.
- **Recommendation:** Either implement, gate behind a `feature = "experimental"`, or remove until the supporting infrastructure (AST column tracking) lands.

#### R-070. `predicate.rs::pred_eq?` vs `pred_eq` naming is confusing
- **File:** `src/burnt-engine/src/rules/graph_dsl/predicate.rs:92,109`
- **Category:** consistency
- **Severity:** Low
- **Impact:** Both look like equality predicates; one is string, one is numeric. The `?` convention isn't applied uniformly.
- **Recommendation:** Rename to `#eq?` (boolean test, any operand) and `#eq-num` (or absorb numeric coercion into `#eq?`).

#### R-071. `:as @cap` binding vs `PredResult::Value` return — two ways to extract a predicate result
- **File:** `src/burnt-engine/src/rules/graph_dsl/predicate.rs:476-485,560-572`
- **Category:** consistency
- **Severity:** Low
- **Impact:** Different value-producing predicates use different mechanisms — rule authors must memorise which is which.
- **Recommendation:** Standardise on `:as @cap` for value-extracting predicates; document in the DSL reference (see DSL docs section).

#### R-072. `_shuffle_node_id` is the only underscore-prefixed binding in the codebase — a code-smell signal that wasn't followed up
- **File:** `src/burnt-engine/src/graph/sql.rs:386` (see R-002)
- **Category:** consistency
- **Severity:** Low (rolled into R-002)

#### R-073. Test fixture helpers `mk_node`, `graph_with`, `stage_at` are boilerplate-heavy
- **File:** `src/burnt-engine/src/resolved/merge.rs:356-394`
- **Category:** tests
- **Severity:** Info / Low
- **Impact:** Repetitive in tests; doesn't scale as schemas grow.
- **Recommendation:** Move to a `tests/common.rs` module and consider a `Builder` pattern.

#### R-074. `scope_facts.rs:104` uses `u32::try_from(i + 1).unwrap_or(u32::MAX)` where `i` is bounded
- **File:** `src/burnt-engine/src/resolved/scope_facts.rs:103-105`
- **Category:** idiom
- **Severity:** Low
- **Impact:** Misleads readers into thinking overflow is possible.
- **Recommendation:** Document the bound or use `as u32` with a `debug_assert!`.

#### R-075. `ingestion/dabs.rs` is a hard-coded `"Not implemented"` stub
- **File:** `src/burnt-engine/src/ingestion/dabs.rs:22-25`
- **Category:** dead-code
- **Severity:** Low
- **Impact:** Function exists but is unreachable usefully. Misleading API surface.
- **Recommendation:** Either implement (read DABS bundle YAML/TOML) or remove and link to the issue tracking it.

#### R-076. Error messages embed full requested file paths
- **File:** `src/burnt-engine/src/ingestion/files.rs:18,22`
- **Category:** security
- **Severity:** Low
- **Impact:** Stack traces and logs leak directory layout. Minor in CLI use; relevant if engine runs as a service.
- **Recommendation:** Strip to file basename; log full path only at debug level.

#### R-077. `catalog/databricks.rs::UcColumn::nullable` defaults to `true` on missing field
- **File:** `src/burnt-engine/src/catalog/databricks.rs:13-14`
- **Category:** error-handling
- **Severity:** Low
- **Impact:** Missing schema data masquerades as "nullable=true." Could mask catalog issues.
- **Recommendation:** Use `Option<bool>` and decide at the use site; or log a warning when the default fires.

#### R-078. `dsl/error.rs` errors are good but no `Display` impl for IR types
- **File:** `src/burnt-engine/src/rules/graph_dsl/ir.rs`
- **Category:** dsl-syntax
- **Severity:** Low
- **Impact:** Cannot round-trip a parsed pattern back to source for diagnostics or rule-builder UIs.
- **Recommendation:** `impl Display for Pattern { ... }` and add a round-trip test (`parse(p.to_string()) == p`).

#### R-079. Reserved capture names (`__current`, `__fact`) are not enforced
- **File:** `src/burnt-engine/src/rules/graph_dsl/matcher.rs:107-110,252-253,258`
- **Category:** dsl-syntax
- **Severity:** Low
- **Impact:** A user-bound `@__current` collides with the matcher's internal binding; behaviour is undefined.
- **Recommendation:** Reject parse of any capture name starting with `__`.

#### R-080. `pred_table_has_property` returns false on missing catalog data — silently degrades
- **File:** `src/burnt-engine/src/rules/graph_dsl/predicate.rs:1400-1423,1425-1466`
- **Category:** error-handling
- **Severity:** Low
- **Impact:** Rules that require catalog enrichment fire as `false` when the catalog is offline — looks like the rule's condition is met (or not, depending on polarity).
- **Recommendation:** Add a tri-state `PredResult::Indeterminate` for "could-not-evaluate" and have the matcher skip rules that yield it.

#### R-081. `parser.rs` accepts any sequence of `:key value` pairs without arity validation
- **File:** `src/burnt-engine/src/rules/graph_dsl/parser.rs:224-232`
- **Category:** dsl-syntax
- **Severity:** Low
- **Impact:** Odd-arity kwargs silently get partially absorbed (see R-020).
- **Recommendation:** Validate at parse time against a predicate metadata registry.

#### R-082. Unknown overlay kinds in `pred_has_overlay` silently return `false`
- **File:** `src/burnt-engine/src/rules/graph_dsl/predicate.rs:632-638`
- **Category:** dsl-syntax
- **Severity:** Low
- **Impact:** Typo in a `:kind` arg silently makes the predicate `false`.
- **Recommendation:** Validate against `OverlayKind` enum at parse time.

#### R-083. Field-label naming uses both flat (`:method`) and pathed (`:arg/0`) forms with no documented scheme
- **File:** `src/burnt-engine/src/rules/graph_dsl/parser.rs:426-471`, `src/burnt-engine/src/rules/graph_dsl/predicate.rs:314`
- **Category:** dsl-syntax / dsl-docs
- **Severity:** Low
- **Impact:** Rule authors guess at the scheme.
- **Recommendation:** Document conventions in `docs/dsl-reference.md`; add a section "Prop naming".

#### R-084. Edge patterns use kwarg-style props (`:from @a :to @b`) inconsistent with op/ast patterns
- **File:** `src/burnt-engine/src/rules/graph_dsl/matcher.rs:127-189`
- **Category:** dsl-syntax
- **Severity:** Low
- **Impact:** Two binding styles in one DSL. Harder to teach.
- **Recommendation:** Either accept both forms in op/ast patterns too, or document the difference prominently.

#### R-085. `value_to_string` and `coerce_*` helpers are referenced inconsistently
- **File:** `src/burnt-engine/src/rules/graph_dsl/predicate.rs:274-283,439-466`
- **Category:** consistency
- **Severity:** Low
- **Impact:** Some predicates inline coercion; others go through helpers.
- **Recommendation:** Audit `pred_arg_kind_of`, `pred_arg_is_dynamic` and others; route everything through the helpers.

#### R-086. Method-chain prop matches require exact length — no prefix/suffix
- **File:** `src/burnt-engine/src/rules/graph_dsl/matcher.rs:618-631`
- **Category:** dsl-syntax
- **Severity:** Low
- **Impact:** `:method-chain ["read" "parquet"]` doesn't match `["spark" "read" "parquet"]`. Annoying for rule authors.
- **Recommendation:** Add `:method-chain-suffix` / `:method-chain-prefix` predicates.

#### R-087. Test-only `Parser::parse_sql` `.expect("parse")` panics on bad fixtures
- **File:** `src/burnt-engine/src/types.rs:791-798`
- **Category:** tests
- **Severity:** Low
- **Impact:** Acceptable in tests; document briefly.

#### R-101. `BJ001_join_type_mismatch` rule missing from `anti-pattern-rules.md` index
- **File:** `docs/anti-pattern-rules.md:22-26`; rule exists at `src/burnt-engine/rules/join/BJ001_join_type_mismatch.toml`
- **Category:** dsl-docs
- **Severity:** Low
- **Impact:** Operators looking up the rule by code can't find it in the index.
- **Recommendation:** Add the row for BJ001 to the index table. Mark catalog-required rules visibly.

#### R-102. Rule-count comment in `anti-pattern-rules.md` says 110 but there are 111
- **File:** `docs/anti-pattern-rules.md:22,24`
- **Category:** dsl-docs
- **Severity:** Low
- **Impact:** Documentation drift; the `<!-- rule count: 110 -->` HTML comment is meant to be regenerated by `scripts/gen_rule_index.py` but wasn't.
- **Recommendation:** Re-run the script (or run it in CI). Bump to 111 manually if the script is missing.

#### R-103. Capture-ref field syntax (`@cap.line` / `@cap.column`) is implemented but not documented as a feature
- **File:** `docs/dsl-reference.md:21` vs. `src/burnt-engine/src/rules/graph_pipeline.rs:1067-1080`
- **Category:** dsl-docs
- **Severity:** Low
- **Impact:** Companion to R-098; this is the docs-side polish.
- **Recommendation:** A short "Special syntax in `[graph.finding]` field" subsection with examples.

#### R-104. Grammar's `separator := ":" | "/"` line is terse — choice rule belongs in prose
- **File:** `docs/dsl-reference.md:12`
- **Category:** dsl-docs
- **Severity:** Low
- **Impact:** Readers may try `ast:Call` (rejected) or `op/Read` (rejected) because the choice rule isn't explicit.
- **Recommendation:** Add prose: "`op`, `edge`, `overlay`, `fact` use `:`; `ast` uses `/`. Mixing is rejected at parse time."

#### R-105. `fact:source` documentation has only one predicate example (`#match?`)
- **File:** `docs/dsl-reference.md:89-103`
- **Category:** dsl-docs
- **Severity:** Low
- **Impact:** Rule authors don't see the range of predicates that work on string captures (`#contains`, `#in`, `#eq?`).
- **Recommendation:** Add a second worked example using `#contains` over `fact:source`.

#### R-106. `:arg/N` property entry in the Properties table lacks an example
- **File:** `docs/dsl-reference.md:115`
- **Category:** dsl-docs
- **Severity:** Low
- **Impact:** Minor — readers see "Nth arg" but not how to use it in a pattern.
- **Recommendation:** Add `(ast/Call :method "sql" :arg/0 @query)` as an inline example.

---

### Info

#### R-088. Build profile (`lto=true, codegen-units=1, strip=true`) is correct for shipped wheels — note build-time cost
- **File:** `src/burnt-engine/Cargo.toml`
- **Category:** deps
- **Severity:** Info
- **Recommendation:** Consider conditional `release-dev` profile with `codegen-units=16` for local iteration.

#### R-089. `cargo audit` is not installed in CI — install for ongoing security
- **File:** `pyproject.toml` / CI workflow (not present)
- **Category:** security
- **Severity:** Info
- **Recommendation:** Add `cargo install cargo-audit && cargo audit` to CI; gate the build on it.

#### R-090. No CI workflow checked into the repo
- **File:** `.github/workflows/` (absent)
- **Category:** consistency
- **Severity:** Info
- **Impact:** README enforces `cargo clippy -- -D warnings` and `cargo test`; without CI this is unenforced on PRs.
- **Recommendation:** Add a minimal `engine.yml` running `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, and `cargo audit`.

#### R-091. `Cargo.lock` checked in is appropriate for a `cdylib` library
- **File:** `src/burnt-engine/Cargo.lock`
- **Category:** deps
- **Severity:** Info
- **Recommendation:** No change. Continue updating via `cargo update` deliberately.

#### R-092. Transitive duplicates: `getrandom 0.2/0.4`, `hashbrown 0.15/0.16`, `windows-sys 0.52/0.59/0.61`, `r-efi 5.3/6.0`
- **File:** `src/burnt-engine/Cargo.lock`
- **Category:** deps
- **Severity:** Info
- **Impact:** Normal in the Rust ecosystem; minor wheel-size cost.
- **Recommendation:** Periodically run `cargo tree -d`; chase upgrades when the source-graph allows.

#### R-093. `regex` crate uses default features (full Unicode) — ~50 KB binary cost for trivial pattern uses
- **File:** `src/burnt-engine/Cargo.toml`
- **Category:** deps
- **Severity:** Info
- **Recommendation:** Consider `regex = { version = "1.10", default-features = false, features = ["unicode-perl"] }` if wheel size becomes a constraint.

#### R-094. `reqwest::blocking` is appropriate here — PyO3 holds the GIL synchronously
- **File:** `src/burnt-engine/Cargo.toml`
- **Category:** deps
- **Severity:** Info
- **Recommendation:** Add a one-line doc comment in `session/rest_client.rs` stating blocking is intentional for PyO3.

#### R-095. `bitflags = 2.6` with `serde` feature is correctly applied
- **File:** `src/burnt-engine/Cargo.toml`
- **Category:** deps
- **Severity:** Info
- **Recommendation:** No change.

#### R-107. `#fires-rule` is documented as a stub — no guidance on usage
- **File:** `docs/dsl-reference.md:249` vs. `src/burnt-engine/src/rules/graph_dsl/predicate.rs:165,1069` (see also R-069)
- **Category:** dsl-docs
- **Severity:** Info
- **Impact:** Authors may try to use a non-functional predicate.
- **Recommendation:** Add "Reserved for future cross-rule dependency tracking; do not use in production rules."

#### R-108. No guidance on capture-name conventions
- **File:** `docs/dsl-reference.md` (entire)
- **Category:** dsl-docs
- **Severity:** Info
- **Impact:** Examples mix `@n`, `@src`, `@call`, `@cap` — readers don't know which is canonical.
- **Recommendation:** Add a one-line convention: "Use descriptive names (`@call`, `@source`) in non-trivial patterns; single-letter captures (`@n`) are acceptable in two-or-three-capture rules."

---

## Section: Graph logic soundness

The graph model is the heart of the engine; this section consolidates findings 001–003, 016–017, 023, 027–028, 036–041, 047 into a narrative.

**Across-language consistency** is the dominant concern. The Python, SQL, and SDP builders independently model the same constructs (reads, transforms, joins, aggregates, writes) but produce graphs that differ in:

- **Identity & dedup** — case-sensitivity is implicit (R-017); the same FQN with different casing yields two separate `TableRef`s in SQL and SDP, possibly one in Python depending on path.
- **Aliases** — none of the builders track table aliases (R-041). Rules cannot map `t1.col` back to `table_a.col`.
- **Subqueries** — `TableFactor::Derived` references are silently dropped (R-001).
- **Joins** — shuffle nodes are created and then orphaned (R-002).
- **Embedded SQL** — `spark.sql("...")` in Python is classified but not nodified (R-003).
- **Variable rebinding** — Python `df = ...; df = ...` causes silent re-targeting of downstream edges (R-016).
- **SDP source-type** — `sdp.read` + `LIVE.x` in the same function: source-type is the last one written (R-027).
- **Edge schema** — edge kinds are ad-hoc strings, not an enum (R-047).

**Type-level soundness** — `TableRef` allows internally inconsistent combinations because its fields are public and there is no constructor invariant (R-023).

**Invariant validation** — there is no post-construction graph validator (R-040). Recursive CTEs produce unmarked cycles (R-039); empty plan node lists default `root` to id 0 even when no node has id 0 (R-038).

**Recommendation summary for graph soundness:**

1. **Migrate the SQL builder to `tree-sitter-sequel` (R-109)** — unifies parsing with the Python builder, gives error recovery, enables embedded-SQL re-parse via `set_included_ranges`, and aligns the SQL AST with the DSL `ast/*` patterns. Several of the items below become natural by-products of this migration.
2. Add `Graph::validate()` invoked under `debug_assertions` at the end of every `Graph::from_*`.
3. Define `EdgeKind` and `OperationKind` enums; replace string edge kinds.
4. Tighten `TableRef` to private fields with constructor-only invariants and a canonical-key method.
5. Implement alias tracking on `TableRef`.
6. Recurse into derived subqueries; wire join shuffle nodes; create nodes for `spark.sql()` (R-001, R-002, R-003 — all easier post-R-109).
7. Make case-insensitivity explicit at the `TableRef` boundary.

---

## Section: Python-layer interface (PyO3 surface)

Reviewing every `#[pyfunction]` and `#[pyclass]` in `lib.rs`, `types.rs`, `resolved/python.rs`, and others.

**Error mapping (R-004)** — every error reaches Python as `PyRuntimeError` or `PyIOError`. There is no domain-typed exception hierarchy. Downstream tooling cannot react structurally (e.g. "ParseError → log and continue, CatalogError → degrade gracefully").

**Performance on the boundary (R-018, R-026)** — `PyResolvedGraph::graph()`, `PyPlanSubtreeNode::metrics()`, `PyResolvedGraph::table_specs()` all clone deeply on each access. Materialise once at construction time and store as `Py<...>`.

**Docstring coverage (R-013)** — some `#[pyfunction]` items have doc comments (`check`, `run_rules`), others do not (`list_rules`, `get_registry_count`). Python's `help()` is empty for the latter.

**Naming consistency** — the `Py*` prefix is used for some exported classes (`PyGraph`, `PyTableRef`) but not for plain types (`AnalysisMode`, `Severity`, `Confidence`). The mixed scheme is fine in Rust but confusing for Python consumers reading `from _engine import ...`. Pick a convention (probably: drop `Py*` for the Python-facing name via `#[pyclass(name = "Graph")]`).

**Type stubs** — no `.pyi` file was located. Without it, IDE autocomplete on the Rust extension surface is degraded.

**GIL hygiene** — code generally uses `Py<T>` and `Bound<T>` correctly; no obvious cases of holding the GIL during blocking I/O. Consider wrapping the `reqwest::blocking` calls in `Python::allow_threads(|| ...)` so concurrent threads can proceed.

**`Send`/`Sync` correctness** — no explicit `static_assertions::assert_impl_all!(PyXxx: Send, Sync);` checks. PyO3 enforces these via macros, but adding explicit assertions in tests would catch regressions.

**Recommendation summary for Python interface:**

1. Introduce a typed exception hierarchy and a `From<EngineError> for PyErr` impl.
2. Cache materialised PyO3 representations at construction; eliminate clone-on-getter.
3. Add docstrings to every `#[pyfunction]` and every `#[pyclass]` (this is a small, high-value change).
4. Generate a `.pyi` stub file (manual or via `pyo3-stub-gen`).
5. Wrap `reqwest::blocking` calls in `Python::allow_threads`.
6. Decide on `Py*` naming once; rename via `#[pyclass(name = "...")]`.

---

## Section: DSL syntax & documentation

**Syntax soundness** — the lexer/parser are sound for the documented grammar. Concerns:

- **No parse-time validation of predicate names or operation-kinds** (R-021) — typos silently produce zero matches.
- **`pred_when` kwarg parsing is hand-rolled** (R-020) — silent acceptance of malformed kwargs.
- **Duplicate predicate registration** (R-022) — `shares-receiver` is registered twice; the registry has no collision check.
- **One-token lookahead in pattern parsing** (R-046) — fragile if the grammar is extended.
- **Reserved capture names** (R-079) — `__current`/`__fact` are not protected.
- **Round-trip stability** (R-078) — no `Display` impl on IR types; cannot serialise a parsed pattern.

**Error diagnostics** — `dsl/error.rs` already uses `thiserror` with line/column tracking; this is the model for the rest of the crate (see R-054).

**Documentation completeness** — cross-referencing `docs/dsl-reference.md`, `docs/writing-rules.md`, `docs/anti-pattern-rules.md` against the predicate registry and parser surfaced **13 drift items** (R-096 through R-108).

The dominant finding is **R-096**: `writing-rules.md` documents three legacy detection tiers (`[query]`, `[context]`, `[dataflow]`) but says nothing about the `[graph]` DSL tier — which is the *only* tier actually used by all 111 currently-shipping rules. A new rule author following the docs writes the wrong kind of rule.

Other concrete drift items:

- **`:method-chain` property** is real and tested but absent from the Properties table (R-097, R-100).
- **`@cap.line` / `@cap.column` field-suffix syntax** is implemented for `[graph.finding].line` but not in the BNF (R-098, R-103).
- **`#column-of` predicate** is documented as functional but is a no-op stub returning `Nil` (R-099, also R-069).
- **`BJ001`** rule file exists but is missing from the rule-index table (R-101); the rule-count comment is stale at 110 (R-102).
- Smaller polish items: separator choice rule terse (R-104), only one `fact:source` example (R-105), `:arg/N` lacks an inline example (R-106), `#fires-rule` is a stub but no guidance (R-107), capture-name conventions undocumented (R-108).

**Recommendation summary for DSL syntax & docs:**

1. Add a `PredicateMeta` registry — name, arity, operand types, doc string — and validate every parsed pattern against it at build time (R-021).
2. Refactor `pred_when` kwarg parsing via a shared `parse_kwargs(...)` helper (R-020).
3. Remove the duplicate `shares-receiver` stub; add a unique-name check in the registry builder (R-022).
4. Implement `Display` on IR types; add a round-trip property test (R-078).
5. Enforce reserved capture-name conventions in the parser (R-079).
6. **Add a graph-DSL chapter to `writing-rules.md`** — this is the single highest-leverage docs change (R-096).
7. Reconcile `dsl-reference.md` against the predicate registry: document `:method-chain`, extend the BNF for `@cap.line`, decide on `#column-of`, document `@cap.line`/`@cap.column` (R-097–R-099, R-103).
8. Re-run `scripts/gen_rule_index.py` (or run in CI) to fix R-101 + R-102.

---

## Section: Dependency hygiene

See findings R-029, R-053–R-057, R-088–R-095, R-109. Pull-requestable items:

1. **Adopt `tree-sitter-sequel` in place of `sqlparser`** — R-109 (Critical). The dep is already declared.
2. **Drop the unused `streaming-iterator` dependency** — R-053, one-line PR.
3. **Enable `abi3`** on PyO3 + maturin — reduces release matrix from N×M (Python × OS) to 1×M.
4. **Unify error handling** via `thiserror`-based `EngineError` — enables R-004 and R-054 together.

---

## Verification (how to act on this report)

For each finding:

1. Open the file at the cited line — confirm the issue still exists at HEAD.
2. For graph-soundness findings, write a regression test in `tests/unit/graph/` exercising the broken case **before** fixing.
3. For perf findings, benchmark the hot path with `criterion` before refactoring; numbers > intuition.
4. Run `cargo clippy --lib --no-deps -- -D warnings` after each fix to ensure no regression.
5. Run `cargo test --workspace` — the existing 4 test suites (`test_pattern_rules`, `test_context_rules`, `test_dataflow_rules`, `resolved_graph`) cover much of the engine; respect them.

For the report itself, the deliverable is doc-only:

- `cargo build` and `cargo clippy -- -D warnings` are unaffected.
- `git diff main...claude/review-rust-engine-nDV61 -- reviews/rust-engine-review.md` shows the new file.

---

## Appendix A — `cargo clippy --lib -- -W clippy::pedantic -W clippy::nursery` lint inventory

| Count | Lint                                                                                     |
|------:|------------------------------------------------------------------------------------------|
| ~325  | `clippy::uninlined_format_args` (`format!("{}", x)` → `format!("{x}")`)                  |
|    44 | `clippy::unnecessary_structure_name_repetition`                                          |
|    41 | `clippy::missing_const_for_fn`                                                           |
|    41 | `clippy::doc_markdown` (missing backticks in docs)                                       |
|    28 | `clippy::must_use_candidate` (method could have `#[must_use]`)                           |
|    21 | `clippy::redundant_closure`                                                              |
|    16 | `clippy::must_use_candidate` (function variant)                                          |
|    13 | `clippy::uninlined_format_args` (in lib tests)                                           |
|    11 | `clippy::match_same_arms`                                                                |
|    10 | `clippy::option_if_let_else` (use `Option::map_or_else`)                                 |
|     9 | `clippy::cast_possible_truncation` (`usize` → `u32`)                                     |
|     8 | `clippy::cast_precision_loss` (`u64` → `f64`)                                            |
|     8 | `clippy::bool_to_int_with_if` / `unnecessary_map_or` (called `map(<f>).unwrap_or(false)`)|
|     7 | `clippy::manual_let_else`                                                                |
|     6 | `clippy::option_if_let_else` (`Option::map_or`)                                          |
|     6 | `clippy::cast_precision_loss` (`usize` → `f64`)                                          |
|     5 | `clippy::redundant_clone`                                                                |
|     5 | `clippy::case_sensitive_file_extension_comparisons` (in `parse/notebooks.rs`)            |
|     5 | `clippy::option_map_or_unit` (`map(<f>).unwrap_or(<a>)`)                                 |
|     4 | `clippy::unsafe_derive_deserialize` (PyO3-generated; R-031)                              |

Total: ~600 warnings under `pedantic`/`nursery`. Most are addressable via `cargo clippy --fix` for the `format!` lint alone; the rest are deliberate API-tightening opportunities.

---

## Appendix B — `cargo doc` warnings (4)

1. `src/resolved/scope_facts.rs:17` — link to private `ImportMap`
2. `src/resolved/mod.rs:71` — unresolved `CatalogClient`
3. `src/rules/graph_dsl/parser.rs:3` — link to private `lexer`
4. `src/types.rs:452` — unresolved `CatalogClient`

Recommendation: see R-029.

---

## Appendix C — Recommended new dependencies

The following crates are not currently declared but address concrete findings in this review. **No code change is proposed here** — this appendix is a curated shopping list for follow-up PRs. Version pins below were verified to build cleanly against the current crate tree (Rust 1.94, edition 2021, PyO3 0.22, rustls).

### Production dependencies

```toml
# --- network / I/O safety ---
url = "2.5"            # proper URL parsing/validation
urlencoding = "2.1"    # URL-segment encoding for catalog paths
walkdir = "2.5"        # symlink-safe directory traversal

# --- data structures ---
smallvec = "1.13"      # 0-2 element vectors without heap
dashmap = "6.1"        # concurrent HashMap for shared caches

# --- observability ---
tracing = "0.1"
tracing-subscriber = { version = "0.3", default-features = false, features = ["fmt", "env-filter"] }
```

| Crate                | Addresses                                                                 | Notes |
|----------------------|---------------------------------------------------------------------------|-------|
| `url`                | R-005 (URL injection), R-011 (https-only catalog URLs)                     | Replace ad-hoc `format!()` URL construction in `session/mod.rs:156` and `catalog/databricks.rs:64-67`. |
| `urlencoding`        | R-013 (catalog `table_fqn` not encoded)                                    | Tiny crate; use `encode_segment` on every path component. |
| `walkdir`            | R-014 (symlink/traversal defense), R-015 (file-size cap on entry)          | `WalkDir::new(root).follow_links(false)`. |
| `smallvec`           | R-027 (SDP source-type bitset), general perf in graph builders             | Apply selectively where Vec<T> hot paths have ≤2 elements typically. |
| `dashmap`            | R-049 (per-instance catalog cache)                                         | Process-global cache backed by `OnceLock<DashMap<...>>`. |
| `tracing`            | R-024, R-033, R-034, R-050, R-063, R-077, R-080, R-101 (silent error paths)| Foundational — replace every "silently swallows" with `warn!` / `error!`. The single biggest signal-to-noise improvement available. |
| `tracing-subscriber` | Companion to `tracing`                                                     | Default subscriber gates on `RUST_LOG` (or `BURNT_LOG`). |

### Dev-only dependencies

```toml
[dev-dependencies]
criterion = "0.5"
proptest = "1.5"
mockito = "1.6"
pretty_assertions = "1.4"
rstest = "0.23"
```

| Crate               | Addresses                                                                     | Notes |
|---------------------|-------------------------------------------------------------------------------|-------|
| `criterion`         | R-018, R-019, R-026, R-068 (perf findings)                                    | Benchmark hot paths (graph getters, predicate evaluation, regex compilation) before refactoring — numbers beat intuition. |
| `proptest`          | R-040 (graph invariants), R-078 (DSL round-trip stability)                    | Property tests for `parse(p.to_string()) == p` and `Graph::validate()` invariants. |
| `mockito`           | R-010, R-011, R-012, R-034, R-035 (REST client testability)                   | Spin up a fake Spark/Databricks endpoint in tests instead of relying on integration env vars. |
| `pretty_assertions` | All tests with structured-value assertions                                    | Drop-in `use pretty_assertions::assert_eq;` — diffs that fit on screen. |
| `rstest`            | All tests with parameterised fixtures                                         | Cleaner test ergonomics for the 110+ rule tests. |

### Dependency *not* added (mentioned for completeness)

- **`parking_lot`** — would fix R-050 (mutex poisoning) but adds a transitive surface; the alternative (`Mutex::clear_poison` + `tracing::error!`) is reasonable in-tree without a new dep.

### Adoption sequencing

A sensible order to land these (each step is independently shippable):

1. **`tracing` + `tracing-subscriber`** first — unblocks fixing every silent-error finding without code reshaping. Add a `#[cfg(not(any(test, feature = "no_log")))]` subscriber init in `lib.rs`.
2. **`url` + `urlencoding`** alongside the R-005/R-013 fixes.
3. **`walkdir`** alongside R-014.
4. **Dev-only deps** in a single PR — they don't affect the wheel.
5. **`smallvec` + `dashmap`** opportunistically as the relevant hot paths are touched.

### Sanity-checked

Each crate version above was test-installed against the engine's current dep tree. `cargo build --lib` completes cleanly; transitive duplicate count does not materially change. Adding these will increase `Cargo.lock` size and first-build time but does not change wheel ABI or runtime characteristics until the code is wired in.

---

## Appendix D — Transitive duplicate dependencies

From `cargo tree --duplicates`:

- `getrandom v0.2.17` (via `ring` → `rustls`) and `v0.4.2` (via `tempfile` → `insta`)
- `memchr v2.8.0` reached two ways (regex/aho-corasick and object/ar_archive_writer)
- `hashbrown 0.15` and `0.16`
- `windows-sys 0.52`, `0.59`, `0.61`
- `r-efi 5.3` and `6.0`

None are critical; ecosystem-typical.

---

## End of report

This review is doc-only — no code changes are made. Findings are stable as of commit `f70adde`. Re-run the agents / clippy if the branch is updated significantly before triage.
