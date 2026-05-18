# Changelog

## [Unreleased]

## [0.3.0] — 2026-05-18

### Added

- **Graph-DSL rule engine** — S-expression pattern language over `ResolvedGraph`.
  Every Spark/SQL/SDP rule now runs through a single pipeline that builds a
  resolved graph, compiles DSL patterns lazily, and caches them for the process
  lifetime. No session or analysis-mode setup required at the call site.

- **`fact:source` head** — fires once per source file and binds the full source
  text to a capture variable; used by credential-scanning, import-hygiene, and
  raw-text pattern rules.

- **`#in-loop` predicate** — true when the matched node appears inside a `for`
  or `while` loop body; powers BD016 (write in loop) and BP020 (withColumn in
  loop).

- **`#method-chain-contains` predicate** — true when any element of a call
  node's method chain contains a given substring; used by streaming trigger,
  watermark, and schema rules.

- **`#self-join?` predicate** — detects self-joins (`df.join(df, ...)`) by
  comparing receiver and first-argument variable names via Rust-side regex
  capture groups.

- **`#shares-receiver` predicate** — true when two nodes are called on the same
  root receiver variable.

- **`#kwargs/missing` and `#kwargs/has` predicates** — check for presence or
  absence of named keyword arguments at a call site.

- **`[graph.finding]` sub-table** — per-rule overrides for severity, confidence,
  message, suggestion, and line attribution.

- **DSL reference documentation** (`docs/dsl-reference.md`) — grammar, head
  prefixes, properties, captures, all predicates with signatures and examples,
  finding overrides, and full rule anatomy.

- All 110 rules migrated to `[graph]` DSL blocks, covering Python, SQL, SDP,
  notebook, streaming, Delta, join, governance, testing, style, and observability
  categories.

### Removed

- **Legacy execution paths** — `rules/query.rs`, `rules/context/` (8 modules),
  `rules/dataflow.rs`, `rules/notebook_queries.rs`, and `rules/context_structs.rs`
  are deleted. `RulePipeline::execute_pattern_rules`,
  `execute_context_rules`, and `execute_dataflow_rules` are removed.
  `RulePipeline::execute` now calls only `run_graph_rules`.

- **`[query]`, `[context]`, `[dataflow]` TOML blocks** stripped from all 110
  rule files. Rules carry only `[rule]`, `[tests]`, and `[graph]`.

- **`QueryPattern` type** and the `patterns`, `has_context`, `has_dataflow`
  fields removed from `CompiledRule`. `build.rs` codegen no longer emits them.

### Changed

- Language matching is now case-insensitive throughout. `"notebook"` is
  accepted everywhere `"python"` is and routes through the Python graph builder.

- `burnt-engine` version bumped to `0.2.0`.

## [0.2.0] — prior release

Initial public release with cost estimation, session analysis, and rule system.
