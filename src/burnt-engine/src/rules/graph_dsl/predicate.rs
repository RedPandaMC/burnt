//! Built-in predicate registry for the graph-query DSL.
//!
//! A predicate is a function from `(args, ctx) -> PredResult`. Predicates
//! compose freely: `#and` / `#or` / `#not` / `#xor` / `#implies` take
//! inner predicates as `PredArg::Predicate`; quantifiers (`#count`,
//! `#all`, `#any`, `#none`) take an inner pattern + a subject.
//!
//! The registry is initialised once at crate startup via `OnceLock`.
//! Adding a new predicate is one `insert(...)` call plus the function
//! definition.

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

use regex::Regex;

use crate::rules::graph_dsl::context::{FindingMutation, MatchCtx};
use crate::rules::graph_dsl::ir::{PredArg, Predicate, Value};
use crate::rules::graph_dsl::value::CaptureValue;
use crate::types::{Confidence, Severity};

/// What a predicate evaluates to.
#[derive(Debug, Clone)]
pub enum PredResult {
    /// Most predicates — pass / fail of the structural match.
    Bool(bool),
    /// Value-extracting predicates — produces a CaptureValue that can
    /// flow into another predicate or a finding template.
    Value(CaptureValue),
    /// `#when` and friends — conditional finding mutation.
    SetFinding(FindingMutation),
    /// Used by short-circuiting composition. Matcher folds back to Bool.
    Skip,
}

impl PredResult {
    #[must_use]
    pub fn as_bool(&self) -> bool {
        matches!(self, Self::Bool(true))
    }
}

/// Function-pointer registry. Predicates are pure functions of args +
/// context; no &mut state means the registry can stay `Sync` without
/// locks and the matcher can hand the same `MatchCtx` to many
/// predicates in any order.
pub type PredicateFn = fn(&[PredArg], &MatchCtx) -> PredResult;

/// Look up a predicate by bare name (no leading `#`). Returns `None`
/// for unknown names; the matcher surfaces that as a build-time error.
#[must_use]
pub fn lookup(name: &str) -> Option<PredicateFn> {
    registry().get(name).copied()
}

/// All registered predicate names, in alphabetical order. Used by
/// `build.rs` to validate DSL syntax at compile time and by docs to
/// list capabilities.
#[must_use]
pub fn registered_names() -> Vec<&'static str> {
    let mut names: Vec<&'static str> = registry().keys().copied().collect();
    names.sort_unstable();
    names
}

/// Number of built-in predicates currently registered.
#[must_use]
pub fn registry_size() -> usize {
    registry().len()
}

fn registry() -> &'static HashMap<&'static str, PredicateFn> {
    static REGISTRY: OnceLock<HashMap<&'static str, PredicateFn>> = OnceLock::new();
    REGISTRY.get_or_init(build_registry)
}

static REGEX_CACHE: OnceLock<RwLock<HashMap<String, Regex>>> = OnceLock::new();

fn cached_regex(pattern: &str) -> Option<regex::Regex> {
    let cache = REGEX_CACHE.get_or_init(|| RwLock::new(HashMap::new()));
    if let Ok(guard) = cache.read() {
        if let Some(re) = guard.get(pattern) {
            return Some(re.clone());
        }
    }
    if let Ok(re) = Regex::new(pattern) {
        if let Ok(mut guard) = cache.write() {
            guard.insert(pattern.to_string(), re.clone());
        }
        return Some(re);
    }
    None
}

fn build_registry() -> HashMap<&'static str, PredicateFn> {
    let mut m: HashMap<&'static str, PredicateFn> = HashMap::new();

    // ------------------------------------------------------------------
    // Composition
    // ------------------------------------------------------------------
    m.insert("and", pred_and);
    m.insert("or", pred_or);
    m.insert("not", pred_not);
    m.insert("xor", pred_xor);
    m.insert("implies", pred_implies);

    // ------------------------------------------------------------------
    // Value / string
    // ------------------------------------------------------------------
    m.insert("eq?", pred_eq);
    m.insert("not-eq?", pred_not_eq);
    m.insert("match?", pred_match);
    m.insert("not-match?", pred_not_match);
    m.insert("in", pred_in);
    m.insert("starts-with", pred_starts_with);
    m.insert("ends-with", pred_ends_with);
    m.insert("contains", pred_contains);
    m.insert("kind", pred_kind);

    // ------------------------------------------------------------------
    // Numeric
    // ------------------------------------------------------------------
    m.insert("gt", pred_gt);
    m.insert("gte", pred_gte);
    m.insert("lt", pred_lt);
    m.insert("lte", pred_lte);
    m.insert("eq", pred_eq_num);

    // ------------------------------------------------------------------
    // Quantifiers
    // ------------------------------------------------------------------
    m.insert("count", pred_count);
    m.insert("all", pred_all);
    m.insert("any", pred_any);
    m.insert("none", pred_none);
    m.insert("exists", pred_exists);
    m.insert("exists-here", pred_exists_here);
    m.insert("unique", pred_unique);

    // ------------------------------------------------------------------
    // Overlay / runtime
    // ------------------------------------------------------------------
    m.insert("has-overlay", pred_has_overlay);
    m.insert("has-provenance", pred_has_provenance);
    m.insert("observed-bytes-gt", pred_observed_bytes_gt);
    m.insert("table-spec-size-gt", pred_table_spec_size_gt);

    // ------------------------------------------------------------------
    // Bindings / dataflow
    // ------------------------------------------------------------------
    m.insert("binds", pred_binds);
    m.insert("reads", pred_reads);

    // ------------------------------------------------------------------
    // Value extraction
    // ------------------------------------------------------------------
    m.insert("value-of", pred_value_of);
    m.insert("method-of", pred_method_of);
    m.insert("method-chain-of", pred_method_chain_of);
    m.insert("line-of", pred_line_of);
    m.insert("column-of", pred_column_of);
    m.insert("fqn-of", pred_fqn_of);
    m.insert("overlay-of", pred_overlay_of);

    // ------------------------------------------------------------------
    // Conditional finding mutation
    // ------------------------------------------------------------------
    m.insert("when", pred_when);

    // ------------------------------------------------------------------
    // Traversal subjects — return CaptureValue::List of related nodes.
    // ------------------------------------------------------------------
    m.insert("descendants", pred_descendants);
    m.insert("ancestors", pred_ancestors);
    m.insert("siblings", pred_siblings);
    m.insert("receiver-of", pred_receiver_of);
    m.insert("callees-of", pred_callees_of);

    // ------------------------------------------------------------------
    // Expansion ideas (§B+) — full implementations below.
    // ------------------------------------------------------------------
    m.insert("fires-rule", pred_stub_false);
    m.insert("prop", pred_prop);
    m.insert("not-receiver-of", pred_not_receiver_of);
    m.insert("kwargs/missing", pred_kwargs_missing);
    m.insert("kwargs/has", pred_kwargs_has);

    // ------------------------------------------------------------------
    // Node-level scope predicates
    // ------------------------------------------------------------------
    m.insert("in-loop", pred_in_loop);
    m.insert("method-chain-contains", pred_method_chain_contains);
    m.insert("source-of", pred_source_of);
    m.insert("shares-receiver", pred_shares_receiver_impl);
    m.insert("self-join?", pred_self_join);

    // ------------------------------------------------------------------
    // Dynamic-argument predicates (Issue #53)
    // ------------------------------------------------------------------
    m.insert("arg-is-dynamic", pred_arg_is_dynamic);
    m.insert("arg-kind-of", pred_arg_kind_of);

    // ------------------------------------------------------------------
    // Catalog-enriched predicates (Issue #72)
    // ------------------------------------------------------------------
    m.insert("table-has-property", pred_table_has_property);
    m.insert("join-type-mismatch", pred_join_type_mismatch);

    m
}

// ----------------------------------------------------------------------
// Helpers shared across predicates
// ----------------------------------------------------------------------

fn first_value(args: &[PredArg], ctx: &MatchCtx) -> Option<CaptureValue> {
    args.first().and_then(|a| resolve_arg(a, ctx))
}

fn resolve_arg(arg: &PredArg, ctx: &MatchCtx) -> Option<CaptureValue> {
    match arg {
        PredArg::Value(v) => resolve_value(v, ctx),
        // Allow (#method-of @x) and other value-extracting predicates
        // to appear anywhere a value is expected. The matcher dispatches
        // them through the registry; if they return a Value, we surface
        // it; if Bool, we lift to CaptureValue::Bool.
        PredArg::Predicate(p) => match evaluate_predicate(p, ctx) {
            PredResult::Value(v) => Some(v),
            PredResult::Bool(b) => Some(CaptureValue::Bool(b)),
            _ => None,
        },
        PredArg::Pattern(_) => None,
    }
}

fn resolve_value(v: &Value, ctx: &MatchCtx) -> Option<CaptureValue> {
    match v {
        Value::String(s) => Some(CaptureValue::String(s.clone())),
        Value::Number(n) => Some(CaptureValue::Number(*n)),
        Value::Bool(b) => Some(CaptureValue::Bool(*b)),
        Value::Size(b) => Some(CaptureValue::Number(*b as f64)),
        Value::DurationMs(ms) => Some(CaptureValue::Number(*ms as f64)),
        Value::Ident(s) => Some(CaptureValue::String(s.clone())),
        Value::CaptureRef(name) => ctx.captures.get(name).cloned(),
        Value::List(items) => Some(CaptureValue::List(
            items.iter().filter_map(|v| resolve_value(v, ctx)).collect(),
        )),
    }
}

fn evaluate_inner(arg: &PredArg, ctx: &MatchCtx) -> PredResult {
    match arg {
        PredArg::Predicate(p) => evaluate_predicate(p, ctx),
        PredArg::Value(v) => match resolve_value(v, ctx) {
            Some(cv) => PredResult::Bool(captured_truthy(&cv)),
            None => PredResult::Bool(false),
        },
        // Run the matcher on the nested pattern. Pattern-as-arg is
        // truthy iff at least one match exists in the resolved graph.
        PredArg::Pattern(p) => {
            let matches = crate::rules::graph_dsl::matcher::run_pattern(p, None, ctx.resolved);
            PredResult::Bool(!matches.is_empty())
        }
    }
}

/// Dispatch a predicate by name through the registry. Public so the
/// matcher (commit 7) and composition predicates use the same path.
pub fn evaluate_predicate(pred: &Predicate, ctx: &MatchCtx) -> PredResult {
    match lookup(&pred.name) {
        Some(f) => f(&pred.args, ctx),
        // Unknown predicate at runtime — defensive; should be caught
        // at build time. Fail closed.
        None => PredResult::Bool(false),
    }
}

fn captured_truthy(v: &CaptureValue) -> bool {
    match v {
        CaptureValue::Bool(b) => *b,
        CaptureValue::Number(n) => *n != 0.0,
        CaptureValue::String(s) => !s.is_empty(),
        CaptureValue::Nil => false,
        _ => true,
    }
}

fn coerce_number(v: &CaptureValue) -> Option<f64> {
    v.as_number().or_else(|| match v {
        CaptureValue::String(s) => s.parse::<f64>().ok(),
        _ => None,
    })
}

fn coerce_string(v: &CaptureValue) -> Option<String> {
    v.as_str_value()
}

// ----------------------------------------------------------------------
// Composition
// ----------------------------------------------------------------------

fn pred_and(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    for a in args {
        if !matches!(
            evaluate_inner(a, ctx),
            PredResult::Bool(true) | PredResult::Skip
        ) {
            return PredResult::Bool(false);
        }
    }
    PredResult::Bool(true)
}

fn pred_or(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    for a in args {
        if matches!(evaluate_inner(a, ctx), PredResult::Bool(true)) {
            return PredResult::Bool(true);
        }
    }
    PredResult::Bool(false)
}

fn pred_not(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let r = args
        .first()
        .map_or(PredResult::Bool(true), |a| evaluate_inner(a, ctx));
    PredResult::Bool(!r.as_bool())
}

fn pred_xor(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let mut count = 0;
    for a in args {
        if evaluate_inner(a, ctx).as_bool() {
            count += 1;
        }
    }
    PredResult::Bool(count % 2 == 1)
}

fn pred_implies(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let antecedent = args
        .first()
        .is_some_and(|a| evaluate_inner(a, ctx).as_bool());
    if !antecedent {
        return PredResult::Bool(true);
    }
    let consequent = args
        .get(1)
        .is_some_and(|a| evaluate_inner(a, ctx).as_bool());
    PredResult::Bool(consequent)
}

// ----------------------------------------------------------------------
// Value / string
// ----------------------------------------------------------------------

fn pred_eq(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let Some(lhs) = first_value(args, ctx) else {
        return PredResult::Bool(false);
    };
    let Some(rhs) = args.get(1).and_then(|a| resolve_arg(a, ctx)) else {
        return PredResult::Bool(false);
    };
    PredResult::Bool(coerce_string(&lhs) == coerce_string(&rhs))
}

fn pred_not_eq(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    PredResult::Bool(!pred_eq(args, ctx).as_bool())
}

fn pred_match(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let (Some(lhs), Some(rhs)) = (
        first_value(args, ctx).and_then(|v| coerce_string(&v)),
        args.get(1)
            .and_then(|a| resolve_arg(a, ctx))
            .and_then(|v| coerce_string(&v)),
    ) else {
        return PredResult::Bool(false);
    };
    let Some(re) = cached_regex(&rhs) else {
        return PredResult::Bool(false);
    };
    PredResult::Bool(re.is_match(&lhs))
}

fn pred_not_match(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    PredResult::Bool(!pred_match(args, ctx).as_bool())
}

fn pred_in(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let (Some(needle), Some(haystack)) = (
        first_value(args, ctx),
        args.get(1).and_then(|a| resolve_arg(a, ctx)),
    ) else {
        return PredResult::Bool(false);
    };
    let needle_s = coerce_string(&needle);
    let CaptureValue::List(items) = haystack else {
        return PredResult::Bool(false);
    };
    for item in &items {
        if needle_s.is_some() && needle_s == coerce_string(item) {
            return PredResult::Bool(true);
        }
    }
    PredResult::Bool(false)
}

fn pred_starts_with(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    string_pair(args, ctx, |a, b| a.starts_with(&b))
}

fn pred_ends_with(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    string_pair(args, ctx, |a, b| a.ends_with(&b))
}

fn pred_contains(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    string_pair(args, ctx, |a, b| a.contains(&b))
}

fn pred_kind(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    // (#kind @cap "FString") — true iff the capture is an AstArg
    // whose variant matches the given name. Also accepts CaptureValue::
    // String for nodes whose kind was extracted as a string.
    let Some(cap) = first_value(args, ctx) else {
        return PredResult::Bool(false);
    };
    let Some(target) = args
        .get(1)
        .and_then(|a| resolve_arg(a, ctx))
        .and_then(|v| coerce_string(&v))
    else {
        return PredResult::Bool(false);
    };
    let actual = match &cap {
        CaptureValue::AstArg(arg) => crate::rules::graph_dsl::value::ast_arg_kind(arg),
        CaptureValue::String(s) => s.as_ref(),
        _ => return PredResult::Bool(false),
    };
    PredResult::Bool(actual == target)
}

fn string_pair(args: &[PredArg], ctx: &MatchCtx, f: impl Fn(String, String) -> bool) -> PredResult {
    let (Some(lhs), Some(rhs)) = (
        first_value(args, ctx).and_then(|v| coerce_string(&v)),
        args.get(1)
            .and_then(|a| resolve_arg(a, ctx))
            .and_then(|v| coerce_string(&v)),
    ) else {
        return PredResult::Bool(false);
    };
    PredResult::Bool(f(lhs, rhs))
}

// ----------------------------------------------------------------------
// Numeric
// ----------------------------------------------------------------------

fn numeric_pair(args: &[PredArg], ctx: &MatchCtx, f: impl Fn(f64, f64) -> bool) -> PredResult {
    let (Some(lhs), Some(rhs)) = (
        first_value(args, ctx).and_then(|v| coerce_number(&v)),
        args.get(1)
            .and_then(|a| resolve_arg(a, ctx))
            .and_then(|v| coerce_number(&v)),
    ) else {
        return PredResult::Bool(false);
    };
    PredResult::Bool(f(lhs, rhs))
}

fn pred_gt(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    numeric_pair(args, ctx, |a, b| a > b)
}
fn pred_gte(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    numeric_pair(args, ctx, |a, b| a >= b)
}
fn pred_lt(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    numeric_pair(args, ctx, |a, b| a < b)
}
fn pred_lte(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    numeric_pair(args, ctx, |a, b| a <= b)
}
fn pred_eq_num(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    numeric_pair(args, ctx, |a, b| (a - b).abs() < f64::EPSILON)
}

// ----------------------------------------------------------------------
// Quantifiers — full graph traversal lives in commit 7's matcher; here
// we implement the *list-form* of each (subject is already a List
// capture). The matcher reduces traversal expressions to List captures
// before invoking these.
// ----------------------------------------------------------------------

fn pred_count(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    // Two forms accepted:
    //   (#count @some-list :as @n)              ← bind count to capture
    //   (#count @some-list :gte 3)              ← shorthand for count >= n
    let Some(subject) = first_value(args, ctx) else {
        return PredResult::Value(CaptureValue::Number(0.0));
    };
    let n = subject.iter_items().count() as f64;
    PredResult::Value(CaptureValue::Number(n))
}

fn pred_all(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let Some(subject) = first_value(args, ctx) else {
        return PredResult::Bool(true);
    };
    if subject.iter_items().next().is_none() {
        return PredResult::Bool(false);
    }
    let inner = args.get(1);
    for item in subject.iter_items() {
        let pass = match inner {
            Some(a) => evaluate_inner_with_it(a, ctx, item),
            None => captured_truthy(item),
        };
        if !pass {
            return PredResult::Bool(false);
        }
    }
    PredResult::Bool(true)
}

fn pred_any(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let Some(subject) = first_value(args, ctx) else {
        return PredResult::Bool(false);
    };
    let inner = args.get(1);
    for item in subject.iter_items() {
        let pass = match inner {
            Some(a) => evaluate_inner_with_it(a, ctx, item),
            None => captured_truthy(item),
        };
        if pass {
            return PredResult::Bool(true);
        }
    }
    PredResult::Bool(false)
}

fn pred_none(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    PredResult::Bool(!pred_any(args, ctx).as_bool())
}

/// Evaluate `inner` in a child context that binds `@it` to the current
/// quantifier item. Used by `#all`, `#any`, `#none` so the inner
/// predicate can refer back to the loop variable.
fn evaluate_inner_with_it(inner: &PredArg, ctx: &MatchCtx, item: &CaptureValue) -> bool {
    let mut child = ctx.captures.clone();
    child.insert("it".into(), item.clone());
    let child_ctx = MatchCtx::new(ctx.resolved, &child);
    evaluate_inner(inner, &child_ctx).as_bool()
}

fn pred_exists(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    // (#exists <pattern>) — true iff the pattern matches at least one
    // anchor in the graph. Predicate arg is parsed by the parser as
    // PredArg::Pattern, so evaluate_inner already handles the heavy
    // lifting via run_pattern.
    let Some(arg) = args.first() else {
        return PredResult::Bool(false);
    };
    PredResult::Bool(evaluate_inner(arg, ctx).as_bool())
}

fn pred_exists_here(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    // (#exists-here <pattern>) — same as #exists but constrained to the
    // current anchor's neighbourhood (ancestors ∪ descendants ∪ self).
    // Today's approximation: any anchor whose StaticNodeId is in the
    // current node's descendants or ancestors.
    let Some(arg) = args.first() else {
        return PredResult::Bool(false);
    };
    let Some(CaptureValue::Node(cur)) = ctx.captures.get("__current") else {
        // No current anchor — degrade to graph-wide existence.
        return PredResult::Bool(evaluate_inner(arg, ctx).as_bool());
    };
    let neighbourhood: std::collections::HashSet<String> = ctx
        .resolved
        .graph()
        .nodes
        .iter()
        .find(|n| n.id == cur.as_str())
        .map(|n| {
            n.scope
                .ancestors
                .iter()
                .chain(n.scope.descendants.iter())
                .map(|id| id.as_str().to_string())
                .chain(std::iter::once(cur.as_str().to_string()))
                .collect()
        })
        .unwrap_or_default();

    let PredArg::Pattern(p) = arg else {
        return PredResult::Bool(evaluate_inner(arg, ctx).as_bool());
    };
    let matches = crate::rules::graph_dsl::matcher::run_pattern(p, None, ctx.resolved);
    PredResult::Bool(
        matches
            .iter()
            .any(|m| neighbourhood.contains(m.anchor.as_str())),
    )
}

fn pred_unique(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let Some(subject) = first_value(args, ctx) else {
        return PredResult::Bool(true);
    };
    let mut seen = std::collections::HashSet::new();
    for item in subject.iter_items() {
        let key = match item {
            CaptureValue::String(s) => s.to_string(),
            CaptureValue::Number(n) => n.to_string(),
            CaptureValue::Bool(b) => b.to_string(),
            CaptureValue::Node(id) => id.as_str().to_string(),
            _ => continue,
        };
        if !seen.insert(key) {
            return PredResult::Bool(false);
        }
    }
    PredResult::Bool(true)
}

// ----------------------------------------------------------------------
// Overlay / runtime
// ----------------------------------------------------------------------

fn pred_has_overlay(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    // (#has-overlay :stage) — true iff the captured node (or current
    // node, if no capture) has the named overlay populated.
    let kind = args
        .first()
        .and_then(|a| resolve_arg(a, ctx))
        .and_then(|v| v.as_str_value())
        .unwrap_or_default();
    let kind = kind.trim_start_matches(':').to_string();
    let node_id = match args.get(1).and_then(|a| resolve_arg(a, ctx)) {
        Some(CaptureValue::Node(id)) => id.as_str().to_string(),
        _ => match ctx.captures.get("__current") {
            Some(CaptureValue::Node(id)) => id.as_str().to_string(),
            _ => return PredResult::Bool(false),
        },
    };
    let Some(overlay) = ctx.resolved.overlay(&node_id) else {
        return PredResult::Bool(false);
    };
    let has = match kind.as_str() {
        "stage" => !overlay.stages.is_empty(),
        "plan" | "plan-subtree" => overlay.plan_subtree.is_some(),
        "table-spec" => ctx.resolved.table_specs().next().is_some(),
        _ => false,
    };
    PredResult::Bool(has)
}

fn pred_has_provenance(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let node_id = match args.last().and_then(|a| resolve_arg(a, ctx)) {
        Some(CaptureValue::Node(id)) => id,
        _ => match ctx.captures.get("__current") {
            Some(CaptureValue::Node(id)) => id.clone(),
            _ => return PredResult::Bool(false),
        },
    };
    let Some(overlay) = ctx.resolved.overlay(node_id.as_str()) else {
        return PredResult::Bool(false);
    };
    // All leading :flag args must be present in provenance.
    let mut required = crate::resolved::Provenance::empty();
    for a in args {
        if let PredArg::Value(Value::Ident(s)) = a {
            match s.trim_start_matches(':') {
                "static" => required |= crate::resolved::Provenance::STATIC,
                "stage" => required |= crate::resolved::Provenance::STAGE,
                "plan" => required |= crate::resolved::Provenance::PLAN,
                _ => {}
            }
        }
    }
    PredResult::Bool(overlay.provenance.contains(required))
}

fn pred_observed_bytes_gt(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    // (#observed-bytes-gt @cap 1Gi)
    let Some(cap) = first_value(args, ctx) else {
        return PredResult::Bool(false);
    };
    let threshold = args
        .get(1)
        .and_then(|a| resolve_arg(a, ctx))
        .and_then(|v| coerce_number(&v));
    let CaptureValue::Node(id) = cap else {
        return PredResult::Bool(false);
    };
    let Some(overlay) = ctx.resolved.overlay(id.as_str()) else {
        return PredResult::Bool(false);
    };
    let observed = overlay.observed_input_bytes().unwrap_or(0) as f64;
    PredResult::Bool(threshold.is_some_and(|t| observed > t))
}

fn pred_table_spec_size_gt(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    // (#table-spec-size-gt "cat.s.t" 1Gi)
    let Some(fqn) = first_value(args, ctx).and_then(|v| coerce_string(&v)) else {
        return PredResult::Bool(false);
    };
    let Some(threshold) = args
        .get(1)
        .and_then(|a| resolve_arg(a, ctx))
        .and_then(|v| coerce_number(&v))
    else {
        return PredResult::Bool(false);
    };
    let Some(spec) = ctx.resolved.table_spec(&fqn) else {
        return PredResult::Bool(false);
    };
    let size = spec.size_bytes.unwrap_or(0) as f64;
    PredResult::Bool(size > threshold)
}

// ----------------------------------------------------------------------
// Bindings / dataflow
// ----------------------------------------------------------------------

fn pred_binds(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let (Some(cap), Some(name)) = (
        first_value(args, ctx),
        args.get(1)
            .and_then(|a| resolve_arg(a, ctx))
            .and_then(|v| coerce_string(&v)),
    ) else {
        return PredResult::Bool(false);
    };
    let CaptureValue::Node(id) = cap else {
        return PredResult::Bool(false);
    };
    let Some(node) = ctx
        .resolved
        .graph()
        .nodes
        .iter()
        .find(|n| n.id == id.as_str())
    else {
        return PredResult::Bool(false);
    };
    PredResult::Bool(node.scope.writes.iter().any(|w| w == &name))
}

fn pred_reads(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let (Some(cap), Some(name)) = (
        first_value(args, ctx),
        args.get(1)
            .and_then(|a| resolve_arg(a, ctx))
            .and_then(|v| coerce_string(&v)),
    ) else {
        return PredResult::Bool(false);
    };
    let CaptureValue::Node(id) = cap else {
        return PredResult::Bool(false);
    };
    let Some(node) = ctx
        .resolved
        .graph()
        .nodes
        .iter()
        .find(|n| n.id == id.as_str())
    else {
        return PredResult::Bool(false);
    };
    PredResult::Bool(node.scope.reads.iter().any(|r| r == &name))
}

/// `(#in-loop @node)` — true iff the captured node was built inside a
/// `for` or `while` loop body. Set by the Python graph builder.
fn pred_in_loop(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let cap = first_value(args, ctx).or_else(|| ctx.captures.get("__current").cloned());
    let Some(CaptureValue::Node(id)) = cap else {
        return PredResult::Bool(false);
    };
    let result = ctx
        .resolved
        .graph()
        .nodes
        .iter()
        .find(|n| n.id == id.as_str())
        .map(|n| n.scope.in_for_loop)
        .unwrap_or(false);
    PredResult::Bool(result)
}

/// `(#method-chain-contains @node "trigger")` — true iff any element in
/// the node's AST Call method_chain contains the given substring.
fn pred_method_chain_contains(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let cap = first_value(args, ctx).or_else(|| ctx.captures.get("__current").cloned());
    let Some(needle) = args
        .get(if cap.is_some() { 1 } else { 0 })
        .and_then(|a| resolve_arg(a, ctx))
        .and_then(|v| coerce_string(&v))
    else {
        return PredResult::Bool(false);
    };
    let chain = match cap {
        Some(CaptureValue::Node(id)) => ctx
            .resolved
            .graph()
            .nodes
            .iter()
            .find(|n| n.id == id.as_str())
            .and_then(|n| n.ast.as_ref())
            .and_then(|s| match &s.root {
                crate::resolved::ast_shape::AstNode::Call(c) => Some(c.method_chain.clone()),
                _ => None,
            })
            .unwrap_or_default(),
        Some(CaptureValue::AstArg(a)) => match *a {
            crate::resolved::ast_shape::AstArg::Call(c) => c.method_chain,
            _ => Vec::new(),
        },
        _ => ctx
            .captures
            .get("__current")
            .and_then(|v| match v {
                CaptureValue::Node(id) => ctx
                    .resolved
                    .graph()
                    .nodes
                    .iter()
                    .find(|n| n.id == id.as_str())
                    .and_then(|n| n.ast.as_ref())
                    .and_then(|s| match &s.root {
                        crate::resolved::ast_shape::AstNode::Call(c) => {
                            Some(c.method_chain.clone())
                        }
                        _ => None,
                    }),
                _ => None,
            })
            .unwrap_or_default(),
    };
    PredResult::Bool(chain.iter().any(|part| part.contains(&needle)))
}

/// `(#source-of @node)` — returns the node's raw source_code text as a
/// `CaptureValue::String`. Useful for regex matching on the call site text.
fn pred_source_of(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let cap = first_value(args, ctx).or_else(|| ctx.captures.get("__current").cloned());
    let Some(CaptureValue::Node(id)) = cap else {
        return PredResult::Value(CaptureValue::Nil);
    };
    let src = ctx
        .resolved
        .graph()
        .nodes
        .iter()
        .find(|n| n.id == id.as_str())
        .and_then(|n| n.source_code.as_deref())
        .map(|s| CaptureValue::String(std::sync::Arc::from(s)))
        .unwrap_or(CaptureValue::Nil);
    PredResult::Value(src)
}

/// `(#shares-receiver @node-a @node-b)` — true iff both nodes call methods
/// on the same root receiver variable. Uses the first element of each node's
/// method_chain as the receiver identity.
fn pred_shares_receiver_impl(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    fn receiver_of_node(id: &str, ctx: &MatchCtx) -> Option<String> {
        ctx.resolved
            .graph()
            .nodes
            .iter()
            .find(|n| n.id == id)
            .and_then(|n| n.ast.as_ref())
            .and_then(|s| match &s.root {
                crate::resolved::ast_shape::AstNode::Call(c) => c.method_chain.first().cloned(),
                _ => None,
            })
    }
    let (Some(CaptureValue::Node(a)), Some(CaptureValue::Node(b))) = (
        first_value(args, ctx),
        args.get(1).and_then(|a| resolve_arg(a, ctx)),
    ) else {
        return PredResult::Bool(false);
    };
    let ra = receiver_of_node(a.as_str(), ctx);
    let rb = receiver_of_node(b.as_str(), ctx);
    PredResult::Bool(ra.is_some() && ra == rb)
}

/// `(#self-join? @node)` — true iff the call's source text shows a
/// self-join: the receiver variable and the first argument are the
/// same identifier (e.g. `df.join(df, ...)`).
fn pred_self_join(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let Some(CaptureValue::Node(id)) = first_value(args, ctx) else {
        return PredResult::Bool(false);
    };
    let src = ctx
        .resolved
        .graph()
        .nodes
        .iter()
        .find(|n| n.id == id.as_str())
        .and_then(|n| n.source_code.as_deref())
        .unwrap_or("");
    let Some(re) = cached_regex(r"([a-zA-Z_]\w*)\.join\s*\(\s*([a-zA-Z_]\w*)") else {
        return PredResult::Bool(false);
    };
    PredResult::Bool(re.captures(src).is_some_and(|caps| {
        let a = caps.get(1).map_or("", |m| m.as_str());
        let b = caps.get(2).map_or("", |m| m.as_str());
        !a.is_empty() && a == b
    }))
}

// ----------------------------------------------------------------------
// Value extraction
// ----------------------------------------------------------------------

fn pred_value_of(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    args.first()
        .and_then(|a| resolve_arg(a, ctx))
        .map_or(PredResult::Value(CaptureValue::Nil), PredResult::Value)
}

fn pred_method_of(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let Some(CaptureValue::Node(id)) = first_value(args, ctx) else {
        return PredResult::Value(CaptureValue::Nil);
    };
    let Some(node) = ctx
        .resolved
        .graph()
        .nodes
        .iter()
        .find(|n| n.id == id.as_str())
    else {
        return PredResult::Value(CaptureValue::Nil);
    };
    let method = match node.ast.as_ref().map(|s| &s.root) {
        Some(crate::resolved::ast_shape::AstNode::Call(c)) => c.method().to_string(),
        Some(crate::resolved::ast_shape::AstNode::Assignment(a)) => {
            if let crate::resolved::ast_shape::AstNode::Call(c) = a.rhs.as_ref() {
                c.method().to_string()
            } else {
                return PredResult::Value(CaptureValue::Nil);
            }
        }
        _ => return PredResult::Value(CaptureValue::Nil),
    };
    PredResult::Value(CaptureValue::String(method.into()))
}

fn pred_method_chain_of(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let Some(CaptureValue::Node(id)) = first_value(args, ctx) else {
        return PredResult::Value(CaptureValue::Nil);
    };
    let Some(node) = ctx
        .resolved
        .graph()
        .nodes
        .iter()
        .find(|n| n.id == id.as_str())
    else {
        return PredResult::Value(CaptureValue::Nil);
    };
    let chain = match node.ast.as_ref().map(|s| &s.root) {
        Some(crate::resolved::ast_shape::AstNode::Call(c)) => c.method_chain.clone(),
        _ => return PredResult::Value(CaptureValue::Nil),
    };
    PredResult::Value(CaptureValue::List(
        chain
            .into_iter()
            .map(|s| CaptureValue::String(s.into()))
            .collect(),
    ))
}

fn pred_line_of(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let Some(cap) = first_value(args, ctx) else {
        return PredResult::Value(CaptureValue::Nil);
    };
    let line = match cap {
        CaptureValue::Node(id) => ctx
            .resolved
            .graph()
            .nodes
            .iter()
            .find(|n| n.id == id.as_str())
            .and_then(|n| n.line_number),
        _ => None,
    };
    line.map_or(PredResult::Value(CaptureValue::Nil), |l| {
        PredResult::Value(CaptureValue::Number(f64::from(l)))
    })
}

fn pred_column_of(_args: &[PredArg], _ctx: &MatchCtx) -> PredResult {
    // `Node` doesn't carry a column today; AstNode::Call.column does.
    // Matcher's AST-bound captures expose it in commit 7.
    PredResult::Value(CaptureValue::Nil)
}

fn pred_fqn_of(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let Some(CaptureValue::Node(id)) = first_value(args, ctx) else {
        return PredResult::Value(CaptureValue::Nil);
    };
    let Some(node) = ctx
        .resolved
        .graph()
        .nodes
        .iter()
        .find(|n| n.id == id.as_str())
    else {
        return PredResult::Value(CaptureValue::Nil);
    };
    let fqns: Vec<CaptureValue> = node
        .tables_referenced
        .iter()
        .map(|t| CaptureValue::String(t.fqn().into()))
        .collect();
    PredResult::Value(CaptureValue::List(fqns))
}

fn pred_overlay_of(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    // (overlay-of @cap :stage :input-bytes) — extract a numeric
    // value out of an overlay. Three-arg form: capture, overlay kind,
    // field name.
    let cap = first_value(args, ctx);
    let kind = args
        .get(1)
        .and_then(|a| resolve_arg(a, ctx))
        .and_then(|v| v.as_str_value())
        .map(|s| s.trim_start_matches(':').to_string());
    let field = args
        .get(2)
        .and_then(|a| resolve_arg(a, ctx))
        .and_then(|v| v.as_str_value())
        .map(|s| s.trim_start_matches(':').to_string());
    let (Some(CaptureValue::Node(id)), Some(kind), Some(field)) = (cap, kind, field) else {
        return PredResult::Value(CaptureValue::Nil);
    };
    let Some(overlay) = ctx.resolved.overlay(id.as_str()) else {
        return PredResult::Value(CaptureValue::Nil);
    };
    let n = match (kind.as_str(), field.as_str()) {
        ("stage", "input-bytes") => overlay.observed_input_bytes().map(|v| v as f64),
        ("stage", "shuffle-read-bytes") => overlay.observed_shuffle_read_bytes().map(|v| v as f64),
        ("stage", "count") => Some(overlay.stages.len() as f64),
        ("plan", "node-count") => overlay.plan_subtree.as_ref().map(|p| p.nodes.len() as f64),
        _ => None,
    };
    n.map_or(PredResult::Value(CaptureValue::Nil), |v| {
        PredResult::Value(CaptureValue::Number(v))
    })
}

// ----------------------------------------------------------------------
// Conditional finding mutation
// ----------------------------------------------------------------------

fn pred_when(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    // (#when <trigger-pred> :confidence "High" :severity "Error" :message-suffix "...")
    let Some(trigger) = args.first() else {
        return PredResult::Bool(false);
    };
    if !evaluate_inner(trigger, ctx).as_bool() {
        return PredResult::Skip;
    }
    let remaining = args.len() - 1;
    if remaining % 2 != 0 {
        return PredResult::Bool(false);
    }
    let mut mutation = FindingMutation::default();
    let mut i = 1;
    while i < args.len() {
        let Some(key) = resolve_arg(&args[i], ctx).and_then(|v| v.as_str_value()) else {
            i += 1;
            continue;
        };
        let key = key.trim_start_matches(':').to_string();
        let Some(value) = args.get(i + 1).and_then(|a| resolve_arg(a, ctx)) else {
            break;
        };
        match key.as_str() {
            "confidence" => mutation.confidence = parse_confidence(&value),
            "severity" => mutation.severity = parse_severity(&value),
            "message-suffix" => mutation.message_suffix = value.as_str_value(),
            "message" => mutation.message = value.as_str_value(),
            _ => {}
        }
        i += 2;
    }
    PredResult::SetFinding(mutation)
}

fn parse_confidence(v: &CaptureValue) -> Option<Confidence> {
    let s = v.as_str_value()?;
    match s.to_ascii_lowercase().as_str() {
        "high" => Some(Confidence::High),
        "medium" => Some(Confidence::Medium),
        "low" => Some(Confidence::Low),
        "none" => Some(Confidence::None),
        _ => None,
    }
}

fn parse_severity(v: &CaptureValue) -> Option<Severity> {
    let s = v.as_str_value()?;
    match s.to_ascii_lowercase().as_str() {
        "error" => Some(Severity::Error),
        "warning" => Some(Severity::Warning),
        "info" => Some(Severity::Info),
        _ => None,
    }
}

// ----------------------------------------------------------------------
// Traversal subjects — value-returning predicates that yield a list of
// related nodes. Used as the subject in quantifiers (#all / #any / etc.).
// ----------------------------------------------------------------------

fn extract_node_id(v: &CaptureValue) -> Option<crate::resolved::ids::StaticNodeId> {
    match v {
        CaptureValue::Node(id) => Some(id.clone()),
        _ => None,
    }
}

fn current_or_arg_node(
    args: &[PredArg],
    ctx: &MatchCtx,
) -> Option<crate::resolved::ids::StaticNodeId> {
    if let Some(v) = first_value(args, ctx).and_then(|v| extract_node_id(&v)) {
        return Some(v);
    }
    ctx.captures.get("__current").and_then(extract_node_id)
}

fn pred_descendants(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let Some(id) = current_or_arg_node(args, ctx) else {
        return PredResult::Value(CaptureValue::List(Vec::new()));
    };
    let list = crate::rules::graph_dsl::traversal::descendants_of(ctx.resolved, &id);
    PredResult::Value(CaptureValue::List(list))
}

fn pred_ancestors(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let Some(id) = current_or_arg_node(args, ctx) else {
        return PredResult::Value(CaptureValue::List(Vec::new()));
    };
    let list = crate::rules::graph_dsl::traversal::ancestors_of(ctx.resolved, &id);
    PredResult::Value(CaptureValue::List(list))
}

fn pred_siblings(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let Some(id) = current_or_arg_node(args, ctx) else {
        return PredResult::Value(CaptureValue::List(Vec::new()));
    };
    let list = crate::rules::graph_dsl::traversal::siblings_of(ctx.resolved, &id);
    PredResult::Value(CaptureValue::List(list))
}

fn pred_receiver_of(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    // (#receiver-of @call) → returns the dotted receiver chain as a
    // string, or Nil if the capture isn't a Call.
    let Some(cap) = first_value(args, ctx) else {
        return PredResult::Value(CaptureValue::Nil);
    };
    let arg = match cap {
        CaptureValue::AstArg(a) => *a,
        // Receiver-of on a Node falls back to the node's AST Call shape.
        CaptureValue::Node(id) => {
            let Some(node) = ctx
                .resolved
                .graph()
                .nodes
                .iter()
                .find(|n| n.id == id.as_str())
            else {
                return PredResult::Value(CaptureValue::Nil);
            };
            match node.ast.as_ref().map(|s| &s.root) {
                Some(crate::resolved::ast_shape::AstNode::Call(c)) => {
                    crate::resolved::ast_shape::AstArg::Call(Box::new(c.clone()))
                }
                _ => return PredResult::Value(CaptureValue::Nil),
            }
        }
        _ => return PredResult::Value(CaptureValue::Nil),
    };
    match crate::rules::graph_dsl::traversal::receiver_of_call(&arg) {
        Some(s) => PredResult::Value(CaptureValue::String(std::sync::Arc::from(s))),
        None => PredResult::Value(CaptureValue::Nil),
    }
}

fn pred_callees_of(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let Some(cap) = first_value(args, ctx) else {
        return PredResult::Value(CaptureValue::List(Vec::new()));
    };
    let arg = match cap {
        CaptureValue::AstArg(a) => *a,
        CaptureValue::Node(id) => match ctx
            .resolved
            .graph()
            .nodes
            .iter()
            .find(|n| n.id == id.as_str())
            .and_then(|n| n.ast.as_ref())
            .map(|s| &s.root)
        {
            Some(crate::resolved::ast_shape::AstNode::Call(c)) => {
                crate::resolved::ast_shape::AstArg::Call(Box::new(c.clone()))
            }
            _ => return PredResult::Value(CaptureValue::List(Vec::new())),
        },
        _ => return PredResult::Value(CaptureValue::List(Vec::new())),
    };
    let list = crate::rules::graph_dsl::traversal::callees_of_call(&arg);
    PredResult::Value(CaptureValue::List(list))
}

// ----------------------------------------------------------------------
// §B+ predicates — full implementations
// ----------------------------------------------------------------------

fn pred_stub_false(_args: &[PredArg], _ctx: &MatchCtx) -> PredResult {
    PredResult::Bool(false)
}

/// `(#not-receiver-of @call "method")` — true iff the call's receiver
/// is *not* a method whose name matches. Used by BP008-style rules
/// that check `.collect()` isn't preceded by `.limit()`.
fn pred_not_receiver_of(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let Some(cap) = first_value(args, ctx) else {
        return PredResult::Bool(true);
    };
    let Some(method_name) = args
        .get(1)
        .and_then(|a| resolve_arg(a, ctx))
        .and_then(|v| v.as_str_value())
    else {
        return PredResult::Bool(true);
    };

    let chain: Vec<String> = match cap {
        CaptureValue::AstArg(arg) => match *arg {
            crate::resolved::ast_shape::AstArg::Call(c) => c.method_chain.clone(),
            _ => return PredResult::Bool(true),
        },
        CaptureValue::Node(id) => ctx
            .resolved
            .graph()
            .nodes
            .iter()
            .find(|n| n.id == id.as_str())
            .and_then(|n| n.ast.as_ref())
            .and_then(|s| match &s.root {
                crate::resolved::ast_shape::AstNode::Call(c) => Some(c.method_chain.clone()),
                _ => None,
            })
            .unwrap_or_default(),
        _ => return PredResult::Bool(true),
    };

    if chain.len() < 2 {
        return PredResult::Bool(true);
    }
    // Receiver tokens — everything before the leaf. When the receiver is
    // itself a nested call expression (e.g. `df.limit(100)`), the chain
    // element holds the raw source text; check whether the method name
    // appears as a dotted method call inside it.
    let receivers = &chain[..chain.len() - 1];
    let dotted = format!(".{method_name}(");
    let bare = format!("{method_name}(");
    let found = receivers
        .iter()
        .any(|m| m == &method_name || m.contains(&dotted) || m.starts_with(&bare));
    PredResult::Bool(!found)
}

/// `(#kwargs/missing @call ["a" "b" "c"])` — true iff *none* of the
/// listed kwarg names appear in the call. Useful for "required option
/// missing" checks like BP021's JDBC partition options.
fn pred_kwargs_missing(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    kwargs_set_check(args, ctx, |present, required| {
        !required.iter().any(|r| present.contains(r))
    })
}

/// `(#kwargs/has @call ["a" "b"])` — true iff *all* listed kwargs appear.
fn pred_kwargs_has(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    kwargs_set_check(args, ctx, |present, required| {
        required.iter().all(|r| present.contains(r))
    })
}

fn kwargs_set_check(
    args: &[PredArg],
    ctx: &MatchCtx,
    f: impl Fn(&std::collections::HashSet<String>, &[String]) -> bool,
) -> PredResult {
    let Some(cap) = first_value(args, ctx) else {
        return PredResult::Bool(false);
    };
    let names = match cap {
        CaptureValue::AstArg(arg) => match *arg {
            crate::resolved::ast_shape::AstArg::Call(c) => {
                crate::rules::graph_dsl::traversal::kwarg_names(
                    &crate::resolved::ast_shape::AstNode::Call(*c),
                )
            }
            _ => return PredResult::Bool(false),
        },
        CaptureValue::Node(id) => ctx
            .resolved
            .graph()
            .nodes
            .iter()
            .find(|n| n.id == id.as_str())
            .and_then(|n| n.ast.as_ref())
            .map(|s| crate::rules::graph_dsl::traversal::kwarg_names(&s.root))
            .unwrap_or_default(),
        _ => return PredResult::Bool(false),
    };
    let present: std::collections::HashSet<String> = names.into_iter().collect();
    let Some(list_arg) = args.get(1).and_then(|a| resolve_arg(a, ctx)) else {
        return PredResult::Bool(false);
    };
    let required: Vec<String> = match list_arg {
        CaptureValue::List(items) => items.iter().filter_map(|v| v.as_str_value()).collect(),
        other => other.as_str_value().into_iter().collect(),
    };
    PredResult::Bool(f(&present, &required))
}

/// `(#prop "description" <inner-predicate>)` — invariant check. The
/// description is preserved on the FindingMutation as a message-suffix
/// hint so the rule author can drop the property text into the
/// emitted message via `{prop.description}`. Returns SetFinding when
/// the inner predicate is false (the *invariant fails*) so the rule
/// fires on violations rather than on passes.
fn pred_prop(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    let description = args
        .first()
        .and_then(|a| resolve_arg(a, ctx))
        .and_then(|v| v.as_str_value())
        .unwrap_or_default();
    let inner_ok = args
        .get(1)
        .is_some_and(|a| evaluate_inner(a, ctx).as_bool());
    if inner_ok {
        return PredResult::Bool(true);
    }
    let mut m = FindingMutation::default();
    if !description.is_empty() {
        m.message_suffix = Some(format!("invariant '{description}' failed"));
    }
    PredResult::SetFinding(m)
}

// ----------------------------------------------------------------------
// Dynamic-argument predicates
// ----------------------------------------------------------------------

/// `(#arg-is-dynamic @node :arg/0)` — true iff the Nth positional argument
/// of the call node is a dynamically-constructed string (f-string, binary
/// concatenation, .format(), %-format) or a bare identifier (variable
/// reference that may carry a dynamic value at runtime).
///
/// Use this to detect `spark.sql(f"SELECT FROM {t}")` at the AST level
/// rather than with a fragile source-text regex.
fn pred_arg_is_dynamic(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    use crate::resolved::ast_shape::AstArg;
    let arg = arg_at_index(args, ctx);
    PredResult::Bool(matches!(
        arg.as_deref(),
        Some(
            AstArg::FString { .. }
                | AstArg::BinaryOp { .. }
                | AstArg::DotFormat { .. }
                | AstArg::PercentFormat { .. }
                | AstArg::Identifier(_)
        )
    ))
}

/// `(#arg-kind-of @node :arg/0)` — returns the kind string of the Nth
/// positional argument ("FString", "BinaryOp", "Literal", "Identifier",
/// "DotFormat", "PercentFormat", "Call", "Attribute", "Unknown", …).
///
/// Used with `#eq?` to test specific argument shapes:
/// `(#eq? (#arg-kind-of @n :arg/0) "FString")`
fn pred_arg_kind_of(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    match arg_at_index(args, ctx) {
        Some(arg) => {
            let kind = crate::rules::graph_dsl::value::ast_arg_kind(&arg);
            PredResult::Value(CaptureValue::String(std::sync::Arc::from(kind)))
        }
        None => PredResult::Value(CaptureValue::Nil),
    }
}

/// Extract the `AstArg` at the positional index specified by the second
/// argument (`@node` is first, `:arg/N` is second). The node is resolved
/// from either a captured `Node` or `AstArg::Call` capture value.
///
/// Returns `None` when the node cannot be found or the index is out of range.
fn arg_at_index(
    args: &[PredArg],
    ctx: &MatchCtx,
) -> Option<Box<crate::resolved::ast_shape::AstArg>> {
    use crate::resolved::ast_shape::{AstArg, AstNode};

    let cap = first_value(args, ctx)?;
    let call_node = match cap {
        CaptureValue::Node(id) => {
            let node = ctx
                .resolved
                .graph()
                .nodes
                .iter()
                .find(|n| n.id == id.as_str())?;
            match node.ast.as_ref().map(|s| &s.root) {
                Some(AstNode::Call(c)) => c.clone(),
                _ => return None,
            }
        }
        CaptureValue::AstArg(a) => match *a {
            AstArg::Call(c) => *c,
            _ => return None,
        },
        _ => return None,
    };

    // Parse `:arg/N` from second predicate argument.
    let idx: usize = args
        .get(1)
        .and_then(|a| resolve_arg(a, ctx))
        .and_then(|v| v.as_str_value())
        .as_deref()
        .and_then(|s| s.trim_start_matches(":arg/").parse().ok())
        .unwrap_or(0);

    call_node.args.get(idx).map(|a| Box::new(a.clone()))
}

// ----------------------------------------------------------------------
// Catalog-enriched predicates (Issue #72)
// ----------------------------------------------------------------------

fn pred_table_has_property(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    // (#table-has-property @n "delta.feature.clustering")
    // True iff any table referenced by @n has the given key in its table_properties.
    // Returns false when no catalog enrichment has been run (table_properties is empty).
    let node_id = match first_value(args, ctx) {
        Some(CaptureValue::Node(id)) => id,
        _ => return PredResult::Bool(false),
    };
    let key = match args
        .get(1)
        .and_then(|a| resolve_arg(a, ctx))
        .and_then(|v| coerce_string(&v))
    {
        Some(k) => k,
        None => return PredResult::Bool(false),
    };
    let Some(node) = ctx
        .resolved
        .graph()
        .nodes
        .iter()
        .find(|n| n.id == node_id.as_str())
    else {
        return PredResult::Bool(false);
    };
    for tref in &node.tables_referenced {
        if let Some(spec) = ctx.resolved.table_spec(&tref.fqn()) {
            if spec.table_properties.contains_key(&key) {
                return PredResult::Bool(true);
            }
        }
    }
    PredResult::Bool(false)
}

fn pred_join_type_mismatch(args: &[PredArg], ctx: &MatchCtx) -> PredResult {
    // (#join-type-mismatch @n)
    // True iff the join node @n pulls from two tables that share a column name
    // but with differing data types in the catalog schema.
    // Returns false when fewer than two schemas are available — the predicate
    // is a no-op when catalog enrichment has not run.
    let node_id = match first_value(args, ctx) {
        Some(CaptureValue::Node(id)) => id,
        _ => return PredResult::Bool(false),
    };
    let Some(node) = ctx
        .resolved
        .graph()
        .nodes
        .iter()
        .find(|n| n.id == node_id.as_str())
    else {
        return PredResult::Bool(false);
    };
    let schemas: Vec<_> = node
        .tables_referenced
        .iter()
        .filter_map(|tref| {
            ctx.resolved
                .table_spec(&tref.fqn())
                .and_then(|s| s.schema.as_ref())
        })
        .collect();

    if schemas.len() < 2 {
        return PredResult::Bool(false);
    }

    for i in 0..schemas.len() {
        for j in (i + 1)..schemas.len() {
            for col_a in &schemas[i].columns {
                if let Some(col_b) = schemas[j].column(&col_a.name) {
                    let type_a = col_a.data_type.to_ascii_uppercase();
                    let type_b = col_b.data_type.to_ascii_uppercase();
                    if type_a != type_b {
                        return PredResult::Bool(true);
                    }
                }
            }
        }
    }
    PredResult::Bool(false)
}

// ----------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolved::scope_facts::ScopeFacts;
    use crate::resolved::{NodeOverlay, ResolvedGraphBuilder};
    use crate::types::{Edge, Node, OperationKind, ScalingBehavior};
    use std::sync::Arc;

    fn arg_string(s: &str) -> PredArg {
        PredArg::Value(Value::String(Arc::from(s)))
    }

    fn arg_capture(name: &str) -> PredArg {
        PredArg::Value(Value::CaptureRef(Arc::from(name)))
    }

    fn arg_size(b: u64) -> PredArg {
        PredArg::Value(Value::Size(b))
    }

    fn arg_ident(s: &str) -> PredArg {
        PredArg::Value(Value::Ident(Arc::from(s)))
    }

    fn empty_ctx() -> (
        crate::resolved::ResolvedGraph,
        std::collections::HashMap<Arc<str>, CaptureValue>,
    ) {
        let graph = crate::graph::Graph {
            nodes: Vec::new(),
            edges: Vec::<Edge>::new(),
            findings: Vec::new(),
            mode: "python".into(),
            confidence: "low".into(),
        };
        let resolved = ResolvedGraphBuilder::new(graph).build();
        let captures = std::collections::HashMap::new();
        (resolved, captures)
    }

    #[test]
    fn registry_contains_expected_categories() {
        let names = registered_names();
        for required in [
            "and",
            "or",
            "not",
            "xor",
            "implies",
            "eq?",
            "match?",
            "in",
            "starts-with",
            "kind",
            "gt",
            "lt",
            "count",
            "all",
            "any",
            "none",
            "has-overlay",
            "has-provenance",
            "observed-bytes-gt",
            "binds",
            "reads",
            "method-of",
            "fqn-of",
            "when",
        ] {
            assert!(
                names.contains(&required),
                "predicate registry missing {required}"
            );
        }
        assert!(registry_size() >= 35);
    }

    #[test]
    fn lookup_returns_some_for_known_names() {
        assert!(lookup("eq?").is_some());
        assert!(lookup("does-not-exist").is_none());
    }

    #[test]
    fn eq_compares_strings() {
        let (resolved, captures) = empty_ctx();
        let ctx = MatchCtx::new(&resolved, &captures);
        let args = vec![arg_string("foo"), arg_string("foo")];
        assert!(pred_eq(&args, &ctx).as_bool());
        let args = vec![arg_string("foo"), arg_string("bar")];
        assert!(!pred_eq(&args, &ctx).as_bool());
    }

    #[test]
    fn match_regex() {
        let (resolved, captures) = empty_ctx();
        let ctx = MatchCtx::new(&resolved, &captures);
        let args = vec![arg_string("hello123world"), arg_string(r"\d+")];
        assert!(pred_match(&args, &ctx).as_bool());
    }

    #[test]
    fn gt_uses_numeric_coercion() {
        let (resolved, captures) = empty_ctx();
        let ctx = MatchCtx::new(&resolved, &captures);
        let args = vec![
            PredArg::Value(Value::Number(5.0)),
            PredArg::Value(Value::Number(3.0)),
        ];
        assert!(pred_gt(&args, &ctx).as_bool());
    }

    #[test]
    fn and_or_not_composition() {
        let (resolved, captures) = empty_ctx();
        let ctx = MatchCtx::new(&resolved, &captures);
        let true_pred = PredArg::Predicate(Predicate {
            name: "eq?".into(),
            args: vec![arg_string("a"), arg_string("a")],
            line: 0,
            column: 0,
        });
        let false_pred = PredArg::Predicate(Predicate {
            name: "eq?".into(),
            args: vec![arg_string("a"), arg_string("b")],
            line: 0,
            column: 0,
        });
        assert!(pred_and(&[true_pred.clone(), true_pred.clone()], &ctx).as_bool());
        assert!(!pred_and(&[true_pred.clone(), false_pred.clone()], &ctx).as_bool());
        assert!(pred_or(&[false_pred.clone(), true_pred.clone()], &ctx).as_bool());
        assert!(pred_not(&[false_pred.clone()], &ctx).as_bool());
    }

    #[test]
    fn has_overlay_inspects_stage_presence() {
        // Build a graph with one node "a" at line 10; attach a stage.
        let node = Node {
            id: "a".into(),
            kind: OperationKind::Read,
            scaling_type: ScalingBehavior::Linear,
            photon_eligible: false,
            shuffle_required: false,
            driver_bound: false,
            tables_referenced: Vec::new(),
            estimated_input_bytes: None,
            estimated_cost_usd: None,
            line_number: Some(10),
            source_code: None,
            ast: None,
            scope: ScopeFacts::default(),
        };
        let graph = crate::graph::Graph {
            nodes: vec![node],
            edges: Vec::new(),
            findings: Vec::new(),
            mode: "python".into(),
            confidence: "low".into(),
        };

        let raw_stage = crate::resolved::RawStage::try_from_json(&serde_json::json!({
            "stageId": 1,
            "name": "save at f.py:10",
            "inputBytes": 1_000_000_000u64,
        }))
        .unwrap();
        let resolved = ResolvedGraphBuilder::new(graph)
            .with_stages(vec![raw_stage])
            .build();

        // Manually inject __current capture for testing.
        let mut captures: std::collections::HashMap<Arc<str>, CaptureValue> = HashMap::new();
        captures.insert(
            "__current".into(),
            CaptureValue::Node(crate::resolved::StaticNodeId::new("a".to_string())),
        );
        let ctx = MatchCtx::new(&resolved, &captures);

        let args = vec![arg_ident(":stage")];
        assert!(pred_has_overlay(&args, &ctx).as_bool());

        let args = vec![arg_ident(":plan")];
        assert!(!pred_has_overlay(&args, &ctx).as_bool());
    }

    #[test]
    fn observed_bytes_gt_compares_to_size_threshold() {
        let node = Node {
            id: "x".into(),
            kind: OperationKind::Shuffle,
            scaling_type: ScalingBehavior::LinearWithCliff,
            photon_eligible: false,
            shuffle_required: true,
            driver_bound: false,
            tables_referenced: Vec::new(),
            estimated_input_bytes: None,
            estimated_cost_usd: None,
            line_number: Some(5),
            source_code: None,
            ast: None,
            scope: ScopeFacts::default(),
        };
        let graph = crate::graph::Graph {
            nodes: vec![node],
            edges: Vec::new(),
            findings: Vec::new(),
            mode: "python".into(),
            confidence: "low".into(),
        };
        let raw_stage = crate::resolved::RawStage::try_from_json(&serde_json::json!({
            "stageId": 1,
            "name": "shuffle at f.py:5",
            "inputBytes": 2_000_000_000u64,
        }))
        .unwrap();
        let resolved = ResolvedGraphBuilder::new(graph)
            .with_stages(vec![raw_stage])
            .build();

        let mut captures: std::collections::HashMap<Arc<str>, CaptureValue> = HashMap::new();
        captures.insert(
            "x".into(),
            CaptureValue::Node(crate::resolved::StaticNodeId::new("x".to_string())),
        );
        let ctx = MatchCtx::new(&resolved, &captures);

        let args = vec![arg_capture("x"), arg_size(1_000_000_000)];
        assert!(pred_observed_bytes_gt(&args, &ctx).as_bool());

        let args = vec![arg_capture("x"), arg_size(5_000_000_000)];
        assert!(!pred_observed_bytes_gt(&args, &ctx).as_bool());
    }

    #[test]
    fn when_returns_set_finding_on_trigger() {
        let (resolved, captures) = empty_ctx();
        let ctx = MatchCtx::new(&resolved, &captures);
        let trigger = PredArg::Predicate(Predicate {
            name: "eq?".into(),
            args: vec![arg_string("a"), arg_string("a")],
            line: 0,
            column: 0,
        });
        let args = vec![trigger, arg_ident(":confidence"), arg_string("High")];
        let result = pred_when(&args, &ctx);
        match result {
            PredResult::SetFinding(m) => {
                assert!(matches!(m.confidence, Some(Confidence::High)));
            }
            other => panic!("expected SetFinding, got {other:?}"),
        }
    }

    #[test]
    fn when_skips_when_trigger_false() {
        let (resolved, captures) = empty_ctx();
        let ctx = MatchCtx::new(&resolved, &captures);
        let trigger = PredArg::Predicate(Predicate {
            name: "eq?".into(),
            args: vec![arg_string("a"), arg_string("b")],
            line: 0,
            column: 0,
        });
        let args = vec![trigger, arg_ident(":confidence"), arg_string("High")];
        let result = pred_when(&args, &ctx);
        assert!(matches!(result, PredResult::Skip));
    }

    #[test]
    fn in_set_membership() {
        let (resolved, captures) = empty_ctx();
        let ctx = MatchCtx::new(&resolved, &captures);
        let list = PredArg::Value(Value::List(vec![
            Value::String(Arc::from("a")),
            Value::String(Arc::from("b")),
            Value::String(Arc::from("c")),
        ]));
        let args = vec![arg_string("b"), list.clone()];
        assert!(pred_in(&args, &ctx).as_bool());
        let args = vec![arg_string("z"), list];
        assert!(!pred_in(&args, &ctx).as_bool());
    }

    #[test]
    fn unique_detects_duplicates() {
        let (resolved, captures) = empty_ctx();
        let ctx = MatchCtx::new(&resolved, &captures);
        let unique_list = PredArg::Value(Value::List(vec![
            Value::String(Arc::from("a")),
            Value::String(Arc::from("b")),
        ]));
        let dup_list = PredArg::Value(Value::List(vec![
            Value::String(Arc::from("a")),
            Value::String(Arc::from("a")),
        ]));
        assert!(pred_unique(&[unique_list], &ctx).as_bool());
        assert!(!pred_unique(&[dup_list], &ctx).as_bool());
    }

    #[test]
    fn xor_is_odd_count_truthy() {
        let (resolved, captures) = empty_ctx();
        let ctx = MatchCtx::new(&resolved, &captures);
        let t = PredArg::Predicate(Predicate {
            name: "eq?".into(),
            args: vec![arg_string("a"), arg_string("a")],
            line: 0,
            column: 0,
        });
        let f = PredArg::Predicate(Predicate {
            name: "eq?".into(),
            args: vec![arg_string("a"), arg_string("b")],
            line: 0,
            column: 0,
        });
        assert!(pred_xor(&[t.clone()], &ctx).as_bool());
        assert!(!pred_xor(&[t.clone(), t.clone()], &ctx).as_bool());
        assert!(pred_xor(&[t.clone(), f.clone()], &ctx).as_bool());
        assert!(!pred_xor(&[f.clone(), f.clone()], &ctx).as_bool());
    }

    #[test]
    fn implies_is_classical_implication() {
        let (resolved, captures) = empty_ctx();
        let ctx = MatchCtx::new(&resolved, &captures);
        let t = PredArg::Predicate(Predicate {
            name: "eq?".into(),
            args: vec![arg_string("a"), arg_string("a")],
            line: 0,
            column: 0,
        });
        let f = PredArg::Predicate(Predicate {
            name: "eq?".into(),
            args: vec![arg_string("a"), arg_string("b")],
            line: 0,
            column: 0,
        });
        // T → T = T; T → F = F; F → * = T
        assert!(pred_implies(&[t.clone(), t.clone()], &ctx).as_bool());
        assert!(!pred_implies(&[t.clone(), f.clone()], &ctx).as_bool());
        assert!(pred_implies(&[f.clone(), t.clone()], &ctx).as_bool());
        assert!(pred_implies(&[f.clone(), f.clone()], &ctx).as_bool());
    }
}
