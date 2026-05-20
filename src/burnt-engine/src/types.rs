use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use strum::Display;

/// A resolved reference to a Spark table, view, or path-based dataset.
///
/// Created by the graph builders from SQL `ObjectName`s, Python `.table()` /
/// `spark.table()` literals, and path-based reads. Constructors are
/// infallible — unparseable inputs land in `raw` with single-component
/// `table = raw`.
///
/// `fqn()` returns a stable key used as the join field with the
/// `TableSpec` overlay attached to `ResolvedGraph`. `canonical_key()`
/// returns the FQN lowercased for case-insensitive dedup.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TableRef {
    raw: String,
    catalog: Option<String>,
    schema: Option<String>,
    table: String,
    is_temp_view: bool,
    is_path_read: bool,
    path: Option<String>,
}

impl TableRef {
    /// Build from explicit catalog/schema/table parts (from tree-sitter CST field access).
    /// Quote characters (backtick, double-quote, square brackets) are stripped
    /// so the fqn always normalises to unquoted dotted form.
    pub fn from_parts(catalog: Option<String>, schema: Option<String>, table: String) -> Self {
        let catalog = catalog.map(|s| Self::strip_quotes(&s));
        let schema = schema.map(|s| Self::strip_quotes(&s));
        let table = Self::strip_quotes(&table);
        let raw = match (&catalog, &schema) {
            (Some(c), Some(s)) => format!("{c}.{s}.{table}"),
            (None, Some(s)) => format!("{s}.{table}"),
            _ => table.clone(),
        };
        Self {
            raw,
            catalog,
            schema,
            table,
            is_temp_view: false,
            is_path_read: false,
            path: None,
        }
    }

    fn strip_quotes(s: &str) -> String {
        let s = s.trim();
        if s.len() >= 2 {
            let first = s.as_bytes()[0];
            let last = s.as_bytes()[s.len() - 1];
            if (first == b'`' && last == b'`')
                || (first == b'"' && last == b'"')
                || (first == b'[' && last == b']')
            {
                return s[1..s.len() - 1].to_string();
            }
        }
        s.to_string()
    }

    /// Build from a dotted string literal such as `"cat.sch.tbl"`. Used by
    /// the Python builder when it sees `spark.table("…")` / `.table("…")`.
    /// Identical splitting to [`TableRef::from_object_name`].
    pub fn from_dotted(raw: &str) -> Self {
        let parts: Vec<&str> = raw.split('.').collect();
        match parts.as_slice() {
            [t] => Self {
                raw: raw.to_string(),
                catalog: None,
                schema: None,
                table: (*t).to_string(),
                is_temp_view: false,
                is_path_read: false,
                path: None,
            },
            [s, t] => Self {
                raw: raw.to_string(),
                catalog: None,
                schema: Some((*s).to_string()),
                table: (*t).to_string(),
                is_temp_view: false,
                is_path_read: false,
                path: None,
            },
            [c, s, t] => Self {
                raw: raw.to_string(),
                catalog: Some((*c).to_string()),
                schema: Some((*s).to_string()),
                table: (*t).to_string(),
                is_temp_view: false,
                is_path_read: false,
                path: None,
            },
            _ => {
                let table = parts.last().copied().unwrap_or_default().to_string();
                Self {
                    raw: raw.to_string(),
                    catalog: parts.first().map(|s| (*s).to_string()),
                    schema: if parts.len() >= 3 {
                        Some(parts[1..parts.len() - 1].join("."))
                    } else {
                        None
                    },
                    table,
                    is_temp_view: false,
                    is_path_read: false,
                    path: None,
                }
            }
        }
    }

    /// Build from a path-based read such as `spark.read.parquet("s3://b/k")`.
    /// `table` is the basename of the path so the ref still has a human-readable
    /// short name; `fqn` returns `path:<full>`.
    pub fn from_path(path: &str) -> Self {
        let trimmed = path.trim_end_matches('/');
        let table = trimmed
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or(trimmed)
            .to_string();
        Self {
            raw: path.to_string(),
            catalog: None,
            schema: None,
            table,
            is_temp_view: false,
            is_path_read: true,
            path: Some(path.to_string()),
        }
    }

    /// Build a temp-view ref such as `LIVE.foo` or a `CREATE TEMP VIEW` target.
    pub fn temp_view(name: &str) -> Self {
        Self {
            raw: name.to_string(),
            catalog: None,
            schema: None,
            table: name.to_string(),
            is_temp_view: true,
            is_path_read: false,
            path: None,
        }
    }

    /// Stable join key used by the `TableSpec` overlay. Format:
    /// - path reads → `path:<full path>`
    /// - named refs → `catalog.schema.table` (missing parts omitted)
    pub fn fqn(&self) -> String {
        if self.is_path_read {
            return format!("path:{}", self.path.as_deref().unwrap_or(&self.raw));
        }
        match (&self.catalog, &self.schema) {
            (Some(c), Some(s)) => format!("{c}.{s}.{}", self.table),
            (None, Some(s)) => format!("{s}.{}", self.table),
            (None, None) => self.table.clone(),
            (Some(c), None) => format!("{c}.{}", self.table),
        }
    }

    /// Case-insensitive join key. Returns the same value as `fqn()` but
    /// lowercased so that `CATALOG.SCHEMA.TBL` and `catalog.schema.tbl`
    /// produce the same key — useful for deduplication in builders.
    pub fn canonical_key(&self) -> String {
        self.fqn().to_ascii_lowercase()
    }

    pub fn raw(&self) -> &str {
        &self.raw
    }
    pub fn catalog(&self) -> Option<&str> {
        self.catalog.as_deref()
    }
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }
    pub fn table(&self) -> &str {
        &self.table
    }
    pub fn is_temp_view(&self) -> bool {
        self.is_temp_view
    }
    pub fn is_path_read(&self) -> bool {
        self.is_path_read
    }
    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlFragment {
    pub text: String,
    pub provenance: Provenance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provenance {
    pub source_path: Option<PathBuf>,
    pub start_line: u32,
    pub end_line: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SdpSignal {
    Import,
    Decorator(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonParseResult {
    pub sql_fragments: Vec<SqlFragment>,
    pub sdp_signals: Vec<SdpSignal>,
    pub findings: Vec<Finding>,
    pub dlt_namespace: Option<String>,
}

#[derive(Debug, Clone, Display, Serialize, Deserialize, PartialEq, Eq)]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum OperationKind {
    Read,
    Transform,
    Shuffle,
    Action,
    Write,
    UdfCall,
    Maintenance,
    Unknown,
}

#[derive(Debug, Clone, Display, Serialize, Deserialize, PartialEq, Eq)]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum ScalingBehavior {
    Linear,
    LinearWithCliff,
    Quadratic,
    StepFailure,
    Maintenance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub kind: OperationKind,
    pub scaling_type: ScalingBehavior,
    pub photon_eligible: bool,
    pub shuffle_required: bool,
    pub driver_bound: bool,
    pub tables_referenced: Vec<TableRef>,
    pub estimated_input_bytes: Option<u64>,
    pub estimated_cost_usd: Option<f64>,
    pub line_number: Option<u32>,
    pub source_code: Option<String>,
    /// Symbolic AST captured at parse time. The tree-sitter Tree this came
    /// from is discarded after the builder finishes — `ast` is the only
    /// AST surface rules will ever see. `None` only while builders catch
    /// up to populating every shape (transitional).
    #[serde(default)]
    pub ast: Option<crate::resolved::AstShape>,
    /// Scope facts (namespace, bindings, DAG ancestry, source order)
    /// consumed by DSL predicates that today's Context/Dataflow rules
    /// reach for via side channels.
    #[serde(default)]
    pub scope: crate::resolved::ScopeFacts,
}

#[derive(Debug, Clone, Copy, Display, Serialize, Deserialize, PartialEq, Eq)]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum EdgeKind {
    DataFlow,
    TableDependency,
    Alias,
    Scope,
}

impl EdgeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            EdgeKind::DataFlow => "data_flow",
            EdgeKind::TableDependency => "table_dependency",
            EdgeKind::Alias => "alias",
            EdgeKind::Scope => "scope",
        }
    }
}

impl From<EdgeKind> for String {
    fn from(kind: EdgeKind) -> Self {
        kind.as_str().to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub source: String,
    pub target: String,
    pub edge_type: String,
}

// Core types for Task 01
#[pyclass(eq, eq_int)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CellKind {
    Python,
    Sql,
    RunRef,
}

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cell {
    #[pyo3(get)]
    pub kind: CellKind,
    #[pyo3(get)]
    pub source: String,
    #[pyo3(get)]
    pub byte_offset: u32,
    #[pyo3(get)]
    pub line_offset: u32,
    #[pyo3(get)]
    pub origin_path: Option<PathBuf>,
}

#[pyclass(eq, eq_int)]
#[derive(Debug, Clone, Display, Serialize, Deserialize, PartialEq, Eq)]
#[strum(serialize_all = "snake_case")]
pub enum AnalysisMode {
    Python,
    Sql,
}

#[pyclass(eq, eq_int)]
#[derive(Debug, Clone, Display, Serialize, Deserialize, PartialEq, Eq)]
#[strum(serialize_all = "snake_case")]
pub enum Severity {
    Error,
    Warning,
    Info,
}

#[pymethods]
impl Severity {
    fn __str__(&self) -> String {
        self.to_string()
    }
}

#[pyclass(eq, eq_int)]
#[derive(Debug, Clone, Display, Serialize, Deserialize, PartialEq, Eq)]
#[strum(serialize_all = "snake_case")]
pub enum Confidence {
    Low,
    Medium,
    High,
    None,
}

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Finding {
    #[pyo3(get)]
    pub rule_id: String,
    #[pyo3(get)]
    pub code: String,
    #[pyo3(get)]
    pub severity: Severity,
    #[pyo3(get)]
    pub message: String,
    #[pyo3(get)]
    pub suggestion: Option<String>,
    #[pyo3(get)]
    pub line_number: Option<u32>,
    #[pyo3(get)]
    pub column: Option<u32>,
    #[pyo3(get)]
    pub confidence: Confidence,
}

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleEntry {
    #[pyo3(get)]
    pub id: String,
    #[pyo3(get)]
    pub code: String,
    #[pyo3(get)]
    pub severity: Severity,
    #[pyo3(get)]
    pub language: String,
    #[pyo3(get)]
    pub description: String,
    #[pyo3(get)]
    pub suggestion: String,
    #[pyo3(get)]
    pub category: String,
    #[pyo3(get)]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledRule {
    pub id: String,
    pub code: String,
    pub severity: Severity,
    pub language: String,
    pub description: String,
    pub suggestion: String,
    pub category: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub has_graph: bool,
    /// `[graph].detect` S-expression source.
    #[serde(default)]
    pub graph_detect: String,
    /// `[graph].exclude` S-expression source, if any.
    #[serde(default)]
    pub graph_exclude: Option<String>,
    /// `[graph.finding].severity`. Overrides `severity` when set.
    #[serde(default)]
    pub graph_finding_severity: Option<String>,
    /// `[graph.finding].confidence`. Default is "high" when unset.
    #[serde(default)]
    pub graph_finding_confidence: Option<String>,
    /// `[graph.finding].message`. Defaults to `description`.
    #[serde(default)]
    pub graph_finding_message: Option<String>,
    /// `[graph.finding].suggestion`. Defaults to `suggestion`.
    #[serde(default)]
    pub graph_finding_suggestion: Option<String>,
    /// `[graph.finding].line` — capture-ref like "@call.line".
    #[serde(default)]
    pub graph_finding_line: Option<String>,
    /// Whether this rule requires catalog enrichment to fire.
    ///
    /// Rules with `requires_catalog = true` in their TOML are skipped by the
    /// standard `run_graph_rules` path and only evaluated when a
    /// [`CatalogClient`](crate::catalog::CatalogClient) is available via `run_graph_rules_with_catalog`.
    #[serde(default)]
    pub has_catalog: bool,
}

impl AnalysisMode {
    pub fn as_lang_str(&self) -> &'static str {
        match self {
            AnalysisMode::Sql => "sql",
            AnalysisMode::Python => "python",
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct AnalysisResultPy {
    #[pyo3(get)]
    pub mode: String,
    #[pyo3(get)]
    pub graph: Option<crate::graph::PyGraph>,
    #[pyo3(get)]
    pub findings: Vec<Finding>,
    #[pyo3(get)]
    pub cells: Vec<Cell>,
    #[pyo3(get)]
    pub path: Option<String>,
}

/// PyO3 adapter for [`TableRef`].
///
/// Exposed read-only — table refs flow Rust → Python only. The `fqn` getter
/// is materialised at construction so Python code can use it as a dict key
/// without a method call.
#[pyclass]
#[derive(Clone)]
pub struct PyTableRef {
    #[pyo3(get)]
    pub raw: String,
    #[pyo3(get)]
    pub catalog: Option<String>,
    #[pyo3(get)]
    pub schema: Option<String>,
    #[pyo3(get)]
    pub table: String,
    #[pyo3(get)]
    pub is_temp_view: bool,
    #[pyo3(get)]
    pub is_path_read: bool,
    #[pyo3(get)]
    pub path: Option<String>,
    #[pyo3(get)]
    pub fqn: String,
}

#[pymethods]
impl PyTableRef {
    fn __repr__(&self) -> String {
        format!("TableRef(fqn={:?})", self.fqn)
    }
}

impl From<TableRef> for PyTableRef {
    fn from(t: TableRef) -> Self {
        let fqn = t.fqn();
        PyTableRef {
            raw: t.raw().to_string(),
            catalog: t.catalog().map(String::from),
            schema: t.schema().map(String::from),
            table: t.table().to_string(),
            is_temp_view: t.is_temp_view(),
            is_path_read: t.is_path_read(),
            path: t.path().map(String::from),
            fqn,
        }
    }
}

impl From<PyTableRef> for TableRef {
    fn from(t: PyTableRef) -> Self {
        TableRef {
            raw: t.raw,
            catalog: t.catalog,
            schema: t.schema,
            table: t.table,
            is_temp_view: t.is_temp_view,
            is_path_read: t.is_path_read,
            path: t.path,
        }
    }
}

/// PyO3 read-only handle on an `AstShape`.
///
/// Designed for debugging and rule-author tooling — full structured access
/// to the AST tree from Python would require a recursive pyclass forest
/// PyO3 doesn't bridge cheaply. Instead we expose:
///
/// - `root_kind() -> str` — discriminator on the root variant.
/// - `as_json() -> str` — the full shape serialised for inspection.
/// - `method_chain() -> list[str] | None` — convenience when the root is a `Call`.
///
/// The DSL matcher consumes the Rust-side `AstShape` directly; this class
/// exists for tests and for users writing migration scripts in Python.
#[pyclass(name = "AstShape")]
#[derive(Clone)]
pub struct PyAstShape {
    inner: crate::resolved::AstShape,
}

#[pymethods]
impl PyAstShape {
    fn root_kind(&self) -> &'static str {
        match &self.inner.root {
            crate::resolved::AstNode::Call(_) => "Call",
            crate::resolved::AstNode::Decorator(_) => "Decorator",
            crate::resolved::AstNode::Assignment(_) => "Assignment",
            crate::resolved::AstNode::FunctionDef(_) => "FunctionDef",
            crate::resolved::AstNode::SqlStatement(_) => "SqlStatement",
            crate::resolved::AstNode::SqlExpression(_) => "SqlExpression",
        }
    }

    /// Returns the dotted method chain when the root is a `Call`, otherwise `None`.
    fn method_chain(&self) -> Option<Vec<String>> {
        if let crate::resolved::AstNode::Call(c) = &self.inner.root {
            Some(c.method_chain.clone())
        } else {
            None
        }
    }

    /// Full JSON dump of the AST shape — for tests and debugging.
    fn as_json(&self) -> String {
        serde_json::to_string(&self.inner).unwrap_or_else(|_| "{}".to_string())
    }

    fn __repr__(&self) -> String {
        format!("AstShape(root={})", self.root_kind())
    }
}

impl From<crate::resolved::AstShape> for PyAstShape {
    fn from(s: crate::resolved::AstShape) -> Self {
        Self { inner: s }
    }
}

/// PyO3 read-only handle on a `ScopeFacts` payload.
///
/// Exposes the fields rule authors and debugging tooling reach for from
/// Python. The DSL matcher works against the Rust-side `ScopeFacts`
/// directly.
#[pyclass(name = "ScopeFacts")]
#[derive(Clone)]
pub struct PyScopeFacts {
    inner: crate::resolved::ScopeFacts,
}

#[pymethods]
impl PyScopeFacts {
    /// Namespace name as a string (or `None` if unresolved). User-defined
    /// namespaces surface as `"user:<name>"` so callers can distinguish
    /// them from built-ins without an enum import.
    #[getter]
    fn namespace(&self) -> Option<String> {
        use crate::resolved::Namespace;
        self.inner.namespace.as_ref().map(|ns| match ns {
            Namespace::Spark => "spark".into(),
            Namespace::Pipeline => "pipeline".into(),
            Namespace::UserDefined(name) => format!("user:{name}"),
            Namespace::Unknown => "unknown".into(),
        })
    }

    /// `{var_name: static_node_id}` — bindings live in this scope.
    #[getter]
    fn bindings(&self) -> std::collections::HashMap<String, String> {
        self.inner
            .bindings
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().to_string()))
            .collect()
    }

    #[getter]
    fn reads(&self) -> Vec<String> {
        self.inner.reads.clone()
    }

    #[getter]
    fn writes(&self) -> Vec<String> {
        self.inner.writes.clone()
    }

    #[getter]
    fn source_order(&self) -> u32 {
        self.inner.source_order
    }

    #[getter]
    fn ancestors(&self) -> Vec<String> {
        self.inner
            .ancestors
            .iter()
            .map(|x| x.as_str().to_string())
            .collect()
    }

    #[getter]
    fn descendants(&self) -> Vec<String> {
        self.inner
            .descendants
            .iter()
            .map(|x| x.as_str().to_string())
            .collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "ScopeFacts(namespace={:?}, source_order={}, descendants={})",
            self.namespace(),
            self.inner.source_order,
            self.inner.descendants.len()
        )
    }
}

impl From<crate::resolved::ScopeFacts> for PyScopeFacts {
    fn from(s: crate::resolved::ScopeFacts) -> Self {
        Self { inner: s }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyNode {
    #[pyo3(get)]
    pub id: String,
    #[pyo3(get)]
    pub kind: String,
    #[pyo3(get)]
    pub scaling_type: String,
    #[pyo3(get)]
    pub photon_eligible: bool,
    #[pyo3(get)]
    pub shuffle_required: bool,
    #[pyo3(get)]
    pub driver_bound: bool,
    #[pyo3(get)]
    pub tables_referenced: Vec<PyTableRef>,
    #[pyo3(get)]
    pub estimated_input_bytes: Option<u64>,
    #[pyo3(get)]
    pub estimated_cost_usd: Option<f64>,
    #[pyo3(get)]
    pub line_number: Option<u32>,
    #[pyo3(get)]
    pub source_code: Option<String>,
    #[pyo3(get)]
    pub ast: Option<PyAstShape>,
    #[pyo3(get)]
    pub scope: PyScopeFacts,
}

impl From<Node> for PyNode {
    fn from(n: Node) -> Self {
        PyNode {
            id: n.id,
            kind: n.kind.to_string(),
            scaling_type: n.scaling_type.to_string(),
            photon_eligible: n.photon_eligible,
            shuffle_required: n.shuffle_required,
            driver_bound: n.driver_bound,
            tables_referenced: n.tables_referenced.into_iter().map(Into::into).collect(),
            estimated_input_bytes: n.estimated_input_bytes,
            estimated_cost_usd: n.estimated_cost_usd,
            line_number: n.line_number,
            source_code: n.source_code,
            ast: n.ast.map(PyAstShape::from),
            scope: PyScopeFacts::from(n.scope),
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyEdge {
    #[pyo3(get)]
    pub source: String,
    #[pyo3(get)]
    pub target: String,
    #[pyo3(get)]
    pub edge_type: String,
}

impl From<Edge> for PyEdge {
    fn from(e: Edge) -> Self {
        PyEdge {
            source: e.source,
            target: e.target,
            edge_type: e.edge_type,
        }
    }
}

#[cfg(test)]
mod table_ref_tests {
    use super::*;

    #[test]
    fn from_parts_one_part() {
        let r = TableRef::from_parts(None, None, "t".into());
        assert_eq!(r.catalog, None);
        assert_eq!(r.schema, None);
        assert_eq!(r.table, "t");
        assert_eq!(r.fqn(), "t");
    }

    #[test]
    fn from_parts_two_part() {
        let r = TableRef::from_parts(None, Some("sch".into()), "t".into());
        assert_eq!(r.catalog, None);
        assert_eq!(r.schema.as_deref(), Some("sch"));
        assert_eq!(r.table, "t");
        assert_eq!(r.fqn(), "sch.t");
    }

    #[test]
    fn from_parts_three_part() {
        let r = TableRef::from_parts(Some("cat".into()), Some("sch".into()), "t".into());
        assert_eq!(r.catalog.as_deref(), Some("cat"));
        assert_eq!(r.schema.as_deref(), Some("sch"));
        assert_eq!(r.table, "t");
        assert_eq!(r.fqn(), "cat.sch.t");
    }

    #[test]
    fn from_parts_strips_backtick_quotes() {
        let r = TableRef::from_parts(
            Some("`my-cat`".into()),
            Some("`my sch`".into()),
            "`weird table`".into(),
        );
        assert_eq!(r.catalog.as_deref(), Some("my-cat"));
        assert_eq!(r.schema.as_deref(), Some("my sch"));
        assert_eq!(r.table, "weird table");
        assert_eq!(r.fqn(), "my-cat.my sch.weird table");
    }

    #[test]
    fn dotted_string_consistent_with_from_parts() {
        let from_parts = TableRef::from_parts(Some("cat".into()), Some("sch".into()), "t".into());
        let from_str = TableRef::from_dotted("cat.sch.t");
        assert_eq!(from_parts.fqn(), from_str.fqn());
        assert_eq!(from_parts.catalog, from_str.catalog);
        assert_eq!(from_parts.schema, from_str.schema);
        assert_eq!(from_parts.table, from_str.table);
    }

    #[test]
    fn path_read_uses_basename_and_path_prefix_in_fqn() {
        let r = TableRef::from_path("s3://my-bucket/warehouse/events/");
        assert!(r.is_path_read);
        assert_eq!(r.table, "events");
        assert_eq!(r.fqn(), "path:s3://my-bucket/warehouse/events/");
    }

    #[test]
    fn temp_view_is_marked() {
        let r = TableRef::temp_view("live_foo");
        assert!(r.is_temp_view);
        assert_eq!(r.table, "live_foo");
        assert_eq!(r.fqn(), "live_foo");
    }

    #[test]
    fn pytable_ref_materialises_fqn_at_construction() {
        let r = TableRef::from_dotted("a.b.c");
        let py: PyTableRef = r.into();
        assert_eq!(py.fqn, "a.b.c");
    }
}
