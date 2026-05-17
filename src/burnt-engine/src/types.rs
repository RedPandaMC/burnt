use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use sqlparser::ast::ObjectName;
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
/// `TableSpec` overlay attached to `ResolvedGraph`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TableRef {
    pub raw: String,
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub table: String,
    pub is_temp_view: bool,
    pub is_path_read: bool,
    pub path: Option<String>,
}

impl TableRef {
    /// Build from a `sqlparser::ast::ObjectName` (1, 2, or 3 part name).
    /// Quote style is dropped — `fqn` always normalises to unquoted dotted form.
    /// `ObjectNamePart::Function` variants (Snowflake-style dynamic identifiers)
    /// preserve their rendered form so the ref is still greppable.
    pub fn from_object_name(name: &ObjectName) -> Self {
        let parts: Vec<String> = name
            .0
            .iter()
            .map(|part| {
                part.as_ident()
                    .map(|i| i.value.clone())
                    .unwrap_or_else(|| part.to_string())
            })
            .collect();
        let raw = name.to_string();
        match parts.as_slice() {
            [t] => Self {
                raw,
                catalog: None,
                schema: None,
                table: t.clone(),
                is_temp_view: false,
                is_path_read: false,
                path: None,
            },
            [s, t] => Self {
                raw,
                catalog: None,
                schema: Some(s.clone()),
                table: t.clone(),
                is_temp_view: false,
                is_path_read: false,
                path: None,
            },
            [c, s, t] => Self {
                raw,
                catalog: Some(c.clone()),
                schema: Some(s.clone()),
                table: t.clone(),
                is_temp_view: false,
                is_path_read: false,
                path: None,
            },
            _ => {
                // Spark allows up to 3 parts; longer names collapse to the last
                // identifier with the rest joined into schema for diagnostic value.
                let table = parts.last().cloned().unwrap_or_default();
                let head = &parts[..parts.len() - 1];
                Self {
                    raw,
                    catalog: head.first().cloned(),
                    schema: if head.len() >= 2 {
                        Some(head[1..].join("."))
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub source: String,
    pub target: String,
    pub edge_type: String,
}

#[derive(Debug, Clone, Copy, Display, Serialize, Deserialize, PartialEq, Eq)]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum SdpTableKind {
    StreamingTable,
    MaterializedView,
    TemporaryView,
}

#[derive(Debug, Clone, Copy, Display, Serialize, Deserialize, PartialEq, Eq)]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum SdpSourceType {
    CloudFiles,
    Kafka,
    SdpRead,
    DpRead,
    LiveRef,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineTable {
    pub id: String,
    pub name: String,
    pub kind: SdpTableKind,
    pub source_type: SdpSourceType,
    pub inner_nodes: Vec<Node>,
    pub expectations: Vec<String>,
    pub is_incremental: bool,
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
    Sdp,
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

// Types for enhanced rule system with tree-sitter queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPattern {
    pub match_pattern: String,
    pub is_negative: bool,
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
    pub patterns: Vec<QueryPattern>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub has_context: bool,
    #[serde(default)]
    pub has_dataflow: bool,
}

impl AnalysisMode {
    pub fn as_lang_str(&self) -> &'static str {
        match self {
            AnalysisMode::Sdp => "sdp",
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
    pub pipeline: Option<crate::graph::PyPipeline>,
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
            raw: t.raw,
            catalog: t.catalog,
            schema: t.schema,
            table: t.table,
            is_temp_view: t.is_temp_view,
            is_path_read: t.is_path_read,
            path: t.path,
            fqn,
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
            Namespace::Dlt => "dlt".into(),
            Namespace::Dp => "dp".into(),
            Namespace::PandasOnSpark => "pandas_on_spark".into(),
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

#[pyclass]
#[derive(Clone)]
pub struct PyPipelineTable {
    #[pyo3(get)]
    pub id: String,
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub kind: String,
    #[pyo3(get)]
    pub source_type: String,
    #[pyo3(get)]
    pub inner_nodes: Vec<PyNode>,
    #[pyo3(get)]
    pub expectations: Vec<String>,
    #[pyo3(get)]
    pub is_incremental: bool,
}

impl From<PipelineTable> for PyPipelineTable {
    fn from(t: PipelineTable) -> Self {
        PyPipelineTable {
            id: t.id,
            name: t.name,
            kind: t.kind.to_string(),
            source_type: t.source_type.to_string(),
            inner_nodes: t.inner_nodes.into_iter().map(|n| n.into()).collect(),
            expectations: t.expectations,
            is_incremental: t.is_incremental,
        }
    }
}

#[cfg(test)]
mod table_ref_tests {
    use super::*;
    use sqlparser::ast::{SetExpr, Statement, TableFactor};
    use sqlparser::dialect::DatabricksDialect;
    use sqlparser::parser::Parser;

    fn parse_object_name(sql_table: &str) -> ObjectName {
        let sql = format!("SELECT 1 FROM {sql_table}");
        let stmts = Parser::parse_sql(&DatabricksDialect {}, &sql).expect("parse");
        let Statement::Query(q) = stmts.into_iter().next().expect("one stmt") else {
            panic!("expected query");
        };
        let SetExpr::Select(s) = *q.body else {
            panic!("expected select");
        };
        let TableFactor::Table { name, .. } = s.from.into_iter().next().unwrap().relation else {
            panic!("expected table factor");
        };
        name
    }

    #[test]
    fn parse_one_part() {
        let r = TableRef::from_object_name(&parse_object_name("t"));
        assert_eq!(r.catalog, None);
        assert_eq!(r.schema, None);
        assert_eq!(r.table, "t");
        assert_eq!(r.fqn(), "t");
    }

    #[test]
    fn parse_two_part() {
        let r = TableRef::from_object_name(&parse_object_name("sch.t"));
        assert_eq!(r.catalog, None);
        assert_eq!(r.schema.as_deref(), Some("sch"));
        assert_eq!(r.table, "t");
        assert_eq!(r.fqn(), "sch.t");
    }

    #[test]
    fn parse_three_part() {
        let r = TableRef::from_object_name(&parse_object_name("cat.sch.t"));
        assert_eq!(r.catalog.as_deref(), Some("cat"));
        assert_eq!(r.schema.as_deref(), Some("sch"));
        assert_eq!(r.table, "t");
        assert_eq!(r.fqn(), "cat.sch.t");
    }

    #[test]
    fn parse_backtick_quoted_drops_quotes_in_fqn() {
        let r = TableRef::from_object_name(&parse_object_name("`my-cat`.`my sch`.`weird table`"));
        assert_eq!(r.catalog.as_deref(), Some("my-cat"));
        assert_eq!(r.schema.as_deref(), Some("my sch"));
        assert_eq!(r.table, "weird table");
        // fqn normalises to unquoted dotted form — callers can compare directly
        // against TableSpec.fqn keys produced from DESCRIBE output.
        assert_eq!(r.fqn(), "my-cat.my sch.weird table");
    }

    #[test]
    fn dotted_string_matches_object_name_parsing() {
        let from_name = TableRef::from_object_name(&parse_object_name("cat.sch.t"));
        let from_str = TableRef::from_dotted("cat.sch.t");
        assert_eq!(from_name.fqn(), from_str.fqn());
        assert_eq!(from_name.catalog, from_str.catalog);
        assert_eq!(from_name.schema, from_str.schema);
        assert_eq!(from_name.table, from_str.table);
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
