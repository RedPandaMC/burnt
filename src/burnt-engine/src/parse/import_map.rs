use std::collections::HashMap;

// ── Well-known framework constants ─────────────────────────────────────────

/// Module names that map to a Pipeline (DLT / SDP) context.
pub const KNOWN_PIPELINE_MODULES: &[&str] = &[
    "dlt",               // Delta Live Tables (legacy)
    "sdp",               // Spark Declarative Pipelines (internal alias)
    "dp",                // Recommended alias: `from pyspark import pipelines as dp`
    "pyspark.pipelines", // Canonical Spark 4.x module path
];

/// Module names that map to a Spark Session / DataFrame context.
pub const KNOWN_SPARK_FRAMEWORKS: &[&str] = &["pyspark", "spark", "databricks"];

/// Common Spark DataFrame method names used for call-site classification.
pub const SPARK_METHODS: &[&str] = &[
    "read",
    "readStream",
    "write",
    "save",
    "saveAsTable",
    "select",
    "filter",
    "where",
    "withColumn",
    "withColumns",
    "drop",
    "alias",
    "join",
    "groupBy",
    "groupby",
    "orderBy",
    "order_by",
    "sort",
    "limit",
    "distinct",
    "union",
    "unionAll",
    "intersect",
    "except",
    "collect",
    "take",
    "show",
    "count",
    "first",
    "head",
    "cache",
    "persist",
    "unpersist",
    "broadcast",
    "toPandas",
    "to_pandas",
    "toJSON",
    "to_spark",
];

// ── Typed import classification ─────────────────────────────────────────────

/// High-level classification of a Python import's target framework.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QualifiedName {
    /// The DLT / SDP / dp / pyspark.pipelines family of modules.
    Pipeline,
    /// Spark session / DataFrame framework (spark, pyspark, databricks).
    SparkSession,
    /// Any other import not matched above.
    Other,
}

/// What kind of DLT/SDP decorator has been resolved from an `@ns.method` text.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum DecoratorKind {
    /// `@dp.table` / `@dlt.table` / `@sdp.table`
    Table,
    /// `@dp.materialized_view` / `@dlt.materialized_view`
    MaterializedView,
    /// `@dp.temporary_view`
    TemporaryView,
    /// `@dp.append_flow`
    AppendFlow,
    /// `@dp.expect*` / `@dlt.expect*` family
    Expect,
}

// ── Core types ──────────────────────────────────────────────────────────────

/// A single resolved import binding.
#[derive(Debug, Clone)]
pub struct ImportBinding {
    /// The local name used in code (the alias, or the imported name itself).
    #[allow(dead_code)]
    pub local_name: String,
    /// The original module path (e.g. `"dlt"`, `"pyspark.pipelines"`).
    pub module: String,
    /// For `from X import Y [as Z]`, the member name `Y`.
    #[allow(dead_code)]
    pub member: Option<String>,
    /// Resolved framework classification.
    pub qualified: QualifiedName,
}

/// Resolved import map for a single source file.
///
/// Replaces the old `NamespaceTracker`. All call sites that previously used
/// `NamespaceTracker` should migrate to this type; the `NamespaceTracker`
/// type alias in `namespace.rs` provides backward compatibility during
/// the transition.
#[derive(Debug, Clone, Default)]
pub struct ImportMap {
    bindings: HashMap<String, ImportBinding>,
}

impl ImportMap {
    pub fn new() -> Self {
        Self {
            bindings: HashMap::new(),
        }
    }

    /// Build an `ImportMap` by walking a tree-sitter parse tree.
    pub fn build(source: &str, root: tree_sitter::Node) -> Self {
        let mut map = Self::new();
        map.collect_imports(source, root);
        map
    }

    // ── Primary API ─────────────────────────────────────────────────────────

    /// Resolve a local name to its source module string.
    ///
    /// Returns the *module* string (e.g. `"dlt"`, `"pyspark.pipelines"`), not
    /// the local alias. Matches the behaviour of the old `NamespaceTracker::resolve`.
    pub fn resolve(&self, name: &str) -> Option<&str> {
        self.bindings.get(name).map(|b| b.module.as_str())
    }

    /// Returns `true` if `alias` refers to a Pipeline (DLT/SDP/dp) module.
    pub fn is_pipeline_ns(&self, alias: &str) -> bool {
        self.bindings
            .get(alias)
            .is_some_and(|b| b.qualified == QualifiedName::Pipeline)
    }

    /// Returns `true` if `alias` refers to a Spark session/framework module.
    pub fn is_spark_ns(&self, alias: &str) -> bool {
        self.bindings
            .get(alias)
            .is_some_and(|b| b.qualified == QualifiedName::SparkSession)
    }

    /// Returns the resolved module string of the first Pipeline namespace found,
    /// or `None` if no pipeline import is present.
    pub fn pipeline_namespace(&self) -> Option<&str> {
        self.bindings
            .values()
            .find(|b| b.qualified == QualifiedName::Pipeline)
            .map(|b| b.module.as_str())
    }

    /// Classify a decorator text (e.g. `"@dl.table"` or `"@dp.materialized_view(name='x')"`)
    /// into a typed `DecoratorKind`. Returns `None` if not a known pipeline decorator.
    pub fn decorator_kind(&self, decorator_text: &str) -> Option<DecoratorKind> {
        let text = decorator_text.trim().trim_start_matches('@');
        let dot_pos = text.find('.')?;
        let ns_part = &text[..dot_pos];
        if !self.is_pipeline_ns(ns_part) {
            return None;
        }
        let after_dot = &text[dot_pos + 1..];
        // Strip call arguments if present: `materialized_view(...)` → `materialized_view`
        let method = after_dot
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .map(|i| &after_dot[..i])
            .unwrap_or(after_dot);

        match method {
            "table" | "create_table" => Some(DecoratorKind::Table),
            "materialized_view" => Some(DecoratorKind::MaterializedView),
            "temporary_view" => Some(DecoratorKind::TemporaryView),
            "append_flow" => Some(DecoratorKind::AppendFlow),
            m if m.starts_with("expect") => Some(DecoratorKind::Expect),
            _ => None,
        }
    }

    /// Classify a call expression (e.g. `"dl.read(\"raw\")"`) into a typed
    /// `(QualifiedName, method)` pair. Returns `None` for unrecognised calls.
    pub fn call_kind<'a>(&self, call_text: &'a str) -> Option<(QualifiedName, &'a str)> {
        let (ns, method) = self.extract_call_parts(call_text)?;
        let qualified = self
            .bindings
            .get(ns)
            .map(|b| b.qualified.clone())
            .unwrap_or(QualifiedName::Other);
        Some((qualified, method))
    }

    /// Returns `true` if `method_name` is a known Spark DataFrame method.
    pub fn is_spark_method(&self, method_name: &str) -> bool {
        SPARK_METHODS.contains(&method_name)
    }

    /// Split `"spark.read.parquet('path')"` into `("spark", "read.parquet")`.
    ///
    /// Returns `None` if the text contains no `.` or the namespace part is empty.
    pub fn extract_call_parts<'a>(&self, call_text: &'a str) -> Option<(&'a str, &'a str)> {
        let dot_pos = call_text.find('.')?;
        if dot_pos == 0 {
            return None;
        }
        let ns = &call_text[..dot_pos];
        let rest = &call_text[dot_pos + 1..];
        if rest.is_empty() {
            return None;
        }
        let method = rest.find('(').map(|p| &rest[..p]).unwrap_or(rest);
        if method.is_empty() {
            return None;
        }
        Some((ns, method))
    }

    /// Extract just the namespace part of a call expression.
    pub fn resolve_call_namespace<'a>(&self, call_text: &'a str) -> Option<&'a str> {
        self.extract_call_parts(call_text).map(|(ns, _)| ns)
    }

    /// Returns `true` if the call text's namespace resolves to a known Spark
    /// or (optionally) only to a recognised Spark method.
    pub fn is_namespace_match(&self, call_text: &str, framework_methods_only: bool) -> bool {
        let Some((ns, method)) = self.extract_call_parts(call_text) else {
            return false;
        };
        let is_spark = KNOWN_SPARK_FRAMEWORKS.contains(&ns) || self.is_spark_ns(ns);
        if !is_spark {
            return false;
        }
        if framework_methods_only {
            let method_base = method.split('.').next().unwrap_or(method);
            return self.is_spark_method(method_base);
        }
        true
    }

    // ── Test helpers ─────────────────────────────────────────────────────────

    #[cfg(test)]
    pub fn with_simple_import(name: &str) -> Self {
        let mut m = Self::new();
        m.add_simple_import(name);
        m
    }

    #[cfg(test)]
    pub fn add_alias(&mut self, alias: &str, module: &str) {
        self.add_binding(alias, module, None);
    }

    #[cfg(test)]
    pub fn add_simple_import(&mut self, name: &str) {
        self.add_binding(name, name, None);
    }

    #[cfg(test)]
    pub fn add_from_import(&mut self, alias: &str, module: &str) {
        self.add_binding(alias, module, Some(alias));
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    fn add_binding(&mut self, local: &str, module: &str, member: Option<&str>) {
        let qualified = Self::classify_module(module);
        self.bindings.insert(
            local.to_string(),
            ImportBinding {
                local_name: local.to_string(),
                module: module.to_string(),
                member: member.map(str::to_string),
                qualified,
            },
        );
    }

    fn classify_module(module: &str) -> QualifiedName {
        // Exact match against known pipeline roots.
        if KNOWN_PIPELINE_MODULES.contains(&module) {
            return QualifiedName::Pipeline;
        }
        // Submodule form: `dlt.table`, `sdp.view`, `dp.expect`, etc.
        if module.starts_with("dlt.")
            || module.starts_with("sdp.")
            || module.starts_with("dp.")
            || module.starts_with("pyspark.pipelines")
        {
            return QualifiedName::Pipeline;
        }
        if KNOWN_SPARK_FRAMEWORKS.contains(&module) {
            return QualifiedName::SparkSession;
        }
        QualifiedName::Other
    }

    fn collect_imports(&mut self, source: &str, node: tree_sitter::Node) {
        match node.kind() {
            "import_statement" => {
                if let Ok(text) = node.utf8_text(source.as_bytes()) {
                    for (module, local) in parse_import_statement(text) {
                        self.add_binding(&local, &module, None);
                    }
                }
            }
            "import_from_statement" => {
                if let Ok(text) = node.utf8_text(source.as_bytes()) {
                    for (module, local) in parse_import_from_statement(text) {
                        self.add_binding(&local, &module, Some(&local));
                    }
                }
            }
            _ => {}
        }
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.collect_imports(source, child);
        }
    }
}

// ── Import-statement parsing helpers ────────────────────────────────────────

fn parse_import_statement(text: &str) -> Vec<(String, String)> {
    let text = text.trim();
    if !text.starts_with("import") {
        return Vec::new();
    }
    let after_import = text[6..].trim();
    after_import
        .split(',')
        .filter_map(|part| parse_import_target(part.trim()))
        .collect()
}

fn parse_import_from_statement(text: &str) -> Vec<(String, String)> {
    let text = text.trim();
    if !text.starts_with("from") {
        return Vec::new();
    }
    let after_from = text[4..].trim();
    let Some((ns, rest)) = after_from.split_once("import") else {
        return Vec::new();
    };
    let ns = ns.trim().trim_start_matches('.');
    if ns.is_empty() {
        return Vec::new();
    }
    rest.split(',')
        .filter_map(|part| {
            let part = part.trim().trim_end_matches(')').trim();
            if part.is_empty() || part == "*" {
                return None;
            }
            parse_import_target(part).map(|(_actual, alias)| (ns.to_string(), alias))
        })
        .collect()
}

fn parse_import_target(target: &str) -> Option<(String, String)> {
    let target = target.trim();
    if target.is_empty() {
        return None;
    }
    if let Some((actual, alias_with_ws)) = target.split_once(" as ") {
        let alias = alias_with_ws
            .split_whitespace()
            .next()
            .unwrap_or(alias_with_ws);
        if !alias.is_empty() {
            return Some((actual.trim().to_string(), alias.to_string()));
        }
    }
    let name = target.split_whitespace().next().unwrap_or(target);
    if !name.is_empty() {
        Some((name.to_string(), name.to_string()))
    } else {
        None
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_import() {
        let mut map = ImportMap::new();
        map.add_simple_import("sdp");
        assert_eq!(map.resolve("sdp"), Some("sdp"));
        assert!(map.is_pipeline_ns("sdp"));
        assert!(!map.is_pipeline_ns("pandas"));
    }

    #[test]
    fn test_alias_import() {
        let mut map = ImportMap::new();
        map.add_alias("dl", "dlt");
        assert_eq!(map.resolve("dl"), Some("dlt"));
        assert!(map.is_pipeline_ns("dl"));
        assert!(!map.is_spark_ns("dl"));
    }

    #[test]
    fn test_from_import() {
        let mut map = ImportMap::new();
        map.add_from_import("table", "sdp");
        assert_eq!(map.resolve("table"), Some("sdp"));
        assert!(map.is_pipeline_ns("table"));
    }

    #[test]
    fn test_spark_ns() {
        let mut map = ImportMap::new();
        map.add_simple_import("spark");
        assert!(map.is_spark_ns("spark"));
        assert!(!map.is_pipeline_ns("spark"));
    }

    #[test]
    fn test_decorator_kind_table() {
        let mut map = ImportMap::new();
        map.add_alias("dl", "dlt");
        assert_eq!(map.decorator_kind("@dl.table"), Some(DecoratorKind::Table));
        assert_eq!(
            map.decorator_kind("@dl.materialized_view"),
            Some(DecoratorKind::MaterializedView)
        );
        assert_eq!(
            map.decorator_kind("@dl.expect_or_drop"),
            Some(DecoratorKind::Expect)
        );
        assert_eq!(
            map.decorator_kind("@dl.append_flow"),
            Some(DecoratorKind::AppendFlow)
        );
    }

    #[test]
    fn test_decorator_kind_non_pipeline() {
        let mut map = ImportMap::new();
        map.add_simple_import("pandas");
        assert_eq!(map.decorator_kind("@pandas.something"), None);
    }

    #[test]
    fn test_parse_import_statement_simple() {
        let result = parse_import_statement("import sdp");
        assert_eq!(result, vec![("sdp".to_string(), "sdp".to_string())]);
    }

    #[test]
    fn test_parse_import_statement_alias() {
        let result = parse_import_statement("import dlt as dl");
        assert_eq!(result, vec![("dlt".to_string(), "dl".to_string())]);
    }

    #[test]
    fn test_parse_import_statement_multiple() {
        let result = parse_import_statement("import sdp, pandas as pd");
        assert_eq!(result.len(), 2);
        assert!(result.contains(&("sdp".to_string(), "sdp".to_string())));
        assert!(result.contains(&("pandas".to_string(), "pd".to_string())));
    }

    #[test]
    fn test_parse_import_from_statement() {
        let result = parse_import_from_statement("from sdp import table");
        assert_eq!(result, vec![("sdp".to_string(), "table".to_string())]);
    }

    #[test]
    fn test_parse_import_from_statement_alias() {
        let result = parse_import_from_statement("from dlt import table as t");
        assert_eq!(result, vec![("dlt".to_string(), "t".to_string())]);
    }

    #[test]
    fn test_extract_call_parts() {
        let map = ImportMap::new();
        assert_eq!(
            map.extract_call_parts("spark.read.parquet('path')"),
            Some(("spark", "read.parquet"))
        );
        assert_eq!(
            map.extract_call_parts("df.select('x')"),
            Some(("df", "select"))
        );
        assert_eq!(map.extract_call_parts("no_dot()"), None);
        assert_eq!(map.extract_call_parts(".method()"), None);
        assert_eq!(map.extract_call_parts("ns."), None);
    }

    #[test]
    fn test_pipeline_namespace() {
        let mut map = ImportMap::new();
        map.add_alias("dl", "dlt");
        assert!(map.pipeline_namespace().is_some());
        assert_eq!(map.pipeline_namespace(), Some("dlt"));
    }

    #[test]
    fn test_pyspark_pipelines_import() {
        let mut map = ImportMap::new();
        // from pyspark import pipelines as dp
        map.add_binding("dp", "pyspark.pipelines", Some("pipelines"));
        assert!(map.is_pipeline_ns("dp"));
    }
}
