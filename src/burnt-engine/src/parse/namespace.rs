use std::collections::HashMap;

pub const KNOWN_FRAMEWORKS: &[&str] = &["pyspark", "spark", "databricks"];

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

#[derive(Debug, Clone)]
pub struct NamespaceTracker {
    simple_imports: HashMap<String, String>,
    aliases: HashMap<String, String>,
    from_imports: HashMap<String, String>,
}

impl NamespaceTracker {
    pub fn new() -> Self {
        Self {
            simple_imports: HashMap::new(),
            aliases: HashMap::new(),
            from_imports: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub fn with_simple_import(name: &str) -> Self {
        let mut t = Self::new();
        t.simple_imports.insert(name.to_string(), name.to_string());
        t
    }

    pub fn resolve(&self, name: &str) -> Option<&str> {
        self.aliases
            .get(name)
            .or_else(|| self.simple_imports.get(name))
            .or_else(|| self.from_imports.get(name))
            .map(|s| s.as_str())
    }

    fn is_known_dlt_namespace(ns: &str) -> bool {
        ns == "sdp" || ns == "dlt" || ns == "dp"
    }

    pub fn is_dlt_namespace(&self, alias: &str) -> bool {
        self.resolve(alias).map_or(false, |ns| {
            Self::is_known_dlt_namespace(ns)
                || ns.starts_with("sdp.")
                || ns.starts_with("dlt.")
                || ns.starts_with("dp.")
        })
    }

    pub fn dlt_namespace(&self) -> Option<&str> {
        for (alias, _) in self.aliases.iter().chain(self.simple_imports.iter()) {
            if self.is_dlt_namespace(alias) {
                return self
                    .resolve(alias)
                    .filter(|ns| Self::is_known_dlt_namespace(ns));
            }
        }
        for (alias, _) in self.from_imports.iter() {
            if self.is_dlt_namespace(alias) {
                return self
                    .resolve(alias)
                    .filter(|ns| Self::is_known_dlt_namespace(ns));
            }
        }
        None
    }

    pub fn is_table_decorator(&self, decorator_name: &str) -> bool {
        self.resolve(decorator_name).map_or(false, |ns| {
            Self::is_known_dlt_namespace(ns)
                || ns.starts_with("sdp.")
                || ns.starts_with("dlt.")
                || ns.starts_with("dp.")
        })
    }

    pub fn is_read_call(&self, namespace: &str) -> bool {
        self.resolve(namespace)
            .map(|ns| ns == "sdp" || ns == "dlt" || ns == "dp")
            .unwrap_or(false)
    }

    pub fn is_spark_namespace(&self, alias: &str) -> bool {
        self.resolve(alias)
            .map(|ns| KNOWN_FRAMEWORKS.iter().any(|f| *f == ns))
            .unwrap_or(false)
    }

    pub fn is_spark_method(&self, method_name: &str) -> bool {
        SPARK_METHODS.iter().any(|m| *m == method_name)
    }

    pub fn extract_call_parts<'a>(&self, call_text: &'a str) -> Option<(&'a str, &'a str)> {
        if let Some(dot_pos) = call_text.find('.') {
            if dot_pos > 0 {
                let ns = &call_text[..dot_pos];
                let rest = &call_text[dot_pos + 1..];
                if !ns.is_empty() && !rest.is_empty() {
                    if let Some(paren_pos) = rest.find('(') {
                        let method = &rest[..paren_pos];
                        if !method.is_empty() {
                            return Some((ns, method));
                        }
                    } else {
                        return Some((ns, rest));
                    }
                }
            }
        }
        None
    }

    pub fn resolve_call_namespace<'a>(&self, call_text: &'a str) -> Option<&'a str> {
        self.extract_call_parts(call_text).map(|(ns, _)| ns)
    }

    pub fn is_namespace_match(&self, call_text: &str, framework_methods_only: bool) -> bool {
        if let Some((ns, method)) = self.extract_call_parts(call_text) {
            if KNOWN_FRAMEWORKS.iter().any(|f| *f == ns) {
                if framework_methods_only {
                    let method_base = method.split('.').next().unwrap_or(method);
                    return self.is_spark_method(method_base);
                }
                return true;
            }
            let resolved = self.resolve(ns);
            if let Some(resolved_ns) = resolved {
                if KNOWN_FRAMEWORKS.iter().any(|f| *f == resolved_ns) {
                    if framework_methods_only {
                        let method_base = method.split('.').next().unwrap_or(method);
                        return self.is_spark_method(method_base);
                    }
                    return true;
                }
            }
        }
        false
    }

    #[cfg(test)]
    pub fn add_alias(&mut self, alias: &str, actual: &str) {
        self.aliases.insert(alias.to_string(), actual.to_string());
    }

    #[cfg(test)]
    pub fn add_simple_import(&mut self, name: &str) {
        self.simple_imports
            .insert(name.to_string(), name.to_string());
    }

    #[cfg(test)]
    pub fn add_from_import(&mut self, alias: &str, actual: &str) {
        self.from_imports
            .insert(alias.to_string(), actual.to_string());
    }
}

impl Default for NamespaceTracker {
    fn default() -> Self {
        Self::new()
    }
}

pub fn build_namespace_tracker(source: &str, root: tree_sitter::Node) -> NamespaceTracker {
    let mut tracker = NamespaceTracker::new();
    collect_imports(source, root, &mut tracker);
    tracker
}

fn collect_imports(source: &str, node: tree_sitter::Node, tracker: &mut NamespaceTracker) {
    match node.kind() {
        "import_statement" => {
            let text = node.utf8_text(source.as_bytes()).unwrap_or("");
            for (actual, alias) in parse_import_statement(text) {
                tracker.aliases.insert(alias, actual);
            }
        }
        "import_from_statement" => {
            let text = node.utf8_text(source.as_bytes()).unwrap_or("");
            for (actual, alias) in parse_import_from_statement(text) {
                tracker.from_imports.insert(alias, actual);
            }
        }
        _ => {}
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_imports(source, child, tracker);
    }
}

fn parse_import_statement(text: &str) -> Vec<(String, String)> {
    let text = text.trim();
    let mut results = Vec::new();

    if !text.starts_with("import") {
        return results;
    }

    let after_import = &text[6..].trim();
    for part in after_import.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((actual, alias)) = parse_import_target(part) {
            results.push((actual, alias));
        }
    }
    results
}

fn parse_import_from_statement(text: &str) -> Vec<(String, String)> {
    let text = text.trim();
    let mut results = Vec::new();

    if !text.starts_with("from") {
        return results;
    }

    let after_from = text[4..].trim();
    let Some((ns, rest)) = after_from.split_once("import") else {
        return results;
    };
    let ns = ns.trim().trim_start_matches('.');
    if ns.is_empty() {
        return results;
    }

    for part in rest.split(',') {
        let part = part.trim().trim_end_matches(')').trim();
        if part.is_empty() || part == "*" {
            continue;
        }
        if let Some((actual, alias)) = parse_import_target(part) {
            results.push((ns.to_string(), alias));
        }
    }
    results
}

fn parse_import_target(target: &str) -> Option<(String, String)> {
    let target = target.trim();
    if target.is_empty() {
        return None;
    }
    if let Some((actual, alias_with_ws)) = target.split_once(" as ") {
        let alias = alias_with_ws
            .trim()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_import() {
        let mut tracker = NamespaceTracker::new();
        tracker
            .simple_imports
            .insert("sdp".to_string(), "sdp".to_string());
        assert_eq!(tracker.resolve("sdp"), Some("sdp"));
        assert!(tracker.is_dlt_namespace("sdp"));
        assert!(!tracker.is_dlt_namespace("pandas"));
    }

    #[test]
    fn test_extract_call_parts() {
        let tracker = NamespaceTracker::new();
        assert_eq!(
            tracker.extract_call_parts("spark.read.parquet('path')"),
            Some(("spark", "read.parquet"))
        );
        assert_eq!(
            tracker.extract_call_parts("my_spark.read.csv('f')"),
            Some(("my_spark", "read.csv"))
        );
        assert_eq!(
            tracker.extract_call_parts("df.select('x')"),
            Some(("df", "select"))
        );
        assert_eq!(
            tracker.extract_call_parts("spark.readStream.format('kafka')"),
            Some(("spark", "readStream.format"))
        );
        assert_eq!(tracker.extract_call_parts("no_dot()"), None);
        assert_eq!(tracker.extract_call_parts(".method()"), None);
        assert_eq!(tracker.extract_call_parts("ns."), None);
    }

    #[test]
    fn test_resolve_call_namespace() {
        let mut tracker = NamespaceTracker::new();
        tracker.add_alias("my_spark", "spark");
        assert_eq!(
            tracker.resolve_call_namespace("my_spark.read.parquet('x')"),
            Some("my_spark")
        );
        assert_eq!(
            tracker.resolve_call_namespace("spark.read.parquet('x')"),
            Some("spark")
        );
        assert_eq!(
            tracker.resolve_call_namespace("unknown.read.parquet('x')"),
            Some("unknown")
        );
    }

    #[test]
    fn test_is_namespace_match() {
        let mut tracker = NamespaceTracker::new();
        tracker.add_alias("my_spark", "spark");
        tracker.add_alias("s", "sdp");
        assert!(tracker.is_namespace_match("my_spark.read.parquet('x')", false));
        assert!(tracker.is_namespace_match("spark.read.parquet('x')", false));
        assert!(tracker.is_namespace_match("my_spark.select('x')", false));
        assert!(!tracker.is_namespace_match("sdp.read.parquet('x')", false));
        assert!(!tracker.is_namespace_match("pandas.read.csv('x')", false));
    }

    #[test]
    fn test_from_import() {
        let mut tracker = NamespaceTracker::new();
        tracker
            .from_imports
            .insert("table".to_string(), "sdp".to_string());
        assert_eq!(tracker.resolve("table"), Some("sdp"));
        assert!(tracker.is_dlt_namespace("table"));
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
}
