use std::collections::HashMap;

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

    pub fn is_dlt_namespace(&self, alias: &str) -> bool {
        self.resolve(alias)
            .map(|ns| ns == "sdp" || ns == "dlt" || ns == "dp")
            .unwrap_or(false)
    }

    pub fn dlt_namespace(&self) -> Option<&str> {
        for (alias, _) in self.aliases.iter().chain(self.simple_imports.iter()) {
            if self.is_dlt_namespace(alias) {
                return self.resolve(alias);
            }
        }
        for (alias, _) in self.from_imports.iter() {
            if self.is_dlt_namespace(alias) {
                return self.resolve(alias);
            }
        }
        None
    }

    pub fn is_table_decorator(&self, decorator_name: &str) -> bool {
        self.resolve(decorator_name)
            .map(|ns| ns == "sdp" || ns == "dlt" || ns == "dp")
            .unwrap_or(false)
    }

    pub fn is_read_call(&self, namespace: &str) -> bool {
        self.resolve(namespace)
            .map(|ns| ns == "sdp" || ns == "dlt" || ns == "dp")
            .unwrap_or(false)
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
            results.push((format!("{}.{}", ns, actual), alias));
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
    fn test_alias_import() {
        let mut tracker = NamespaceTracker::new();
        tracker.aliases.insert("x".to_string(), "dlt".to_string());
        assert_eq!(tracker.resolve("x"), Some("dlt"));
        assert!(tracker.is_dlt_namespace("x"));
    }

    #[test]
    fn test_from_import() {
        let mut tracker = NamespaceTracker::new();
        tracker
            .from_imports
            .insert("table".to_string(), "sdp.table".to_string());
        assert_eq!(tracker.resolve("table"), Some("sdp.table"));
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
        assert_eq!(result, vec![("sdp.table".to_string(), "table".to_string())]);
    }

    #[test]
    fn test_parse_import_from_statement_alias() {
        let result = parse_import_from_statement("from dlt import table as t");
        assert_eq!(result, vec![("dlt.table".to_string(), "t".to_string())]);
    }
}
