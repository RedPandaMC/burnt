use crate::parse::namespace::{build_namespace_tracker, NamespaceTracker};
use crate::types::AnalysisMode;
use tree_sitter::Parser;

pub fn detect_mode_from_source(source: &str) -> AnalysisMode {
    let trimmed = source.trim();
    let lower = trimmed.to_lowercase();

    if lower.starts_with("select")
        || lower.starts_with("with")
        || lower.starts_with("insert")
        || lower.starts_with("update")
        || lower.starts_with("delete")
        || lower.starts_with("merge")
        || lower.starts_with("create")
    {
        if lower.starts_with("create streaming table")
            || lower.starts_with("create materialized view")
        {
            return AnalysisMode::Sdp;
        }
        return AnalysisMode::Sql;
    }

    if lower.contains("create streaming table")
        || lower.contains("create materialized view")
        || lower.contains("live.ref")
    {
        return AnalysisMode::Sdp;
    }

    if source.is_empty() {
        return AnalysisMode::Python;
    }

    let mut parser = Parser::new();
    if parser
        .set_language(&tree_sitter_python::LANGUAGE.into())
        .is_err()
    {
        return AnalysisMode::Python;
    }
    let tree = match parser.parse(source, None) {
        Some(t) => t,
        None => return AnalysisMode::Python,
    };

    let ns = build_namespace_tracker(source, tree.root_node());

    let mut stack: Vec<tree_sitter::Node> = vec![tree.root_node()];
    while let Some(node) = stack.pop() {
        if node.kind() == "decorator" {
            let text = node.utf8_text(source.as_bytes()).unwrap_or("");
            if is_dlt_decorator(text, &ns) {
                return AnalysisMode::Sdp;
            }
        }
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            stack.push(child);
        }
    }

    AnalysisMode::Python
}

fn is_dlt_decorator(text: &str, ns: &NamespaceTracker) -> bool {
    let text = text.trim();
    let at_pos = text.find('@').unwrap_or(usize::MAX);
    let after_at = text[at_pos + 1..].trim();

    if let Some(dot_pos) = after_at.find('.') {
        let ns_part = &after_at[..dot_pos];
        if ns.is_dlt_namespace(ns_part) {
            return true;
        }
    }

    if after_at.starts_with("@") {
        let bare = &after_at[1..];
        let name_end = bare
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .unwrap_or(bare.len());
        let name = &bare[..name_end];
        if ns.is_dlt_namespace(name) {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_sdp_from_import() {
        let source = "import sdp\n@sdp.table\ndef my_table(): pass";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Sdp);
    }

    #[test]
    fn test_detect_dlt_as_alias() {
        let source = "import dlt as dl\n@dl.table\ndef my_table(): pass";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Sdp);
    }

    #[test]
    fn test_detect_dp_as_alias() {
        let source = "import dp as d\n@d.table\ndef my_table(): pass";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Sdp);
    }

    #[test]
    fn test_detect_from_dlt_alias() {
        let source = "from dlt import table as t\n@t.table\ndef my_table(): pass";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Sdp);
    }

    #[test]
    fn test_detect_from_sdp_alias() {
        let source = "from sdp import table as sp\n@sp.table\ndef my_table(): pass";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Sdp);
    }

    #[test]
    fn test_detect_sql_mode() {
        let source = "SELECT 1";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Sql);
    }

    #[test]
    fn test_detect_python_mode() {
        let source = "import pandas as pd\ndf = pd.read_csv('data.csv')";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Python);
    }

    #[test]
    fn test_detect_dlt_create_streaming() {
        let source = "CREATE STREAMING TABLE my_table AS SELECT * FROM source";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Sdp);
    }

    #[test]
    fn test_detect_dlt_create_materialized() {
        let source = "CREATE MATERIALIZED VIEW my_view AS SELECT * FROM source";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Sdp);
    }

    #[test]
    fn test_detect_mixed_python_with_sql_falls_to_python() {
        let source = "import pandas as pd\nspark.sql('SELECT 1')";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Python);
    }

    #[test]
    fn test_detect_empty_source() {
        assert_eq!(detect_mode_from_source(""), AnalysisMode::Python);
        assert_eq!(detect_mode_from_source("   "), AnalysisMode::Python);
    }

    #[test]
    fn test_detect_sdp_with_live_ref() {
        let source = "LIVE.ref('other_table')";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Sdp);
    }

    #[test]
    fn test_detect_unrelated_import_no_decorator() {
        let source = "import dlt as dl\nprint('hello')";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Python);
    }

    #[test]
    fn test_detect_dp_table_decorator() {
        let source = "import dp\n@dp.table\ndef t(): pass";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Sdp);
    }

    #[test]
    fn test_detect_from_dlt_no_alias() {
        let source = "from dlt import table\n@table.table\ndef my_table(): pass";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Sdp);
    }
}
