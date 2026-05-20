use crate::types::AnalysisMode;

pub fn detect_mode_from_source(source: &str) -> AnalysisMode {
    let lower = source.trim().to_lowercase();

    if lower.starts_with("select")
        || lower.starts_with("with")
        || lower.starts_with("insert")
        || lower.starts_with("update")
        || lower.starts_with("delete")
        || lower.starts_with("merge")
        || lower.starts_with("create")
    {
        return AnalysisMode::Sql;
    }

    AnalysisMode::Python
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_pipeline_from_import_is_python() {
        // Pipeline files (dlt, sdp, dp) are Python mode — namespace is
        // resolved at graph-build time via ImportMap, not by detect.
        let source = "import sdp\n@sdp.table\ndef my_table(): pass";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Python);
    }

    #[test]
    fn test_detect_dlt_decorator_is_python() {
        let source = "import dlt as dl\n@dl.table\ndef my_table(): pass";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Python);
    }

    #[test]
    fn test_detect_dp_decorator_is_python() {
        let source = "import dp as d\n@d.table\ndef my_table(): pass";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Python);
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
    fn test_detect_create_streaming_table_is_sql() {
        // CREATE STREAMING TABLE is SQL syntax — detected as Sql mode.
        let source = "CREATE STREAMING TABLE my_table AS SELECT * FROM source";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Sql);
    }

    #[test]
    fn test_detect_create_materialized_view_is_sql() {
        let source = "CREATE MATERIALIZED VIEW my_view AS SELECT * FROM source";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Sql);
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
    fn test_detect_live_ref_falls_to_python() {
        // LIVE.ref() is not a SQL keyword — falls to Python mode.
        let source = "LIVE.ref('other_table')";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Python);
    }

    #[test]
    fn test_detect_unrelated_import_no_decorator() {
        let source = "import dlt as dl\nprint('hello')";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Python);
    }

    #[test]
    fn test_detect_dp_table_decorator() {
        // Pipeline decorator files are Python — namespace tells rules it's a pipeline
        let source = "import dp\n@dp.table\ndef t(): pass";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Python);
    }

    #[test]
    fn test_detect_from_dlt_no_alias_is_python() {
        let source = "from dlt import table\n@table.table\ndef my_table(): pass";
        assert_eq!(detect_mode_from_source(source), AnalysisMode::Python);
    }
}
