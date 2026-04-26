use crate::types::AnalysisMode;

pub fn detect_mode_from_source(source: &str) -> AnalysisMode {
    let source_lower = source.to_lowercase();

    if source_lower.contains("import sdp")
        || source_lower.contains("from sdp import")
        || source_lower.contains("@sdp.table")
        || source_lower.contains("@dp.table")
        || source_lower.contains("@dp.materialized_view")
        || source_lower.contains("create streaming table")
        || source_lower.contains("create materialized view")
        || source_lower.contains("live.ref")
    {
        return AnalysisMode::Sdp;
    }

    let trimmed = source.trim();
    if trimmed.to_uppercase().starts_with("SELECT")
        || trimmed.to_uppercase().starts_with("WITH")
        || trimmed.to_uppercase().starts_with("INSERT")
        || trimmed.to_uppercase().starts_with("UPDATE")
        || trimmed.to_uppercase().starts_with("DELETE")
        || trimmed.to_uppercase().starts_with("MERGE")
        || trimmed.to_uppercase().starts_with("CREATE")
    {
        AnalysisMode::Sql
    } else {
        AnalysisMode::Python
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_dlt_from_import() {
        let source = "import dlt\n@dlt.table\ndef my_table(): pass";
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
}
