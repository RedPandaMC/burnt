use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceFile {
    pub path: String,
    pub language: String,
    pub content: String,
}

pub fn ingest_file(path: &str) -> Result<SourceFile, String> {
    let _ = path;
    Err("Not implemented".to_string())
}