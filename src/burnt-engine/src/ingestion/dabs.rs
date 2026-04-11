
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DabsConfig {
    pub clusters: Vec<ClusterDef>,
    pub jobs: Vec<JobDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterDef {
    pub name: String,
    pub node_type: Option<String>,
    pub num_workers: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDef {
    pub name: String,
    pub cluster: Option<String>,
}

pub fn parse_dabs(path: &str) -> Result<DabsConfig, String> {
    let _ = path;
    Err("Not implemented".to_string())
}
