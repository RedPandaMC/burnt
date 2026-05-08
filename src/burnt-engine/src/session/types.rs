//! Spark monitoring REST API types.
//!
//! Serde models for `/stages`, `/jobs`, `/sql`, and `/executors` endpoints.
//! Fields are deserialized from camelCase JSON and normalised to snake_case Rust.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Stages
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StageMetrics {
    pub stage_id: i64,
    pub name: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub num_active_tasks: i32,
    #[serde(default)]
    pub num_complete_tasks: i32,
    #[serde(default)]
    pub num_failed_tasks: i32,
    #[serde(default)]
    pub executor_run_time: i64,
    #[serde(default)]
    pub executor_cpu_time: i64,
    #[serde(default)]
    pub input_bytes: i64,
    #[serde(default)]
    pub output_bytes: i64,
    #[serde(default)]
    pub shuffle_read_bytes: i64,
    #[serde(default)]
    pub shuffle_write_bytes: i64,
    #[serde(default)]
    pub memory_bytes_spilled: i64,
    #[serde(default)]
    pub disk_bytes_spilled: i64,
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobSummary {
    pub job_id: i64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub num_stages: i32,
    #[serde(default)]
    pub num_active_stages: i32,
    #[serde(default)]
    pub num_completed_stages: i32,
    #[serde(default)]
    pub num_skipped_stages: i32,
    #[serde(default)]
    pub num_failed_stages: i32,
    #[serde(default)]
    pub num_tasks: i32,
    #[serde(default)]
    pub num_active_tasks: i32,
    #[serde(default)]
    pub num_completed_tasks: i32,
    #[serde(default)]
    pub num_skipped_tasks: i32,
    #[serde(default)]
    pub num_failed_tasks: i32,
}

// ---------------------------------------------------------------------------
// SQL executions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlExecution {
    pub id: i64,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub submission_time: Option<i64>,
    #[serde(default)]
    pub duration: Option<i64>,
    #[serde(default)]
    pub running_jobs: Vec<i64>,
    #[serde(default)]
    pub success_jobs: Vec<i64>,
    #[serde(default)]
    pub failed_jobs: Vec<i64>,
}

// ---------------------------------------------------------------------------
// Executors
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorSummary {
    pub id: String,
    #[serde(default)]
    pub host_port: String,
    #[serde(default)]
    pub is_active: bool,
    #[serde(default)]
    pub rdd_blocks: i32,
    #[serde(default)]
    pub memory_used: i64,
    #[serde(default)]
    pub disk_used: i64,
    #[serde(default)]
    pub total_cores: i32,
    #[serde(default)]
    pub max_tasks: i32,
    #[serde(default)]
    pub active_tasks: i32,
    #[serde(default)]
    pub failed_tasks: i32,
    #[serde(default)]
    pub completed_tasks: i32,
    #[serde(default)]
    pub total_tasks: i32,
    #[serde(default)]
    pub total_duration: i64,
    #[serde(default)]
    pub total_gc_time: i64,
    #[serde(default)]
    pub total_input_bytes: i64,
    #[serde(default)]
    pub total_shuffle_read: i64,
    #[serde(default)]
    pub total_shuffle_write: i64,
    #[serde(default)]
    pub max_memory: i64,
}
