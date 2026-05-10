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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_stage_metrics() {
        let json = r#"{"stageId": 1, "name": "hashAgg", "status": "COMPLETE", "numActiveTasks": 0, "numCompleteTasks": 8, "numFailedTasks": 0, "executorRunTime": 1234, "executorCpuTime": 1100, "inputBytes": 1048576, "outputBytes": 524288}"#;
        let stage: StageMetrics = serde_json::from_str(json).unwrap();
        assert_eq!(stage.stage_id, 1);
        assert_eq!(stage.name, "hashAgg");
        assert_eq!(stage.status, "COMPLETE");
        assert_eq!(stage.input_bytes, 1048576);
    }

    #[test]
    fn test_deserialize_stage_metrics_minimal() {
        let json = r#"{"stageId": 0, "name": "minimal"}"#;
        let stage: StageMetrics = serde_json::from_str(json).unwrap();
        assert_eq!(stage.stage_id, 0);
        assert_eq!(stage.status, "");
        assert_eq!(stage.num_active_tasks, 0);
        assert_eq!(stage.input_bytes, 0);
    }

    #[test]
    fn test_deserialize_job_summary() {
        let json = r#"{"jobId": 5, "name": "collect at <stdin>:1", "status": "SUCCEEDED", "numStages": 1, "numCompletedStages": 1, "numTasks": 2, "numCompletedTasks": 2}"#;
        let job: JobSummary = serde_json::from_str(json).unwrap();
        assert_eq!(job.job_id, 5);
        assert_eq!(job.status, "SUCCEEDED");
        assert_eq!(job.num_completed_stages, 1);
    }

    #[test]
    fn test_deserialize_sql_execution() {
        let json = r#"{"id": 42, "description": "SELECT * FROM t", "status": "COMPLETED", "submissionTime": 1700000000000, "duration": 5000, "runningJobs": [], "successJobs": [1, 2], "failedJobs": []}"#;
        let sql: SqlExecution = serde_json::from_str(json).unwrap();
        assert_eq!(sql.id, 42);
        assert_eq!(sql.duration, Some(5000));
        assert_eq!(sql.success_jobs, vec![1, 2]);
    }

    #[test]
    fn test_deserialize_executor_summary() {
        let json = r#"{"id": "driver", "hostPort": "10.0.0.1:4040", "isActive": true, "rddBlocks": 0, "memoryUsed": 268435456, "diskUsed": 0, "totalCores": 4, "maxTasks": 4, "activeTasks": 0, "failedTasks": 0, "completedTasks": 0, "totalTasks": 0, "totalDuration": 0, "totalGcTime": 0, "totalInputBytes": 0, "totalShuffleRead": 0, "totalShuffleWrite": 0, "maxMemory": 536870912}"#;
        let exec: ExecutorSummary = serde_json::from_str(json).unwrap();
        assert_eq!(exec.id, "driver");
        assert!(exec.is_active);
        assert_eq!(exec.total_cores, 4);
        assert_eq!(exec.memory_used, 268435456);
    }

    #[test]
    fn test_deserialize_executor_summary_inactive() {
        let json = r#"{"id": "1", "hostPort": "10.0.0.2:4041", "isActive": false}"#;
        let exec: ExecutorSummary = serde_json::from_str(json).unwrap();
        assert_eq!(exec.id, "1");
        assert!(!exec.is_active);
        assert_eq!(exec.total_cores, 0);
        assert_eq!(exec.memory_used, 0);
    }
}
