//! Errors returned by adapters that translate external payloads (REST JSON,
//! plan-parser output) into the `resolved` module's internal types.
//!
//! The merge builder itself is infallible — orphaned data lands in
//! `Unmatched`. These errors are raised only by `try_from` paths on the
//! adapter structs.

use crate::resolved::ids::SqlExecId;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ResolveError {
    #[error("malformed stage payload: {reason}")]
    MalformedStage { reason: String },

    #[error("malformed plan bundle for sqlExecId {sql_exec_id}: {reason}")]
    MalformedPlanBundle {
        sql_exec_id: SqlExecId,
        reason: String,
    },
}
