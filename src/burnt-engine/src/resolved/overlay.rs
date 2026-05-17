//! Per-static-node overlay payloads attached by the merge step.
//!
//! Each [`NodeOverlay`] describes what the runtime data tells us about a
//! single canonical static node — which Spark stages it produced, the
//! Catalyst plan subtree that ran, and a bitflag set summarising which
//! signals are present (provenance).
//!
//! All structs here are `#[non_exhaustive]` so adding fields in a later
//! commit (e.g. new stage metrics, plan annotations) does not break the
//! public surface.

use bitflags::bitflags;
use serde::{Deserialize, Serialize};

use crate::plan_parser::PlanNode;
use crate::resolved::ids::{PlanNodeId, SqlExecId, StageId};

/// Diagnostic bucket for signals that couldn't be attached to any static
/// node. Surfaces in the resolved graph so callers can render
/// "N stages unattached" warnings to users without losing the data.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Unmatched {
    pub stages: Vec<StageObservation>,
    pub plan_bundles: Vec<SqlExecId>,
}

bitflags! {
    /// Which signals contributed to a [`NodeOverlay`].
    ///
    /// `STATIC` is always set (the canonical node is the source of identity).
    /// `STAGE` is set when at least one observed Spark stage attached to the
    /// node; `PLAN` when a Catalyst plan subtree attached.
    #[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
    pub struct Provenance: u8 {
        const STATIC = 0b0000_0001;
        const PLAN   = 0b0000_0010;
        const STAGE  = 0b0000_0100;
    }
}

impl std::fmt::Display for Provenance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts: Vec<&str> = Vec::new();
        if self.contains(Provenance::STATIC) {
            parts.push("static");
        }
        if self.contains(Provenance::PLAN) {
            parts.push("plan");
        }
        if self.contains(Provenance::STAGE) {
            parts.push("stage");
        }
        if parts.is_empty() {
            write!(f, "<empty>")
        } else {
            write!(f, "{}", parts.join("+"))
        }
    }
}

/// Runtime observation for a single Spark stage attached to a static node.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct StageObservation {
    pub stage_id: StageId,
    pub input_bytes: Option<u64>,
    pub shuffle_read_bytes: Option<u64>,
    pub shuffle_write_bytes: Option<u64>,
    pub duration_ms: Option<u64>,
    pub num_tasks: Option<u32>,
    /// Source line recovered from the stage name (`<file>:<N>` pattern).
    pub source_line: Option<u32>,
}

impl StageObservation {
    #[must_use]
    pub fn new(stage_id: StageId) -> Self {
        Self {
            stage_id,
            input_bytes: None,
            shuffle_read_bytes: None,
            shuffle_write_bytes: None,
            duration_ms: None,
            num_tasks: None,
            source_line: None,
        }
    }
}

/// A Catalyst physical plan subtree attached to a static node — the SQL
/// execution that fired from that call site, with all operator nodes owned.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PlanSubtree {
    pub sql_exec_id: SqlExecId,
    pub root: PlanNodeId,
    pub nodes: Vec<PlanNode>,
}

impl PlanSubtree {
    #[must_use]
    pub fn new(sql_exec_id: SqlExecId, root: PlanNodeId, nodes: Vec<PlanNode>) -> Self {
        Self {
            sql_exec_id,
            root,
            nodes,
        }
    }
}

/// The set of overlays attached to a single canonical static node.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[non_exhaustive]
pub struct NodeOverlay {
    pub stages: Vec<StageObservation>,
    pub plan_subtree: Option<PlanSubtree>,
    pub provenance: Provenance,
}

impl Default for Provenance {
    fn default() -> Self {
        Provenance::STATIC
    }
}

impl NodeOverlay {
    #[must_use]
    pub fn new() -> Self {
        Self {
            stages: Vec::new(),
            plan_subtree: None,
            provenance: Provenance::STATIC,
        }
    }

    /// Sum of `input_bytes` across attached stages, ignoring `None`s. Returns
    /// `None` when no stage carries any input bytes.
    #[must_use]
    pub fn observed_input_bytes(&self) -> Option<u64> {
        let mut total: u64 = 0;
        let mut found = false;
        for s in &self.stages {
            if let Some(b) = s.input_bytes {
                total = total.saturating_add(b);
                found = true;
            }
        }
        found.then_some(total)
    }

    /// Sum of `shuffle_read_bytes` across attached stages.
    #[must_use]
    pub fn observed_shuffle_read_bytes(&self) -> Option<u64> {
        let mut total: u64 = 0;
        let mut found = false;
        for s in &self.stages {
            if let Some(b) = s.shuffle_read_bytes {
                total = total.saturating_add(b);
                found = true;
            }
        }
        found.then_some(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provenance_default_is_static_only() {
        let p: Provenance = Provenance::default();
        assert_eq!(p, Provenance::STATIC);
        assert!(!p.contains(Provenance::PLAN));
        assert!(!p.contains(Provenance::STAGE));
    }

    #[test]
    fn provenance_display_lists_set_bits() {
        let p = Provenance::STATIC | Provenance::STAGE;
        assert_eq!(format!("{p}"), "static+stage");
        let q = Provenance::STATIC | Provenance::PLAN | Provenance::STAGE;
        assert_eq!(format!("{q}"), "static+plan+stage");
    }

    #[test]
    fn observed_input_bytes_sums_present_stages_returns_none_when_all_empty() {
        let mut ov = NodeOverlay::new();
        ov.stages.push(StageObservation::new(StageId::new(1)));
        ov.stages.push(StageObservation::new(StageId::new(2)));
        assert_eq!(ov.observed_input_bytes(), None);

        ov.stages[0].input_bytes = Some(100);
        ov.stages[1].input_bytes = Some(50);
        assert_eq!(ov.observed_input_bytes(), Some(150));

        // Saturating add — pathological huge values don't panic.
        ov.stages[0].input_bytes = Some(u64::MAX);
        ov.stages[1].input_bytes = Some(1);
        assert_eq!(ov.observed_input_bytes(), Some(u64::MAX));
    }
}
