//! Fire-and-forget CH audit writer for execution outcomes and rejections.

use crate::infra::async_writer::AsyncWriter;
use oxide_arb_models::{
    clickhouse::OpportunityAuditRow,
    domain::{
        execution::{PostTradeJob, ResolvedOutcome},
        opportunity::Opportunity,
        scored_snapshot::ScoredOpportunitySnapshot,
    },
    types::ExecutionId,
};
use std::sync::Arc;

/// Non-blocking writer for `ClickHouse` `opportunity_audit` rows.
///
/// Handles both terminal outcomes (filled/miss/failed) and pre-dispatch
/// rejections. Backed by [`AsyncWriter`] with batched CH inserts.
pub struct ExecutionAuditWriter {
    writer: Arc<AsyncWriter<OpportunityAuditRow>>,
}

impl ExecutionAuditWriter {
    pub const fn new(writer: Arc<AsyncWriter<OpportunityAuditRow>>) -> Self {
        Self { writer }
    }

    pub fn write_rejection(
        &self,
        execution_id: &ExecutionId,
        opp: &Opportunity,
        stage: &str,
        reason: &str,
        snapshot: &ScoredOpportunitySnapshot,
    ) {
        let row = (execution_id, opp, stage, reason, snapshot).into();
        self.writer.write(row);
    }

    pub fn write_terminal(&self, job: &PostTradeJob, resolved: &ResolvedOutcome) {
        let row = (job, resolved).into();
        self.writer.write(row);
    }
}
