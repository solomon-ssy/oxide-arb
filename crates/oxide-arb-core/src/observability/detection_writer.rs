//! Fire-and-forget CH writer for scanner detection events.

use crate::{
    infra::async_writer::AsyncWriter,
    observability::{
        book_decision_context_capture::BookDecisionContextCapture,
        book_decision_context_writer::BookDecisionContextWriter,
    },
};
use chrono::Utc;
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_models::{
    clickhouse::OpportunityDetectionRow,
    domain::{ScoredOpportunitySnapshot, book::EndgameBookPair},
};
use std::sync::Arc;

/// Non-blocking writer for `ClickHouse` `opportunity_detection` rows.
///
/// Records every opportunity the scanner produces for funnel analytics.
pub struct DetectionWriter {
    writer: Arc<AsyncWriter<OpportunityDetectionRow>>,
    decision_context_writer: Arc<BookDecisionContextWriter>,
}

impl DetectionWriter {
    pub const fn new(
        writer: Arc<AsyncWriter<OpportunityDetectionRow>>,
        decision_context_writer: Arc<BookDecisionContextWriter>,
    ) -> Self {
        Self {
            writer,
            decision_context_writer,
        }
    }

    pub fn write(
        &self,
        scored: &ScoredOpportunity,
        pair: &EndgameBookPair,
        fresh_book_max_age_ms: u64,
    ) {
        let opp = scored.opportunity.as_ref();
        let publication_id = scored
            .applied_factors
            .first()
            .map(|factor| factor.publication_id.clone());
        let now_ms = Utc::now().timestamp_millis();
        let capture = BookDecisionContextCapture::default();
        let context_row = capture.capture_detection(scored, pair, fresh_book_max_age_ms);
        let context_id = context_row.context_id.clone();
        let book_age_ms = pair.max_staleness_ms(u64::try_from(now_ms.max(0)).unwrap_or(u64::MAX));
        let snapshot = ScoredOpportunitySnapshot::from_opportunity(opp)
            .with_score_components(
                scored.fill_probability,
                scored.score,
                scored.urgency_factor,
                scored.category_weight,
                scored.staleness_discount,
            )
            .with_book_context(
                scored.token_yes.clone(),
                scored.token_no.clone(),
                scored.book_yes_version,
                scored.book_no_version,
                Some(book_age_ms),
                Some(context_id),
            )
            .with_applied_control_factors(publication_id, &scored.applied_factors);
        let mut row = OpportunityDetectionRow::from(&snapshot);
        row.ingestion_time = now_ms;
        row.sequence = scored.book_yes_version.max(scored.book_no_version);
        if !self.writer.write(row) {
            tracing::warn!(
                opportunity_id = %opp.opportunity_id,
                "opportunity detection row dropped by async writer"
            );
        }
        if !self.decision_context_writer.write(context_row) {
            tracing::warn!(
                opportunity_id = %opp.opportunity_id,
                "detection book decision context row dropped by async writer"
            );
        }
    }
}
