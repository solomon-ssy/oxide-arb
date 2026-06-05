//! Fire-and-forget CH writer for scanner detection events.

use crate::infra::async_writer::AsyncWriter;
use chrono::Utc;
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_models::{clickhouse::OpportunityDetectionRow, domain::ScoredOpportunitySnapshot};
use std::sync::Arc;

/// Non-blocking writer for `ClickHouse` `opportunity_detection` rows.
///
/// Records every opportunity the scanner produces for funnel analytics.
pub struct DetectionWriter {
    writer: Arc<AsyncWriter<OpportunityDetectionRow>>,
}

impl DetectionWriter {
    pub const fn new(writer: Arc<AsyncWriter<OpportunityDetectionRow>>) -> Self {
        Self { writer }
    }

    pub fn write(&self, scored: &ScoredOpportunity) {
        let opp = scored.opportunity.as_ref();
        let publication_id = scored
            .applied_factors
            .first()
            .map(|factor| factor.publication_id.clone());
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
            )
            .with_applied_control_factors(publication_id, &scored.applied_factors);
        let mut row = OpportunityDetectionRow::from(&snapshot);
        let now_ms = Utc::now().timestamp_millis();
        row.ingestion_time = now_ms;
        row.sequence = scored.book_yes_version.max(scored.book_no_version);
        self.writer.write(row);
    }
}
