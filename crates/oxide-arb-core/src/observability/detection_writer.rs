//! Fire-and-forget CH writer for scanner detection events.

use crate::infra::async_writer::AsyncWriter;
use oxide_arb_models::{clickhouse::OpportunityDetectionRow, domain::opportunity::Opportunity};
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

    pub fn write(&self, opp: &Opportunity) {
        let row = opp.into();
        self.writer.write(row);
    }
}
