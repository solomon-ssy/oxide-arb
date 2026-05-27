//! Opportunity + scored-opportunity fixtures.

use std::sync::Arc;

use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_models::domain::latency::LatencyTrace;
use oxide_arb_models::types::{MicroProb, MicroScore, TokenId};
use rust_decimal_macros::dec;

#[path = "opportunity_fixture.rs"]
mod opportunity_fixture;

pub use opportunity_fixture::sample_opportunity;

#[must_use]
pub fn sample_scored() -> Arc<ScoredOpportunity> {
    let yes = TokenId::new("yes-token");
    let no = TokenId::new("no-token");

    Arc::new(ScoredOpportunity {
        opportunity: Arc::new(sample_opportunity()),
        score: MicroScore::try_from_decimal(dec!(0.8)).unwrap(),
        token_yes: yes,
        token_no: no,
        book_yes_version: 1,
        book_no_version: 1,
        fill_probability: MicroProb::try_from_decimal(dec!(0.99)).unwrap(),
        urgency_factor: MicroProb::ONE,
        category_weight: MicroProb::ONE,
        staleness_discount: MicroProb::ONE,
        trace: Arc::new(LatencyTrace::default()),
    })
}
