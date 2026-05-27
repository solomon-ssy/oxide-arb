//! Property tests: Funnel submit invariants under random load.

use std::sync::Arc;
use std::time::Duration;

use oxide_arb_core::detection::funnel::Funnel;
use oxide_arb_core::observability::metrics_hub::MetricsHub;
use proptest::prelude::*;
use rust_decimal::Decimal;

#[path = "support/test_util/scored_opportunity.rs"]
mod scored_opportunity;

use scored_opportunity::sample_scored;

fn scored_with_score(score: f64) -> Arc<oxide_arb_algorithm::scorer::ScoredOpportunity> {
    use oxide_arb_models::types::MicroScore;

    let mut base = sample_scored();
    Arc::make_mut(&mut base).score =
        MicroScore::try_from_decimal(Decimal::try_from(score).unwrap()).unwrap();
    base
}

proptest! {
    #[test]
    fn submit_accounting_matches_attempts(
        scores in prop::collection::vec(0.01f64..1.0, 1..100),
    ) {
        let metrics = Arc::new(MetricsHub::new());
        let (tx, _rx) = flume::bounded(64);
        let funnel = Funnel::new(
            vec![tx],
            32,
            Duration::from_millis(75),
            Arc::clone(&metrics),
        );

        for score in &scores {
            funnel.submit(scored_with_score(*score));
        }

        let accounted = metrics.funnel_enqueued.get() + metrics.funnel_dropped.get();
        prop_assert_eq!(accounted, scores.len() as u64);
    }
}
