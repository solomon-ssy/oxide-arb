use std::sync::Arc;

use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use oxide_arb_algorithm::{pipeline::MarketScanInput, scorer::ScoredOpportunity};

use crate::bridge::CoreOpportunityPipeline;
use crate::observability::metrics_hub::MetricsHub;
use crate::pipeline::book_gate::BookGate;
use crate::pipeline::book_store::BookStore;
use crate::pipeline::dual_book_assembler::DualBookAssembler;
use crate::pipeline::market_cache::{CachedMarketScanEntry, MarketCache};
use crate::pipeline::staleness_classifier::StalenessClassifier;

pub struct Scanner {
    pipeline: Arc<CoreOpportunityPipeline>,
    book_store: Arc<BookStore>,
    market_cache: Arc<MarketCache>,
    staleness_classifier: StalenessClassifier,
    metrics: Arc<MetricsHub>,
}

impl Scanner {
    pub const fn new(
        pipeline: Arc<CoreOpportunityPipeline>,
        book_store: Arc<BookStore>,
        market_cache: Arc<MarketCache>,
        staleness_classifier: StalenessClassifier,
        metrics: Arc<MetricsHub>,
    ) -> Self {
        Self {
            pipeline,
            book_store,
            market_cache,
            staleness_classifier,
            metrics,
        }
    }

    pub fn scan_market(
        &self,
        entry: &CachedMarketScanEntry,
        now: DateTime<Utc>,
    ) -> Option<ScoredOpportunity> {
        let _timer = self.metrics.scan_duration_seconds.start_timer();
        let pair =
            DualBookAssembler::assemble(&self.book_store, &entry.token_yes, &entry.token_no)?;

        let now_ms = ToPrimitive::to_u64(&now.timestamp_millis().max(0)).unwrap_or(0);
        if !BookGate::pass(
            &pair,
            now_ms,
            self.staleness_classifier.expired_ms(),
            &entry.token_yes,
            &entry.token_no,
        ) {
            self.metrics.scans_gate_rejected.inc();
            return None;
        }

        let staleness = self
            .staleness_classifier
            .classify(pair.max_staleness_ms(now_ms));

        let input = MarketScanInput {
            market_id: entry.market_id.clone(),
            event_id: entry.event_id.clone(),
            token_yes: entry.token_yes.clone(),
            token_no: entry.token_no.clone(),
            book: pair,
            category: entry.category,
            staleness,
            settlement_deadline: entry.settlement_deadline,
        };
        self.pipeline.process(&input, now)
    }

    pub fn scan_all(&self, now: DateTime<Utc>) -> Vec<ScoredOpportunity> {
        let entries = self.market_cache.entries();
        let mut results: Vec<ScoredOpportunity> = entries
            .iter()
            .filter_map(|entry| self.scan_market(entry, now))
            .collect();
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        self.metrics.scan_results_total.observe(f64::from(
            ToPrimitive::to_u32(&results.len()).unwrap_or(u32::MAX),
        ));
        results
    }
}
