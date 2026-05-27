use crate::{
    bridge::CoreOpportunityPipeline,
    observability::{latency::observe_ws_to_scan, metrics_hub::MetricsHub},
    pipeline::{
        book_gate::BookGate,
        book_store::BookStore,
        dual_book_assembler::DualBookAssembler,
        market_cache::{CachedMarketScanEntry, MarketCache},
        staleness_classifier::StalenessClassifier,
    },
};
use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use oxide_arb_algorithm::{pipeline::MarketScanInputRef, scorer::ScoredOpportunity};
use oxide_arb_models::domain::latency::LatencyTrace;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

static SCAN_SAMPLE: AtomicU64 = AtomicU64::new(0);

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

    #[inline]
    pub fn scan_market(
        &self,
        entry: &CachedMarketScanEntry,
        now: DateTime<Utc>,
    ) -> Option<Arc<ScoredOpportunity>> {
        let sample = SCAN_SAMPLE.fetch_add(1, Ordering::Relaxed).trailing_zeros() >= 6;
        let timer = sample.then(|| self.metrics.scan_duration_seconds.start_timer());

        let pair =
            DualBookAssembler::assemble(&self.book_store, &entry.token_yes, &entry.token_no)?;

        let now_ms = ToPrimitive::to_u64(&now.timestamp_millis().max(0)).unwrap_or(0);
        if !BookGate::pass(
            &pair,
            now_ms,
            self.staleness_classifier.acceptable_ms(),
            &entry.token_yes,
            &entry.token_no,
        ) {
            self.metrics.scans_gate_rejected.inc();
            return None;
        }

        let staleness = self
            .staleness_classifier
            .classify(pair.max_staleness_ms(now_ms));

        let mut latency = LatencyTrace::merge_pair(
            self.book_store
                .token_latency_trace(&entry.token_yes)
                .as_deref(),
            self.book_store
                .token_latency_trace(&entry.token_no)
                .as_deref(),
        );
        latency.mark_scan_started();
        let latency = Arc::new(latency);

        let input = MarketScanInputRef {
            market_id: &entry.market_id,
            event_id: &entry.event_id,
            token_yes: &entry.token_yes,
            token_no: &entry.token_no,
            book: &pair,
            category: entry.category,
            staleness,
            settlement_deadline: entry.settlement_deadline,
            latency,
        };
        let result = self.pipeline.process_ref(&input, now);
        if let Some(ref scored) = result {
            observe_ws_to_scan(&scored.trace, &self.metrics);
        }
        drop(timer);
        result
    }

    pub fn scan_all(&self, now: DateTime<Utc>) -> Vec<Arc<ScoredOpportunity>> {
        let entries = self.market_cache.entries();
        let mut results: Vec<Arc<ScoredOpportunity>> = entries
            .iter()
            .filter_map(|entry| self.scan_market(entry, now))
            .collect();
        results.sort_by(|a, b| a.score.cmp_desc(b.score));
        self.metrics.scan_results_total.observe(f64::from(
            ToPrimitive::to_u32(&results.len()).unwrap_or(u32::MAX),
        ));
        results
    }
}
