use crate::{
    bridge::CoreOpportunityPipeline,
    observability::{
        detection_writer::DetectionWriter, latency::observe_ws_to_scan, metrics_hub::MetricsHub,
    },
    pipeline::{
        book_gate::BookGate,
        book_store::BookStore,
        dual_book_assembler::DualBookAssembler,
        market_cache::{CachedMarketScanEntry, MarketCache},
        staleness_classifier::StalenessClassifier,
    },
    service::{catalog_readiness::CatalogReadiness, detection_readiness::DetectionReadiness},
};
use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use oxide_arb_algorithm::{pipeline::MarketScanInputRef, scorer::ScoredOpportunity};
use oxide_arb_models::domain::{
    CatalogStatusPort, CoreEvent, CoreEventPublisher, latency::LatencyTrace,
};
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
    detection_writer: Option<Arc<DetectionWriter>>,
    /// Non-blocking real-time bus handle; emits `OpportunityDetected` on detect.
    events: CoreEventPublisher,
    /// Catalog warmup gate: no opportunities are produced while `Warming`
    /// (fail-closed; lock-free atomic read on the hot path).
    catalog: Arc<CatalogReadiness>,
    /// Lifecycle detection gate mirrored from operational phase on status publish.
    detection_readiness: Arc<DetectionReadiness>,
}

/// Construction dependencies for [`Scanner`].
pub struct ScannerDeps {
    pub pipeline: Arc<CoreOpportunityPipeline>,
    pub book_store: Arc<BookStore>,
    pub market_cache: Arc<MarketCache>,
    pub staleness_classifier: StalenessClassifier,
    pub metrics: Arc<MetricsHub>,
    pub detection_writer: Option<Arc<DetectionWriter>>,
    pub events: CoreEventPublisher,
    pub catalog: Arc<CatalogReadiness>,
    pub detection_readiness: Arc<DetectionReadiness>,
}

impl Scanner {
    pub fn new(deps: ScannerDeps) -> Self {
        Self {
            pipeline: deps.pipeline,
            book_store: deps.book_store,
            market_cache: deps.market_cache,
            staleness_classifier: deps.staleness_classifier,
            metrics: deps.metrics,
            detection_writer: deps.detection_writer,
            events: deps.events,
            catalog: deps.catalog,
            detection_readiness: deps.detection_readiness,
        }
    }

    #[inline]
    pub fn scan_market(
        &self,
        entry: &CachedMarketScanEntry,
        now: DateTime<Utc>,
    ) -> Option<Arc<ScoredOpportunity>> {
        if !self.catalog.is_ready() || !self.detection_readiness.allows_detection() {
            return None;
        }
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
            category: entry.fee_category,
            staleness,
            settlement_deadline: entry.settlement_deadline,
            latency,
        };
        let result = self.pipeline.process_ref(&input, now);
        if let Some(ref scored) = result {
            observe_ws_to_scan(&scored.trace, &self.metrics);
            self.metrics.opportunities_detected.inc();
            if let Some(writer) = &self.detection_writer {
                writer.write(scored);
            }
            // Surface the detection to the real-time bus. Project the public
            // `Opportunity` (no internal algorithm/latency trace leakage);
            // fire-and-forget, drops on a full bus rather than blocking the scan.
            self.events.publish(CoreEvent::OpportunityDetected(
                (*scored.opportunity).clone(),
            ));
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
