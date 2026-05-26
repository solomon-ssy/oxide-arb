//! Graceful backpressure — coalesce, dedup, evict, spill. Never halts trading.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use oxide_arb_models::domain::pipeline::PipelineEvent;
use oxide_arb_models::types::TokenId;
use parking_lot::Mutex;

use crate::execution::execution_pipeline::PostTradeJob;
use crate::observability::alert_dispatcher::{Alert, AlertDispatcher, AlertSeverity};
use crate::observability::metrics_hub::MetricsHub;
use crate::outbox::in_memory::SharedInMemoryEventStore;

const COALESCER_DEDUP_WINDOW: Duration = Duration::from_micros(500);
const SPILL_ALERT_INTERVAL: Duration = Duration::from_secs(60);

type BookCoalesceKey = (usize, TokenId);

/// Result of applying a backpressure policy at one site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureAction {
    /// Book event stored for latest-wins coalesce.
    Coalesced,
    /// Coalescer notify deduplicated within the window.
    Deduped,
    /// Event/job truly dropped (non-coalescable or below eviction threshold).
    Dropped,
    /// Post-trade job spilled to in-memory outbox.
    Spilled,
}

/// Central backpressure policy for all four hot-path sites.
pub struct BackpressurePolicy {
    metrics: Arc<MetricsHub>,
    alerts: Option<Arc<AlertDispatcher>>,
    post_trade_spill: SharedInMemoryEventStore,
    book_coalesce: DashMap<BookCoalesceKey, PipelineEvent>,
    coalescer_dedup: DashMap<TokenId, Instant>,
    last_spill_alert: Mutex<Option<Instant>>,
}

impl BackpressurePolicy {
    pub fn new(
        metrics: Arc<MetricsHub>,
        alerts: Option<Arc<AlertDispatcher>>,
        post_trade_spill: SharedInMemoryEventStore,
    ) -> Self {
        Self {
            metrics,
            alerts,
            post_trade_spill,
            book_coalesce: DashMap::new(),
            coalescer_dedup: DashMap::new(),
            last_spill_alert: Mutex::new(None),
        }
    }

    pub const fn post_trade_spill(&self) -> &SharedInMemoryEventStore {
        &self.post_trade_spill
    }

    /// Site 1 — `book_apply` channel full: latest-wins coalesce per (shard, token).
    pub fn on_book_channel_full(&self, shard: usize, event: PipelineEvent) -> BackpressureAction {
        let Some(token) = token_id_from_pipeline_event(&event) else {
            self.record_book_drop();
            return BackpressureAction::Dropped;
        };

        if !is_book_coalescable(&event) {
            self.record_book_drop();
            return BackpressureAction::Dropped;
        }

        self.book_coalesce.insert((shard, token), event);
        self.metrics.book_apply_coalesced_total.inc();
        self.record_event("book_apply", "coalesce");
        BackpressureAction::Coalesced
    }

    /// Drain coalesced book events for one shard after each worker event.
    pub fn drain_book_coalesce(&self, shard: usize, mut apply: impl FnMut(PipelineEvent)) {
        let keys: Vec<BookCoalesceKey> = self
            .book_coalesce
            .iter()
            .filter(|entry| entry.key().0 == shard)
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys {
            if let Some((_, event)) = self.book_coalesce.remove(&key) {
                apply(event);
            }
        }
    }

    /// Site 2 — coalescer notify succeeded: record dedup window anchor.
    pub fn on_coalescer_notify_success(&self, token: &TokenId) {
        self.coalescer_dedup.insert(token.clone(), Instant::now());
    }

    /// Site 2 — coalescer `token_tx` full: dedup within 500µs else count drop.
    pub fn on_coalescer_channel_full(&self, token: &TokenId) -> BackpressureAction {
        if let Some(last) = self.coalescer_dedup.get(token) {
            if last.elapsed() < COALESCER_DEDUP_WINDOW {
                self.record_event("coalescer", "dedup");
                return BackpressureAction::Deduped;
            }
        }

        self.record_coalescer_drop();
        BackpressureAction::Dropped
    }

    /// Site 3 — execution shard backpressure: record eviction metric.
    pub fn on_execution_shard_evict(&self) {
        self.metrics.execution_shard_evicted_total.inc();
        self.record_event("execution_shard", "evict");
    }

    /// Site 4 — `post_trade` channel full: spill to in-memory outbox, never halt.
    pub fn on_post_trade_channel_full(&self, job: PostTradeJob) -> BackpressureAction {
        match self
            .post_trade_spill
            .enqueue_sync_post_trade(job, &self.metrics)
        {
            Ok(()) => {
                self.metrics.post_trade_spilled_total.inc();
                self.record_event("post_trade", "spill");
                self.maybe_warn_post_trade_spill();
                BackpressureAction::Spilled
            }
            Err(error) => {
                tracing::error!(%error, "post-trade spill failed — job dropped");
                self.record_post_trade_drop();
                BackpressureAction::Dropped
            }
        }
    }

    #[cold]
    fn record_book_drop(&self) {
        self.metrics.book_apply_dropped.inc();
        self.record_event("book_apply", "drop");
    }

    #[cold]
    fn record_coalescer_drop(&self) {
        self.metrics.coalescer_dropped.inc();
        self.record_event("coalescer", "drop");
    }

    #[cold]
    fn record_post_trade_drop(&self) {
        self.metrics.post_trade_dropped.inc();
        self.record_event("post_trade", "drop");
    }

    fn record_event(&self, site: &'static str, action: &'static str) {
        self.metrics
            .backpressure_events
            .with_label_values(&[site, action])
            .inc();
    }

    fn maybe_warn_post_trade_spill(&self) {
        let mut last = self.last_spill_alert.lock();
        let now = Instant::now();
        if last
            .map(|t| now.duration_since(t) >= SPILL_ALERT_INTERVAL)
            .unwrap_or(true)
        {
            *last = Some(now);
            drop(last);
            tracing::warn!("post_trade queue full — job spilled to in-memory outbox");
            if let Some(alerts) = &self.alerts {
                let alerts = Arc::clone(alerts);
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    handle.spawn(async move {
                        alerts
                            .dispatch(Alert {
                                severity: AlertSeverity::Warning,
                                title: "post_trade_spilled".into(),
                                body: "Post-trade queue full; jobs spilled to in-memory outbox"
                                    .into(),
                                timestamp: chrono::Utc::now(),
                            })
                            .await;
                    });
                }
            }
        }
    }
}

fn token_id_from_pipeline_event(event: &PipelineEvent) -> Option<TokenId> {
    match event {
        PipelineEvent::BookSnapshot(cmd) => Some(cmd.asset_id.clone()),
        PipelineEvent::PriceDelta(cmd) => Some(cmd.asset_id.clone()),
        PipelineEvent::BestBidAsk { asset_id, .. }
        | PipelineEvent::TickSizeChange { asset_id, .. }
        | PipelineEvent::LastTradePrice { asset_id, .. } => Some(asset_id.clone()),
        _ => None,
    }
}

const fn is_book_coalescable(event: &PipelineEvent) -> bool {
    matches!(
        event,
        PipelineEvent::BookSnapshot(_) | PipelineEvent::PriceDelta(_)
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use oxide_arb_models::domain::pipeline::{
        IngressTrace, PipelineEvent, PriceDeltaCmd, PriceLevelDelta,
    };
    use oxide_arb_models::enums::execution::ExecutionOutcome;
    use oxide_arb_models::types::{MarketId, Price, Shares, TokenId, TradeId, Usd};
    use rust_decimal_macros::dec;

    use super::*;
    use crate::execution::fsm::ExecutionFSM;
    use crate::outbox::in_memory::InMemoryEventStore;

    #[test]
    fn book_coalesce_does_not_halt() {
        let metrics = Arc::new(MetricsHub::new());
        let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
        let bp = BackpressurePolicy::new(
            Arc::clone(&metrics),
            None,
            Arc::new(InMemoryEventStore::new()),
        );

        let event = PipelineEvent::PriceDelta(PriceDeltaCmd {
            asset_id: TokenId::new("t1"),
            changes: Arc::from([PriceLevelDelta {
                price: Price::new(dec!(0.5)),
                size: Shares::new(dec!(100)),
            }]),
            timestamp_ms: 1,
            trace: IngressTrace::new(Instant::now(), 1),
        });

        assert_eq!(
            bp.on_book_channel_full(0, event),
            BackpressureAction::Coalesced
        );
        assert!(!fsm.is_emergency());
        assert_eq!(metrics.book_apply_coalesced_total.get(), 1);
    }

    #[test]
    fn coalescer_dedup_within_window() {
        let metrics = Arc::new(MetricsHub::new());
        let bp = BackpressurePolicy::new(
            Arc::clone(&metrics),
            None,
            Arc::new(InMemoryEventStore::new()),
        );
        let token = TokenId::new("t1");

        bp.on_coalescer_notify_success(&token);
        assert_eq!(
            bp.on_coalescer_channel_full(&token),
            BackpressureAction::Deduped
        );
        assert_eq!(metrics.coalescer_dropped.get(), 0);
    }

    #[test]
    fn post_trade_spill_does_not_halt() {
        let metrics = Arc::new(MetricsHub::new());
        let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
        let spill = Arc::new(InMemoryEventStore::new());
        let bp = BackpressurePolicy::new(Arc::clone(&metrics), None, Arc::clone(&spill));

        let job = PostTradeJob {
            trade_id: TradeId::new("trade-1"),
            market_id: MarketId::new("m1"),
            token_id: TokenId::new("t1"),
            entry_price: Price::new(dec!(0.5)),
            net_profit: Usd::new(dec!(1)),
            outcome: ExecutionOutcome::Miss {
                reason: "test".into(),
                execution_mode: oxide_arb_models::enums::common::ExecutionMode::Paper,
            },
        };

        assert_eq!(
            bp.on_post_trade_channel_full(job),
            BackpressureAction::Spilled
        );
        assert!(!fsm.is_emergency());
        assert_eq!(metrics.post_trade_spilled_total.get(), 1);
        assert_eq!(spill.pending_post_trade_count(), 1);
    }
}
