//! Graceful backpressure for book ingest — coalesce and dedup under channel pressure.

use crate::observability::metrics_hub::MetricsHub;
use dashmap::DashMap;
use quant_pivot_models::{domain::pipeline::PipelineEvent, types::TokenId};
use std::sync::Arc;

type BookCoalesceKey = TokenId;

/// Result of applying a backpressure policy at one site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureAction {
    /// Book event stored for latest-wins coalesce.
    Coalesced,
    /// Event truly dropped (non-coalescable or below eviction threshold).
    Dropped,
}

/// Central backpressure policy for the book-apply hot-path coalesce site.
///
/// When a per-shard `book_apply` channel is full, the latest book event for a
/// token replaces any pending one (latest-wins), so the worker never blocks the
/// WS ingress and stale intermediate deltas are coalesced away.
pub struct BackpressurePolicy {
    metrics: Arc<MetricsHub>,
    book_coalesce: Vec<DashMap<BookCoalesceKey, PipelineEvent>>,
}

impl BackpressurePolicy {
    pub fn new(metrics: Arc<MetricsHub>, book_shard_count: usize) -> Self {
        let shard_count = book_shard_count.max(1);
        Self {
            metrics,
            book_coalesce: (0..shard_count).map(|_| DashMap::new()).collect(),
        }
    }

    /// `book_apply` channel full: latest-wins coalesce per (shard, token).
    pub fn on_book_channel_full(&self, shard: usize, event: PipelineEvent) -> BackpressureAction {
        let Some(token) = event.asset_id().cloned() else {
            self.record_book_drop();
            return BackpressureAction::Dropped;
        };

        if !event.is_book_coalescable() {
            self.record_book_drop();
            return BackpressureAction::Dropped;
        }

        self.book_coalesce[shard].insert(token, event);
        self.metrics.book_apply_coalesced_total.inc();
        self.record_event("book_apply", "coalesce");
        BackpressureAction::Coalesced
    }

    /// Drain coalesced book events for one shard after each worker event.
    pub fn drain_book_coalesce(&self, shard: usize, mut apply: impl FnMut(PipelineEvent)) {
        let keys: Vec<BookCoalesceKey> = self.book_coalesce[shard]
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys {
            if let Some((_, event)) = self.book_coalesce[shard].remove(&key) {
                apply(event);
            }
        }
    }

    #[cold]
    fn record_book_drop(&self) {
        self.metrics.book_apply_dropped.inc();
        self.record_event("book_apply", "drop");
    }

    fn record_event(&self, site: &'static str, action: &'static str) {
        self.metrics
            .backpressure_events
            .with_label_values(&[site, action])
            .inc();
    }
}

#[cfg(test)]
mod tests {
    use super::{BackpressureAction, BackpressurePolicy};
    use crate::observability::metrics_hub::MetricsHub;
    use quant_pivot_models::{
        domain::pipeline::{IngressTrace, PipelineEvent, PriceDeltaCmd, PriceLevelDelta},
        enums::common::Side,
        types::{Price, Shares, TokenId},
    };
    use rust_decimal_macros::dec;
    use std::{sync::Arc, time::Instant};

    #[test]
    fn book_coalesce_does_not_halt() {
        let metrics = Arc::new(MetricsHub::new());
        let bp = BackpressurePolicy::new(Arc::clone(&metrics), 1);

        let event = PipelineEvent::PriceDelta(PriceDeltaCmd {
            asset_id: TokenId::new("t1"),
            changes: Arc::from([PriceLevelDelta {
                price: Price::new(dec!(0.5)),
                size: Shares::new(dec!(100)),
                side: Side::Buy,
            }]),
            timestamp_ms: 1,
            trace: IngressTrace::new(Instant::now(), 1),
        });

        assert_eq!(
            bp.on_book_channel_full(0, event),
            BackpressureAction::Coalesced
        );
        assert_eq!(metrics.book_apply_coalesced_total.get(), 1);
    }
}
