//! Throttled, coalesced per-market order-book pushes for WebSocket dashboards.
//!
//! This task runs **off** the detection/execution hot path. Each tick it asks
//! the [`SessionRegistry`] which markets a dashboard is currently watching,
//! diffs the published [`BookStore`] versions for those markets' YES/NO tokens,
//! and emits a latest-wins [`CoreEvent::MarketBookUpdate`] only for markets whose
//! book changed since the previous tick.
//!
//! It never blocks producers and never touches the hot path: change detection is
//! a lock-free `book_version` read, and emission goes through the bounded,
//! drop-on-full [`CoreEventPublisher`] (6.6 §2.5: backpressure drops, never
//! halts trading).

use std::{collections::HashMap, sync::Arc, time::Duration};

use oxide_arb_models::{
    domain::{CoreEvent, CoreEventPublisher, MarketBookSideView, MarketBookView},
    types::MarketId,
};
use oxide_arb_web::ws::SessionRegistry;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::pipeline::{book_store::BookStore, market_registry::MarketRegistry};

/// Default coalescing interval — the latest-wins throttle window.
pub const DEFAULT_COALESCE_INTERVAL: Duration = Duration::from_millis(200);

/// Periodic task that fans coalesced order-book snapshots to watching sessions.
pub struct BookUpdateCoalescer {
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    sessions: SessionRegistry,
    events: CoreEventPublisher,
    interval: Duration,
    /// Last `(yes_version, no_version)` emitted per market — drives change
    /// detection so an unchanged book is never re-sent.
    last_versions: HashMap<MarketId, (u64, u64)>,
}

impl BookUpdateCoalescer {
    /// Build a coalescer with the [`DEFAULT_COALESCE_INTERVAL`].
    #[must_use]
    pub fn new(
        book_store: Arc<BookStore>,
        market_registry: Arc<MarketRegistry>,
        sessions: SessionRegistry,
        events: CoreEventPublisher,
    ) -> Self {
        Self {
            book_store,
            market_registry,
            sessions,
            events,
            interval: DEFAULT_COALESCE_INTERVAL,
            last_versions: HashMap::new(),
        }
    }

    /// Override the coalescing interval (primarily for tests).
    #[must_use]
    pub const fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Run the coalescing loop until `shutdown` is cancelled.
    pub async fn run(mut self, shutdown: CancellationToken) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    tracing::info!("book update coalescer shutting down");
                    return;
                }
                _ = ticker.tick() => {
                    self.tick();
                }
            }
        }
    }

    /// One coalescing pass: emit `MarketBookUpdate` for every watched market
    /// whose YES/NO book version advanced since the last pass.
    pub fn tick(&mut self) {
        let subscribed = self.sessions.subscribed_markets();
        // Forget markets nobody watches anymore so the map cannot grow unbounded.
        self.last_versions
            .retain(|market_id, _| subscribed.contains(market_id));

        for market_id in subscribed {
            let Some((token_yes, token_no)) = self.market_registry.token_pair(&market_id) else {
                continue;
            };
            let yes_version = self.book_store.book_version(&token_yes);
            let no_version = self.book_store.book_version(&token_no);
            // No book published yet for either leg — nothing to send.
            if yes_version == 0 && no_version == 0 {
                continue;
            }
            let changed = self
                .last_versions
                .get(&market_id)
                .is_none_or(|&(yes, no)| yes != yes_version || no != no_version);
            if !changed {
                continue;
            }

            let yes = self
                .book_store
                .load(&token_yes)
                .map(|snapshot| MarketBookSideView::from_snapshot(token_yes.clone(), &snapshot));
            let no = self
                .book_store
                .load(&token_no)
                .map(|snapshot| MarketBookSideView::from_snapshot(token_no.clone(), &snapshot));
            let view = MarketBookView {
                market_id: market_id.clone(),
                yes,
                no,
            };
            self.events.publish(CoreEvent::MarketBookUpdate {
                market_id: market_id.clone(),
                view: Box::new(view),
            });
            self.last_versions
                .insert(market_id, (yes_version, no_version));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BookStore, BookUpdateCoalescer};
    use crate::{
        observability::metrics_hub::MetricsHub, pipeline::market_registry::MarketRegistry,
    };
    use oxide_arb_models::{
        domain::{CoreEvent, CoreEventPublisher, SubscriptionKey, WsChannel},
        types::MarketId,
    };
    use oxide_arb_web::ws::{SessionHandle, SessionRegistry};
    use std::{
        collections::HashSet,
        sync::{Arc, RwLock},
    };

    fn coalescer(sessions: SessionRegistry) -> (BookUpdateCoalescer, flume::Receiver<CoreEvent>) {
        let book_store = Arc::new(BookStore::new(Arc::new(MetricsHub::new())));
        let market_registry = Arc::new(MarketRegistry::new());
        let (events, rx) = CoreEventPublisher::bounded(16);
        (
            BookUpdateCoalescer::new(book_store, market_registry, sessions, events),
            rx,
        )
    }

    #[test]
    fn tick_emits_nothing_without_subscribers() {
        let (mut coalescer, rx) = coalescer(SessionRegistry::default());
        coalescer.tick();
        assert!(
            rx.try_recv().is_err(),
            "no watching session -> no book event reaches the bus"
        );
    }

    #[test]
    fn tick_skips_markets_absent_from_the_registry() {
        let sessions = SessionRegistry::default();
        let (outbound, _rx) = flume::bounded::<String>(8);
        let subscriptions: HashSet<SubscriptionKey> = std::iter::once(SubscriptionKey::scoped(
            WsChannel::MarketBookUpdate,
            MarketId::new("0xunknown"),
        ))
        .collect();
        sessions.register(SessionHandle {
            outbound,
            subscriptions: Arc::new(RwLock::new(subscriptions)),
        });

        let (mut coalescer, rx) = coalescer(sessions);
        coalescer.tick();
        assert!(
            rx.try_recv().is_err(),
            "unknown market resolves to no token pair -> skipped"
        );
    }
}
