//! Book-update coalescer — throttled `market.book_update` WS producer.
//!
//! Off the ingest hot path: the data pipeline keeps applying deltas into the
//! [`BookStore`] at full rate, while this task samples the *published*
//! snapshots on a fixed cadence and only for markets that a WebSocket session
//! is actively watching ([`SessionRegistry::subscribed_markets`]). Each tick
//! compares the per-token publish versions against the last emitted pair and
//! publishes a [`CoreEvent::MarketBookUpdate`] only when either side changed —
//! so an idle book costs nothing and a hot book is throttled to one frame per
//! tick regardless of delta rate.

use crate::{
    app::{AppContext, task_id::TaskId, task_registry::AppRunner},
    ingest::{book_store::BookStore, market_registry::MarketRegistry},
};
use quant_pivot_models::{
    domain::{
        CoreEvent, CoreEventPublisher,
        api::{MarketBookSideView, MarketBookView},
    },
    types::MarketId,
};
use quant_pivot_web::ws::SessionRegistry;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

/// Sampling cadence for watched-market book frames.
const TICK_INTERVAL: Duration = Duration::from_millis(200);

/// Last emitted `(yes_version, no_version)` pair per watched market.
type EmittedVersions = HashMap<MarketId, (u64, u64)>;

/// Samples watched markets from the [`BookStore`] and publishes throttled
/// [`CoreEvent::MarketBookUpdate`] frames for the WS fanout plane.
pub struct BookUpdateCoalescer {
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    sessions: SessionRegistry,
    events: CoreEventPublisher,
    emitted: EmittedVersions,
}

impl BookUpdateCoalescer {
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
            emitted: EmittedVersions::new(),
        }
    }

    /// Run one sampling pass; returns how many book frames were published.
    ///
    /// State for markets that lost their last subscriber is dropped so a
    /// re-subscribe always receives a fresh baseline frame on the next tick.
    pub fn tick(&mut self) -> usize {
        let watched = self.sessions.subscribed_markets();
        self.emitted
            .retain(|market_id, _| watched.contains(market_id));

        let mut published = 0_usize;
        for market_id in watched {
            let Some((yes_token, no_token)) = self.market_registry.token_pair(&market_id) else {
                // Unknown market (e.g. subscribed before catalog sync) — skip
                // silently; the registry will learn it on the next Gamma sync.
                continue;
            };

            let versions = (
                self.book_store.book_version(&yes_token),
                self.book_store.book_version(&no_token),
            );
            if versions == (0, 0) {
                // No published book on either side yet — nothing to emit.
                continue;
            }
            if self.emitted.get(&market_id) == Some(&versions) {
                continue;
            }

            let view = MarketBookView {
                market_id: market_id.clone(),
                yes: self
                    .book_store
                    .load(&yes_token)
                    .map(|snapshot| MarketBookSideView::from_snapshot(yes_token, &snapshot)),
                no: self
                    .book_store
                    .load(&no_token)
                    .map(|snapshot| MarketBookSideView::from_snapshot(no_token, &snapshot)),
            };
            self.events.publish(CoreEvent::MarketBookUpdate {
                market_id: market_id.clone(),
                view: Box::new(view),
            });
            self.emitted.insert(market_id, versions);
            published += 1;
        }
        published
    }

    /// Sampling loop: one [`Self::tick`] per interval until shutdown.
    pub async fn run(mut self, shutdown: CancellationToken) {
        let mut ticker = interval(TICK_INTERVAL);
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
}

impl AppContext {
    /// Spawn the coalescer over the web session registry (must run after
    /// [`AppContext::register_web_services`] created the shared registry).
    pub fn register_book_update_coalescer(
        &self,
        runner: &mut AppRunner,
        ws_sessions: SessionRegistry,
    ) {
        let coalescer = BookUpdateCoalescer::new(
            Arc::clone(&self.data.book_store),
            Arc::clone(&self.data.market_registry),
            ws_sessions,
            self.events.clone(),
        );
        runner.spawn(TaskId::BookUpdateCoalescer, move |token| {
            coalescer.run(token)
        });
    }
}

#[cfg(test)]
mod tests {
    use super::BookUpdateCoalescer;
    use crate::{
        ingest::{book_store::BookStore, market_registry::MarketRegistry},
        observability::metrics_hub::MetricsHub,
    };
    use chrono::Utc;
    use quant_pivot_models::{
        domain::{
            BookLevel, CoreEvent, CoreEventPublisher, SubscriptionKey, WsChannel,
            market::{MarketRegistryInfo, TokenInfo},
        },
        enums::{
            common::{CategorySet, MarketCategory, TickSize},
            market::MarketStatus,
        },
        types::{EventId, MarketId, Price, Shares, TokenId},
    };
    use quant_pivot_web::ws::{SessionHandle, SessionRegistry};
    use rust_decimal_macros::dec;
    use std::{
        collections::HashSet,
        sync::{Arc, RwLock},
    };

    fn market_info(id: &str) -> MarketRegistryInfo {
        MarketRegistryInfo {
            market_id: MarketId::new(id),
            event_id: EventId::new("evt-1"),
            token_yes: TokenId::new(format!("{id}-yes")),
            token_no: TokenId::new(format!("{id}-no")),
            question: "Test?".into(),
            slug: "test".into(),
            description: None,
            categories: CategorySet::from(MarketCategory::Other),
            status: MarketStatus::Active,
            outcome: None,
            neg_risk: false,
            tick_size: TickSize::Hundredth,
            tokens: vec![
                TokenInfo {
                    token_id: TokenId::new(format!("{id}-yes")),
                    outcome: "Yes".into(),
                    neg_risk: false,
                },
                TokenInfo {
                    token_id: TokenId::new(format!("{id}-no")),
                    outcome: "No".into(),
                    neg_risk: false,
                },
            ],
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: dec!(5),
            liquidity_usd: None,
            volume_24h: None,
            fee_schedule: None,
            start_date: None,
            end_date: None,
            resolved_at: None,
            created_at: Some(Utc::now()),
            updated_at: Utc::now(),
        }
    }

    fn subscribe_market(registry: &SessionRegistry, market: &str) {
        let (outbound, rx) = flume::bounded::<String>(8);
        let keys: HashSet<SubscriptionKey> = HashSet::from([SubscriptionKey::scoped(
            WsChannel::MarketBookUpdate,
            MarketId::new(market),
        )]);
        registry.register(SessionHandle {
            outbound,
            subscriptions: Arc::new(RwLock::new(keys)),
        });
        drop(rx);
    }

    fn apply_book(store: &BookStore, token: &str) {
        let level = BookLevel::from_decimal(Price::new(dec!(0.5)), Shares::new(dec!(10)))
            .expect("valid level");
        store.apply_snapshot(
            &TokenId::new(token),
            vec![level],
            vec![level],
            1_700_000_000_000,
            None,
        );
    }

    fn build(
        markets: &[&str],
        subscribed: &[&str],
    ) -> (BookUpdateCoalescer, flume::Receiver<CoreEvent>) {
        let book_store = Arc::new(BookStore::new(Arc::new(MetricsHub::new())));
        let market_registry = Arc::new(MarketRegistry::new());
        for id in markets {
            market_registry.register_market(market_info(id));
        }
        let sessions = SessionRegistry::default();
        for id in subscribed {
            subscribe_market(&sessions, id);
        }
        let (events, rx) = CoreEventPublisher::bounded(64);
        (
            BookUpdateCoalescer::new(book_store, market_registry, sessions, events),
            rx,
        )
    }

    fn drain_book_updates(rx: &flume::Receiver<CoreEvent>) -> Vec<MarketId> {
        let mut ids = Vec::new();
        while let Ok(event) = rx.try_recv() {
            if let CoreEvent::MarketBookUpdate { market_id, .. } = event {
                ids.push(market_id);
            }
        }
        ids
    }

    #[test]
    fn emits_only_for_subscribed_markets_with_published_books() {
        let (mut coalescer, rx) = build(&["m1", "m2"], &["m1"]);
        apply_book(&coalescer.book_store, "m1-yes");
        apply_book(&coalescer.book_store, "m2-yes");

        assert_eq!(coalescer.tick(), 1);
        assert_eq!(drain_book_updates(&rx), vec![MarketId::new("m1")]);
    }

    #[test]
    fn skips_markets_without_any_published_side() {
        let (mut coalescer, rx) = build(&["m1"], &["m1"]);
        assert_eq!(coalescer.tick(), 0);
        assert!(drain_book_updates(&rx).is_empty());
    }

    #[test]
    fn dedupes_unchanged_versions_and_reemits_on_change() {
        let (mut coalescer, rx) = build(&["m1"], &["m1"]);
        apply_book(&coalescer.book_store, "m1-yes");

        assert_eq!(coalescer.tick(), 1, "baseline frame");
        assert_eq!(coalescer.tick(), 0, "unchanged book suppressed");

        apply_book(&coalescer.book_store, "m1-no");
        assert_eq!(coalescer.tick(), 1, "no-side change re-emits");
        assert_eq!(drain_book_updates(&rx).len(), 2);
    }

    #[test]
    fn skips_markets_unknown_to_the_registry() {
        let (mut coalescer, rx) = build(&[], &["m1"]);
        apply_book(&coalescer.book_store, "m1-yes");
        assert_eq!(coalescer.tick(), 0);
        assert!(drain_book_updates(&rx).is_empty());
    }

    #[test]
    fn emitted_state_resets_after_unsubscribe() {
        let (mut coalescer, rx) = build(&["m1"], &["m1"]);
        apply_book(&coalescer.book_store, "m1-yes");
        assert_eq!(coalescer.tick(), 1);

        // Simulate all subscribers leaving, then a fresh subscribe.
        coalescer.sessions = SessionRegistry::default();
        assert_eq!(coalescer.tick(), 0);
        subscribe_market(&coalescer.sessions, "m1");

        assert_eq!(
            coalescer.tick(),
            1,
            "fresh subscriber gets a baseline frame"
        );
        assert_eq!(drain_book_updates(&rx).len(), 2);
    }
}
