//! Book-update coalescer — throttled `market.book_update` WS producer.
//!
//! Off the ingest hot path: the data pipeline keeps applying deltas into the
//! [`BookStore`] at full rate, while this task samples the *published*
//! snapshots on a fixed cadence and only for markets that a WebSocket session
//! is actively watching (the [`SessionRegistry`] `ArcSwap` snapshot). Each tick
//! compares the per-token publish versions against the last emitted pair and
//! publishes a [`CoreEvent::MarketBookUpdate`] only when either side changed —
//! so an idle book costs nothing and a hot book is throttled to one frame per
//! tick regardless of delta rate.

use std::{collections::HashMap, sync::Arc, time::Duration};

use quant_pivot_models::{
    domain::{
        api::{MarketBookSideView, MarketBookView},
        runtime::{CoreEvent, CoreEventPublisher},
    },
    types::MarketId,
};
use quant_pivot_web::ws::SessionRegistry;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

use crate::{
    app::{AppContext, task_id::TaskId, task_registry::AppRunner},
    ingest::{book_store::BookStore, market_registry::MarketRegistry},
};

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
        let Self {
            book_store,
            market_registry,
            sessions,
            events,
            emitted,
        } = self;
        sessions.read_watched_markets(|watched| {
            emitted.retain(|market_id, _| watched.contains(market_id));

            let mut published = 0_usize;
            for market_id in watched {
                let Some((yes_token, no_token)) = market_registry.token_pair(market_id) else {
                    // Unknown market (e.g. subscribed before catalog sync) — skip
                    // silently; the registry will learn it on the next Gamma sync.
                    continue;
                };
                let Some(pair) = market_registry
                    .data_plane()
                    .with_index(|index| index.market_token_pair(market_id))
                else {
                    continue;
                };

                let versions = (
                    book_store.book_version(pair.yes),
                    book_store.book_version(pair.no),
                );
                if versions == (0, 0) {
                    // No published book on either side yet — nothing to emit.
                    continue;
                }
                if emitted.get(market_id) == Some(&versions) {
                    continue;
                }

                let view = MarketBookView {
                    market_id: market_id.clone(),
                    yes: book_store
                        .load_owned(pair.yes)
                        .map(|snapshot| MarketBookSideView::from_snapshot(yes_token, &snapshot)),
                    no: book_store
                        .load_owned(pair.no)
                        .map(|snapshot| MarketBookSideView::from_snapshot(no_token, &snapshot)),
                };
                events.publish(CoreEvent::MarketBookUpdate {
                    market_id: market_id.clone(),
                    view: Box::new(view),
                });
                emitted.insert(market_id.clone(), versions);
                published += 1;
            }
            published
        })
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
    use std::sync::Arc;

    use chrono::Utc;
    use flume::Receiver;
    use prometheus::IntCounter;
    use quant_pivot_models::{
        domain::{
            market::{BookLevel, MarketRegistryInfo, TokenInfo, book::BookSnapshot},
            runtime::{CoreEvent, CoreEventPublisher},
            ws::{SubscriptionKey, WsChannel},
        },
        enums::{
            catalog::CatalogFilterReasonSet,
            common::{CategorySet, MarketCategory, TickSize},
            market::MarketStatus,
        },
        types::{EventId, MarketId, Price, Shares, TokenId, UserId},
    };
    use quant_pivot_web::ws::{SessionHubMetrics, SessionRegistration, SessionRegistry};
    use rust_decimal_macros::dec;
    use tokio::{sync::mpsc, task::JoinHandle};
    use tokio_util::sync::CancellationToken;

    use super::BookUpdateCoalescer;
    use crate::{
        ingest::{
            book_store::BookStore, data_plane_index::DataPlane, market_registry::MarketRegistry,
        },
        observability::metrics_hub::MetricsHub,
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
            filter_reasons: CatalogFilterReasonSet::default(),
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
            start_date: None,
            end_date: None,
            resolved_at: None,
            created_at: Some(Utc::now()),
            updated_at: Utc::now(),
        }
    }

    async fn subscribe_market(registry: &SessionRegistry, market: &str) {
        let (outbound, receiver) = mpsc::channel(8);
        let session_id = registry
            .register(SessionRegistration {
                outbound,
                subject: UserId::from_v7(),
                family_id: "test-family".to_owned(),
                can_read_system: false,
                cancellation: CancellationToken::new(),
            })
            .await
            .expect("register coalescer test session");
        assert!(
            registry
                .subscribe(
                    session_id,
                    SubscriptionKey::scoped(WsChannel::MarketBookUpdate, MarketId::new(market),),
                )
                .await
        );
        drop(receiver);
    }

    fn apply_book(store: &BookStore, token: &str) {
        let level = BookLevel::from_decimal(Price::new(dec!(0.5)), Shares::new(dec!(10)))
            .expect("valid level");
        let token = store
            .resolve(&TokenId::new(token))
            .expect("registered book token");
        assert!(store.publish(
            token,
            BookSnapshot::new(Arc::from([level]), Arc::from([level]), 1_700_000_000_000, 1,),
            1,
            1,
            None,
        ));
    }

    async fn build(
        markets: &[&str],
        subscribed: &[&str],
    ) -> (
        BookUpdateCoalescer,
        Receiver<CoreEvent>,
        CancellationToken,
        JoinHandle<()>,
    ) {
        let data_plane = Arc::new(DataPlane::new());
        let book_store = Arc::new(BookStore::new(
            Arc::clone(&data_plane),
            Arc::new(MetricsHub::new()),
        ));
        let market_registry = Arc::new(MarketRegistry::new(data_plane));
        for id in markets {
            market_registry.register_market(market_info(id));
        }
        let (sessions, hub) = SessionRegistry::new(SessionHubMetrics {
            best_effort_dropped: IntCounter::new("test_coalescer_ws_dropped", "test")
                .expect("drop counter"),
            reliable_disconnects: IntCounter::new("test_coalescer_ws_closed", "test")
                .expect("disconnect counter"),
        });
        let shutdown = CancellationToken::new();
        let hub_task = tokio::spawn(hub.run(shutdown.clone()));
        for id in subscribed {
            subscribe_market(&sessions, id).await;
        }
        let (events, rx) = CoreEventPublisher::bounded(64);
        (
            BookUpdateCoalescer::new(book_store, market_registry, sessions, events),
            rx,
            shutdown,
            hub_task,
        )
    }

    fn drain_book_updates(rx: &Receiver<CoreEvent>) -> Vec<MarketId> {
        let mut ids = Vec::new();
        while let Ok(event) = rx.try_recv() {
            if let CoreEvent::MarketBookUpdate { market_id, .. } = event {
                ids.push(market_id);
            }
        }
        ids
    }

    #[tokio::test]
    async fn emits_only_for_subscribed_markets_with_published_books() {
        let (mut coalescer, rx, shutdown, hub_task) = build(&["m1", "m2"], &["m1"]).await;
        apply_book(&coalescer.book_store, "m1-yes");
        apply_book(&coalescer.book_store, "m2-yes");

        assert_eq!(coalescer.tick(), 1);
        assert_eq!(drain_book_updates(&rx), vec![MarketId::new("m1")]);
        shutdown.cancel();
        hub_task.await.expect("hub task");
    }

    #[tokio::test]
    async fn skips_markets_without_any_published_side() {
        let (mut coalescer, rx, shutdown, hub_task) = build(&["m1"], &["m1"]).await;
        assert_eq!(coalescer.tick(), 0);
        assert!(drain_book_updates(&rx).is_empty());
        shutdown.cancel();
        hub_task.await.expect("hub task");
    }

    #[tokio::test]
    async fn dedupes_unchanged_versions_and_reemits_on_change() {
        let (mut coalescer, rx, shutdown, hub_task) = build(&["m1"], &["m1"]).await;
        apply_book(&coalescer.book_store, "m1-yes");

        assert_eq!(coalescer.tick(), 1, "baseline frame");
        assert_eq!(coalescer.tick(), 0, "unchanged book suppressed");

        apply_book(&coalescer.book_store, "m1-no");
        assert_eq!(coalescer.tick(), 1, "no-side change re-emits");
        assert_eq!(drain_book_updates(&rx).len(), 2);
        shutdown.cancel();
        hub_task.await.expect("hub task");
    }

    #[tokio::test]
    async fn skips_markets_unknown_to_the_registry() {
        let (mut coalescer, rx, shutdown, hub_task) = build(&[], &["m1"]).await;
        assert_eq!(coalescer.tick(), 0);
        assert!(drain_book_updates(&rx).is_empty());
        shutdown.cancel();
        hub_task.await.expect("hub task");
    }

    #[tokio::test]
    async fn emitted_state_resets_after_unsubscribe() {
        let (mut coalescer, rx, shutdown, hub_task) = build(&["m1"], &["m1"]).await;
        apply_book(&coalescer.book_store, "m1-yes");
        assert_eq!(coalescer.tick(), 1);

        // Simulate all subscribers leaving, then a fresh subscribe.
        coalescer.sessions.close_all().await;
        assert_eq!(coalescer.tick(), 0);
        subscribe_market(&coalescer.sessions, "m1").await;

        assert_eq!(
            coalescer.tick(),
            1,
            "fresh subscriber gets a baseline frame"
        );
        assert_eq!(drain_book_updates(&rx).len(), 2);
        shutdown.cancel();
        hub_task.await.expect("hub task");
    }
}
