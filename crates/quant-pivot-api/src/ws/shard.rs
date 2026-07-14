//! Single WebSocket shard: a resident actor owning one SDK connection.
//!
//! Each shard is spawned **once** by the router and lives until shutdown. The
//! desired token set arrives over a `tokio::sync::watch` channel (full-state,
//! last-write-wins): changes are debounced and applied by dropping the old SDK
//! client and resubscribing — there is never more than one connection per
//! shard. An empty token set parks the actor instead of busy-looping.

use super::{
    health::ShardHealthBoard,
    ingest_hooks::BookLevelRejectHook,
    normalize::normalize_ws_message,
    reconnect::{ReconnectPolicy, ReconnectState},
    session_hook::WsSessionInvalidationHook,
};
use futures_util::StreamExt;
use polymarket_client_sdk_v2::{
    clob::ws::{Client as SdkWsClient, types::response::WsMessage},
    types::U256,
    ws::config::Config as SdkWsConfig,
};
use quant_pivot_models::{
    domain::pipeline::{PipelineEvent, StreamSessionEndReason},
    enums::system::ShardConnectionStatus,
    hashing::CanonicalDigest,
    types::{ContentHash, TokenId},
};
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, oneshot, watch},
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Debounce window for token-set changes: bursts of assign/remove during a
/// catalog sync coalesce into a single teardown + resubscribe.
const TOKEN_DEBOUNCE: Duration = Duration::from_millis(500);

/// How long a fresh connection keeps holding the global connect permit. The
/// permit bounds concurrent TLS handshakes (thundering-herd protection); it is
/// released on the first received message or after this grace period.
const CONNECT_PERMIT_GRACE: Duration = Duration::from_secs(10);

/// Startup stagger: shard `n` waits `(n % SLOTS) * STEP` before its first
/// connection attempt so dozens of shards never handshake simultaneously.
const STARTUP_STAGGER_STEP: Duration = Duration::from_millis(250);
const STARTUP_STAGGER_SLOTS: usize = 16;
const OUTPUT_ENQUEUE_TIMEOUT: Duration = Duration::from_millis(250);
const SESSION_LEDGER_TIMEOUT: Duration = Duration::from_secs(2);

/// Shared construction dependencies, owned by the router and cloned per shard.
#[derive(Clone)]
pub(super) struct ShardDeps {
    pub output_tx: flume::Sender<PipelineEvent>,
    pub ws_url: String,
    pub shutdown: CancellationToken,
    pub last_message_at: Arc<parking_lot::Mutex<Option<Instant>>>,
    pub on_session_invalidated: Option<WsSessionInvalidationHook>,
    pub on_book_level_rejected: Option<BookLevelRejectHook>,
    /// Shard-level reconnect backoff (from `[market_data.websocket]`).
    pub reconnect_policy: ReconnectPolicy,
    /// SDK-internal reconnect backoff (same config source).
    pub sdk_initial_backoff: Duration,
    pub sdk_max_backoff: Duration,
    /// Global connect-concurrency limiter shared by every shard.
    pub connect_limiter: Arc<Semaphore>,
    /// Aggregated connection-state board for health summaries.
    pub health: Arc<ShardHealthBoard>,
}

/// Why a streaming session ended.
enum StreamEnd {
    /// Process shutdown requested.
    Shutdown,
    /// The desired token set changed — rebuild the subscription immediately.
    Resubscribe,
    /// Connection / subscription failure — back off before retrying.
    Failed(String),
    /// A bounded output queue timed out. The session is invalid and must reconnect.
    Overflow(String),
    /// The router dropped the token channel (manager torn down).
    RouterDropped,
}

/// A single shard actor: one SDK WebSocket connection, multiplexed streams.
pub struct WsShard {
    shard_id: usize,
    tokens_rx: watch::Receiver<Arc<HashSet<TokenId>>>,
    deps: ShardDeps,
}

impl WsShard {
    pub(super) const fn new(
        shard_id: usize,
        tokens_rx: watch::Receiver<Arc<HashSet<TokenId>>>,
        deps: ShardDeps,
    ) -> Self {
        Self {
            shard_id,
            tokens_rx,
            deps,
        }
    }

    /// Resident actor loop — runs until shutdown or router teardown.
    pub async fn run_loop(self) {
        let Self {
            shard_id,
            mut tokens_rx,
            deps,
        } = self;
        let mut reconnect = ReconnectState::new(shard_id, &deps.reconnect_policy);

        // Stagger initial connects across shards (thundering-herd protection).
        let stagger = STARTUP_STAGGER_STEP * stagger_slot(shard_id);
        if !stagger.is_zero() {
            tokio::select! {
                () = deps.shutdown.cancelled() => return,
                () = sleep(stagger) => {}
            }
        }

        loop {
            if deps.shutdown.is_cancelled() {
                break;
            }

            // Park while the shard owns no tokens — no reconnect churn on idle.
            if tokens_rx.borrow_and_update().is_empty() {
                tokio::select! {
                    () = deps.shutdown.cancelled() => break,
                    changed = tokens_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        debounce_token_changes(&mut tokens_rx, &deps.shutdown).await;
                        continue;
                    }
                }
            }

            emit_status(
                &deps,
                shard_id,
                ShardConnectionStatus::Reconnecting {
                    attempt: reconnect.retries_used(),
                },
            );

            let end = connect_and_stream(&deps, shard_id, &mut tokens_rx, &mut reconnect).await;
            deps.health.set_connected(shard_id, false);
            emit_status(&deps, shard_id, ShardConnectionStatus::Disconnected);

            match end {
                StreamEnd::Shutdown | StreamEnd::RouterDropped => break,
                StreamEnd::Resubscribe => reconnect.reset(),
                StreamEnd::Failed(error) | StreamEnd::Overflow(error) => {
                    let Some(delay) = reconnect.next_delay() else {
                        tracing::error!(shard_id, "WS shard reconnection budget exhausted");
                        break;
                    };
                    // Reconnect spam is aggregated by the health checker;
                    // per-attempt detail stays at debug.
                    tracing::debug!(
                        shard_id,
                        error,
                        backoff_ms = delay.as_millis(),
                        "shard stream ended — reconnect scheduled"
                    );
                    tokio::select! {
                        () = deps.shutdown.cancelled() => break,
                        () = sleep(delay) => {}
                        changed = tokens_rx.changed() => {
                            // Token changes cut the backoff short: resubscribe
                            // with the fresh set right away.
                            if changed.is_err() {
                                break;
                            }
                            debounce_token_changes(&mut tokens_rx, &deps.shutdown).await;
                        }
                    }
                }
            }
        }

        tracing::info!(shard_id, "WS shard shutting down");
    }
}

/// Map `shard_id` onto a bounded stagger slot multiplier.
fn stagger_slot(shard_id: usize) -> u32 {
    u32::try_from(shard_id % STARTUP_STAGGER_SLOTS).unwrap_or(0)
}

/// Wait for the token set to settle: every further change restarts the window.
async fn debounce_token_changes(
    tokens_rx: &mut watch::Receiver<Arc<HashSet<TokenId>>>,
    shutdown: &CancellationToken,
) {
    loop {
        let deadline = sleep(TOKEN_DEBOUNCE);
        tokio::pin!(deadline);
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = &mut deadline => return,
            changed = tokens_rx.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
    }
}

/// Build one SDK client over the current token set and pump its streams until
/// shutdown, failure, or a (debounced) token-set change.
async fn connect_and_stream(
    deps: &ShardDeps,
    shard_id: usize,
    tokens_rx: &mut watch::Receiver<Arc<HashSet<TokenId>>>,
    reconnect: &mut ReconnectState,
) -> StreamEnd {
    let tokens = Arc::clone(&tokens_rx.borrow_and_update());
    let asset_ids: Vec<U256> = tokens
        .iter()
        .filter_map(|t| U256::from_str(t.as_str()).ok())
        .collect();
    if asset_ids.is_empty() {
        // Non-empty set with zero parseable ids: back off instead of spinning.
        return StreamEnd::Failed("no subscribable asset ids in token set".to_owned());
    }

    // Bound concurrent connection establishment across all shards. The permit
    // is parked in a detached holder task and released on first traffic,
    // grace expiry, or shutdown — whichever comes first.
    let mut traffic_tx = tokio::select! {
        () = deps.shutdown.cancelled() => return StreamEnd::Shutdown,
        permit = Arc::clone(&deps.connect_limiter).acquire_owned() => match permit {
            Ok(permit) => Some(spawn_permit_holder(permit, &deps.shutdown)),
            Err(_) => return StreamEnd::Shutdown,
        },
    };

    let mut sdk_config = SdkWsConfig::default();
    sdk_config.reconnect.initial_backoff = deps.sdk_initial_backoff;
    sdk_config.reconnect.max_backoff = deps.sdk_max_backoff;
    let client = match SdkWsClient::new(&deps.ws_url, sdk_config) {
        Ok(client) => client,
        Err(error) => return StreamEnd::Failed(format!("WS client creation failed: {error}")),
    };

    // `subscribe_market_resolutions` first — enables SDK `custom_features`
    // on the channel (also required by `best_bid_ask`).
    macro_rules! subscribe {
        ($method:ident) => {
            match client.$method(asset_ids.clone()) {
                Ok(stream) => Box::pin(stream),
                Err(error) => {
                    return StreamEnd::Failed(format!(concat!(stringify!($method), ": {}"), error));
                }
            }
        };
    }
    let mut resolution_stream = subscribe!(subscribe_market_resolutions);
    let mut book_stream = subscribe!(subscribe_orderbook);
    let mut price_stream = subscribe!(subscribe_prices);
    let mut last_trade_stream = subscribe!(subscribe_last_trade_price);
    let mut tick_size_stream = subscribe!(subscribe_tick_size_change);
    let mut bbo_stream = subscribe!(subscribe_best_bid_ask);

    reconnect.reset();
    deps.health.set_connected(shard_id, true);
    emit_status(deps, shard_id, ShardConnectionStatus::Connected);
    let stream_session_id = Uuid::now_v7();
    let opened_at_ms = chrono::Utc::now().timestamp_millis();
    let subscription_token_hash = match subscription_token_hash(&tokens) {
        Ok(hash) => hash,
        Err(error) => return StreamEnd::Failed(error),
    };
    let subscription_token_count = u32::try_from(tokens.len()).unwrap_or(u32::MAX);
    if !send_session_event(
        deps,
        PipelineEvent::StreamSessionOpened {
            stream_session_id,
            shard_id: u32::try_from(shard_id).unwrap_or(u32::MAX),
            subscription_token_hash: subscription_token_hash.clone(),
            subscription_token_count,
            opened_at_ms,
        },
    )
    .await
    {
        return StreamEnd::Overflow("stream-session open ledger enqueue timed out".to_owned());
    }
    let mut token_sequences = HashMap::<TokenId, u64>::new();

    macro_rules! stream_arm {
        ($item:expr, $name:literal, $variant:expr) => {
            match on_stream_item(
                deps,
                shard_id,
                stream_session_id,
                &mut token_sequences,
                $item,
                $name,
                $variant,
            )
            .await
            {
                Ok(()) => {
                    // First traffic observed — release the connect permit.
                    if let Some(tx) = traffic_tx.take() {
                        let _ = tx.send(());
                    }
                }
                Err(error) => break StreamEnd::Overflow(error),
            }
        };
    }

    let end = loop {
        tokio::select! {
            () = deps.shutdown.cancelled() => break StreamEnd::Shutdown,
            changed = tokens_rx.changed() => {
                if changed.is_err() {
                    break StreamEnd::RouterDropped;
                }
                debounce_token_changes(tokens_rx, &deps.shutdown).await;
                if tokens_rx.borrow_and_update().as_ref() != tokens.as_ref() {
                    break StreamEnd::Resubscribe;
                }
            }
            item = book_stream.next() =>
                stream_arm!(item, "book", WsMessage::Book),
            item = price_stream.next() =>
                stream_arm!(item, "price", WsMessage::PriceChange),
            item = resolution_stream.next() =>
                stream_arm!(item, "resolution", WsMessage::MarketResolved),
            item = last_trade_stream.next() =>
                stream_arm!(item, "last trade", WsMessage::LastTradePrice),
            item = tick_size_stream.next() =>
                stream_arm!(item, "tick size", WsMessage::TickSizeChange),
            item = bbo_stream.next() =>
                stream_arm!(item, "best bid/ask", WsMessage::BestBidAsk),
        }
    };
    let closing = ClosingStreamSession {
        stream_session_id,
        shard_id,
        subscription_token_hash,
        subscription_token_count,
        opened_at_ms,
        token_sequences: &token_sequences,
    };
    close_stream_session(deps, closing, stream_end_reason(&end)).await;
    end
}

/// Park the connect permit in a detached holder task.
///
/// The permit bounds concurrent TLS handshakes; it is released when the shard
/// signals first traffic over the returned sender, when
/// [`CONNECT_PERMIT_GRACE`] expires (silent books), or on shutdown.
fn spawn_permit_holder(
    permit: OwnedSemaphorePermit,
    shutdown: &CancellationToken,
) -> oneshot::Sender<()> {
    let (traffic_tx, traffic_rx) = oneshot::channel();
    let shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _hold = permit;
        tokio::select! {
            _ = traffic_rx => {}
            () = sleep(CONNECT_PERMIT_GRACE) => {}
            () = shutdown.cancelled() => {}
        }
    });
    traffic_tx
}

/// Handle one multiplexed stream item: normalize + dispatch payloads, log
/// transient errors, and treat a closed stream as a reconnect signal.
async fn on_stream_item<T, E: Display>(
    deps: &ShardDeps,
    shard_id: usize,
    stream_session_id: Uuid,
    token_sequences: &mut HashMap<TokenId, u64>,
    item: Option<Result<T, E>>,
    stream: &'static str,
    into_message: impl FnOnce(T) -> WsMessage,
) -> Result<(), String> {
    match item {
        Some(Ok(payload)) => {
            let ws_ingress = Instant::now();
            dispatch_events(
                deps,
                shard_id,
                stream_session_id,
                token_sequences,
                normalize_ws_message(
                    into_message(payload),
                    ws_ingress,
                    deps.on_book_level_rejected.as_ref(),
                ),
            )
            .await
        }
        Some(Err(error)) => {
            tracing::debug!(shard_id, %error, stream, "stream error");
            Ok(())
        }
        None => Err(format!("{stream} stream closed")),
    }
}

async fn dispatch_events(
    deps: &ShardDeps,
    shard_id: usize,
    stream_session_id: Uuid,
    token_sequences: &mut HashMap<TokenId, u64>,
    events: Vec<PipelineEvent>,
) -> Result<(), String> {
    let received_at = Instant::now();
    if !events.is_empty() {
        *deps.last_message_at.lock() = Some(received_at);
    }
    for mut event in events {
        let token_sequence = event.asset_id().map_or(0, |token_id| {
            let sequence = token_sequences.entry(token_id.clone()).or_insert(0);
            *sequence = sequence.saturating_add(1);
            *sequence
        });
        event.assign_stream_provenance(
            stream_session_id,
            u32::try_from(shard_id).unwrap_or(u32::MAX),
            token_sequence,
        );
        let send = deps.output_tx.send_async(event);
        if !matches!(timeout(OUTPUT_ENQUEUE_TIMEOUT, send).await, Ok(Ok(()))) {
            if let Some(hook) = &deps.on_session_invalidated {
                hook(1);
            }
            return Err("WS output queue timed out; canonical session invalidated".to_owned());
        }
    }
    Ok(())
}

fn subscription_token_hash(tokens: &HashSet<TokenId>) -> Result<ContentHash, String> {
    let mut token_ids = tokens.iter().map(TokenId::as_str).collect::<Vec<_>>();
    token_ids.sort_unstable();
    CanonicalDigest::content_hash_json(&token_ids).map_err(|error| error.to_string())
}

const fn stream_end_reason(end: &StreamEnd) -> StreamSessionEndReason {
    match end {
        StreamEnd::Shutdown | StreamEnd::RouterDropped => StreamSessionEndReason::Shutdown,
        StreamEnd::Resubscribe => StreamSessionEndReason::Resubscribe,
        StreamEnd::Overflow(_) => StreamSessionEndReason::Overflow,
        StreamEnd::Failed(_) => StreamSessionEndReason::Disconnect,
    }
}

async fn send_session_event(deps: &ShardDeps, event: PipelineEvent) -> bool {
    matches!(
        timeout(SESSION_LEDGER_TIMEOUT, deps.output_tx.send_async(event)).await,
        Ok(Ok(()))
    )
}

struct ClosingStreamSession<'a> {
    stream_session_id: Uuid,
    shard_id: usize,
    subscription_token_hash: ContentHash,
    subscription_token_count: u32,
    opened_at_ms: i64,
    token_sequences: &'a HashMap<TokenId, u64>,
}

async fn close_stream_session(
    deps: &ShardDeps,
    session: ClosingStreamSession<'_>,
    reason: StreamSessionEndReason,
) {
    let ClosingStreamSession {
        stream_session_id,
        shard_id,
        subscription_token_hash,
        subscription_token_count,
        opened_at_ms,
        token_sequences,
    } = session;
    let closed_at_ms = chrono::Utc::now().timestamp_millis();
    let mut received_sequences = token_sequences
        .iter()
        .map(|(token_id, sequence)| (token_id.clone(), *sequence))
        .collect::<Vec<_>>();
    received_sequences.sort_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
    let closed = PipelineEvent::StreamSessionClosed {
        stream_session_id,
        shard_id: u32::try_from(shard_id).unwrap_or(u32::MAX),
        subscription_token_hash,
        subscription_token_count,
        received_sequences: Arc::from(received_sequences.clone()),
        opened_at_ms,
        closed_at_ms,
        reason,
    };
    if !send_session_event(deps, closed).await {
        tracing::error!(%stream_session_id, shard_id, "failed to enqueue invalid stream-session close ledger");
    }
    if reason == StreamSessionEndReason::Normal {
        return;
    }
    for (token_id, last_received_sequence) in received_sequences {
        let gap = PipelineEvent::StreamGap {
            asset_id: token_id,
            stream_session_id,
            shard_id: u32::try_from(shard_id).unwrap_or(u32::MAX),
            last_received_sequence,
            timestamp_ms: u64::try_from(closed_at_ms).unwrap_or(0),
        };
        if !send_session_event(deps, gap).await {
            tracing::error!(%stream_session_id, shard_id, "failed to enqueue stream gap");
        }
    }
}

fn emit_status(deps: &ShardDeps, shard_id: usize, status: ShardConnectionStatus) {
    let _ = deps
        .output_tx
        .try_send(PipelineEvent::ShardStatus { shard_id, status });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_deps(
        output_tx: flume::Sender<PipelineEvent>,
        hook: Option<WsSessionInvalidationHook>,
    ) -> ShardDeps {
        ShardDeps {
            output_tx,
            ws_url: "ws://test".into(),
            shutdown: CancellationToken::new(),
            last_message_at: Arc::new(parking_lot::Mutex::new(None)),
            on_session_invalidated: hook,
            on_book_level_rejected: None,
            reconnect_policy: ReconnectPolicy::default(),
            sdk_initial_backoff: Duration::from_secs(1),
            sdk_max_backoff: Duration::from_secs(30),
            connect_limiter: Arc::new(Semaphore::new(4)),
            health: Arc::new(ShardHealthBoard::default()),
        }
    }

    #[tokio::test]
    async fn dispatch_preserves_every_event_when_capacity_is_available() {
        let (tx, rx) = flume::bounded(3);
        let dropped = Arc::new(AtomicU64::new(0));
        let hook: WsSessionInvalidationHook = {
            let dropped = Arc::clone(&dropped);
            Arc::new(move |n| {
                dropped.fetch_add(n, Ordering::Relaxed);
            })
        };
        let deps = test_deps(tx, Some(hook));

        let status = |_n| PipelineEvent::ShardStatus {
            shard_id: 0,
            status: ShardConnectionStatus::Connected,
        };

        let mut sequences = HashMap::new();
        dispatch_events(
            &deps,
            0,
            Uuid::new_v4(),
            &mut sequences,
            vec![status(1), status(2), status(3)],
        )
        .await
        .expect("dispatch");

        assert_eq!(rx.len(), 3);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn debounce_waits_for_token_set_to_settle() {
        let (tx, mut rx) = watch::channel(Arc::new(HashSet::<TokenId>::new()));
        let shutdown = CancellationToken::new();

        tx.send_replace(Arc::new(HashSet::from([TokenId::new("1")])));
        rx.changed().await.expect("sender alive");

        let debounce = tokio::spawn(async move {
            debounce_token_changes(&mut rx, &shutdown).await;
            Arc::clone(&rx.borrow())
        });

        // A second update inside the window must be folded into one rebuild.
        tokio::time::sleep(Duration::from_millis(200)).await;
        tx.send_replace(Arc::new(HashSet::from([
            TokenId::new("1"),
            TokenId::new("2"),
        ])));

        let settled = debounce.await.expect("debounce task");
        assert_eq!(settled.len(), 2, "debounced set reflects the last write");
    }
}
