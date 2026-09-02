use std::{
    collections::{BTreeSet, HashMap, HashSet},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use quant_pivot_api::gamma::GammaClient;
use quant_pivot_models::{
    config::GammaConfig,
    domain::market::{BookLevel, EventRegistryInfo, MarketRegistryInfo},
    types::TokenId,
};
use reqwest::Client;
use serde_json::{Value, json};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{
        Notify, RwLock,
        mpsc::{self, Sender},
    },
    task::{JoinHandle, JoinSet},
    time::{Instant, sleep, timeout, timeout_at},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const FIXTURE_TIMESTAMP: &str = "2026-01-01T00:00:00Z";
const WS_MARKET: &str = "0x5f65177b394277fd294cd75650044e32ba009a95022d88a0c1d565897d72f8f1";
const CONNECTION_OUTBOX_CAPACITY: usize = 8_192;
const OWNER_READINESS_POLL: Duration = Duration::from_millis(25);

pub struct DeterministicCatalog {
    pub events: Vec<EventRegistryInfo>,
    pub markets: Vec<MarketRegistryInfo>,
    pub tokens: Vec<TokenId>,
    pub fixture_hash: String,
    _server: MockServer,
}

impl DeterministicCatalog {
    pub async fn load(market_count: usize) -> Result<Self> {
        let body = gamma_fixture(market_count);
        let fixture_bytes = serde_json::to_vec(&body).context("serialize Gamma fixture")?;
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/events/keyset"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;
        let client = GammaClient::new(GammaConfig {
            base_url: server.uri(),
            page_size: 500,
            ..GammaConfig::default()
        });
        let (events, markets) = client
            .full_sync_detailed()
            .await
            .context("parse deterministic Gamma catalog through production client")?;
        if markets.len() != market_count {
            bail!(
                "Gamma fixture produced {} accepted markets; expected {market_count}",
                markets.len()
            );
        }
        let mut tokens = markets
            .iter()
            .flat_map(|market| [market.token_yes.clone(), market.token_no.clone()])
            .collect::<Vec<_>>();
        tokens.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        if tokens.len() != market_count.saturating_mul(2) {
            bail!("Gamma fixture did not produce a complete binary token pair per market");
        }
        Ok(Self {
            events,
            markets,
            tokens,
            fixture_hash: blake3::hash(&fixture_bytes).to_hex().to_string(),
            _server: server,
        })
    }
}

fn gamma_fixture(market_count: usize) -> Value {
    let markets = (0..market_count)
        .map(|index| {
            let yes_token = index.saturating_mul(2).saturating_add(1);
            let no_token = yes_token.saturating_add(1);
            json!({
                "id": format!("performance-market-{index}"),
                "conditionId": format!("0x{index:064x}"),
                "question": format!("Deterministic performance market {index}?"),
                "slug": format!("performance-market-{index}"),
                "clobTokenIds": [yes_token.to_string(), no_token.to_string()],
                "outcomes": ["Yes", "No"],
                "active": true,
                "closed": false,
                "enableOrderBook": true,
                "acceptingOrders": true,
                "orderMinSize": "5",
                "orderPriceMinTickSize": "0.01",
                "liquidityNum": "100000",
                "volume24hr": "10000",
                "createdAt": FIXTURE_TIMESTAMP,
                "updatedAt": FIXTURE_TIMESTAMP,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "events": [{
            "id": "performance-event",
            "title": "Deterministic performance event",
            "slug": "deterministic-performance-event",
            "active": true,
            "closed": false,
            "negRisk": false,
            "tags": [{"label": "Crypto", "slug": "crypto"}],
            "markets": markets,
            "createdAt": FIXTURE_TIMESTAMP,
            "updatedAt": FIXTURE_TIMESTAMP,
        }],
        "next_cursor": null,
    })
}

struct ConnectionState {
    tokens: RwLock<HashSet<String>>,
    tx: Sender<String>,
}

#[derive(Clone)]
struct ExactBookSnapshot {
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
    generation: u64,
}

struct WsState {
    connections: RwLock<HashMap<u64, Arc<ConnectionState>>>,
    token_owner: RwLock<HashMap<String, u64>>,
    exact_books: RwLock<HashMap<String, ExactBookSnapshot>>,
    subscription_changed: Notify,
    next_connection_id: AtomicU64,
    active_connections: AtomicU64,
    connection_high_water: AtomicU64,
    keepalive_enabled: AtomicBool,
}

pub struct DeterministicClobServer {
    address: SocketAddr,
    state: Arc<WsState>,
    shutdown: CancellationToken,
    accept_task: JoinHandle<()>,
    keepalive_task: Option<JoinHandle<Result<()>>>,
}

/// Cloneable control plane for a bounded report-readiness refresh window.
#[derive(Clone)]
pub struct DeterministicClobRefreshHandle {
    state: Arc<WsState>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DeliveryStats {
    pub events: u64,
    pub encoded_bytes: u64,
}

impl DeterministicClobServer {
    pub async fn start() -> Result<Self> {
        Self::start_inner(None).await
    }

    /// Start the deterministic transport with a continuous venue-data pulse.
    ///
    /// Production-stack functional runs can spend many minutes in governed
    /// research before requesting a report. The pulse keeps the real CLOB
    /// transport, decoder, shard-health board, and data pipeline live for that
    /// entire interval; it does not bypass the production readiness boundary.
    pub async fn start_keepalive(period: Duration) -> Result<Self> {
        if period.is_zero() {
            bail!("deterministic CLOB keepalive period must be non-zero");
        }
        Self::start_inner(Some(period)).await
    }

    async fn start_inner(keepalive_period: Option<Duration>) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("bind deterministic CLOB WebSocket upstream")?;
        let address = listener
            .local_addr()
            .context("read deterministic CLOB WebSocket address")?;
        let state = Arc::new(WsState {
            connections: RwLock::new(HashMap::new()),
            token_owner: RwLock::new(HashMap::new()),
            exact_books: RwLock::new(HashMap::new()),
            subscription_changed: Notify::new(),
            next_connection_id: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            connection_high_water: AtomicU64::new(0),
            keepalive_enabled: AtomicBool::new(true),
        });
        let shutdown = CancellationToken::new();
        let accept_state = Arc::clone(&state);
        let accept_shutdown = shutdown.clone();
        let accept_task = tokio::spawn(async move {
            let mut connection_tasks = JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    () = accept_shutdown.cancelled() => break,
                    Some(joined) = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                        match joined {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => {
                                tracing::debug!(%error, "deterministic CLOB connection closed");
                            }
                            Err(error) => {
                                tracing::error!(%error, "deterministic CLOB connection task failed");
                            }
                        }
                    }
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else {
                            break;
                        };
                        let connection_state = Arc::clone(&accept_state);
                        let connection_shutdown = accept_shutdown.clone();
                        connection_tasks.spawn(async move {
                            handle_connection(
                                stream,
                                connection_state,
                                connection_shutdown,
                            ).await
                        });
                    }
                }
            }
            while let Some(joined) = connection_tasks.join_next().await {
                match joined {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        tracing::debug!(%error, "deterministic CLOB connection closed");
                    }
                    Err(error) => {
                        tracing::error!(%error, "deterministic CLOB connection task failed");
                    }
                }
            }
        });
        let keepalive_task = keepalive_period.map(|period| {
            let keepalive_state = Arc::clone(&state);
            let keepalive_shutdown = shutdown.clone();
            tokio::spawn(async move {
                let mut generation = 0_u64;
                loop {
                    tokio::select! {
                        biased;
                        () = keepalive_shutdown.cancelled() => return Ok(()),
                        () = tokio::time::sleep(period) => {
                            generation = generation.saturating_add(1);
                            pulse_connections(&keepalive_state, generation).await?;
                        }
                    }
                }
            })
        });
        Ok(Self {
            address,
            state,
            shutdown,
            accept_task,
            keepalive_task,
        })
    }

    #[must_use]
    pub fn base_url(&self) -> String {
        format!("ws://{}", self.address)
    }

    pub async fn wait_for_subscriptions(&self, expected: usize, wait: Duration) -> Result<()> {
        timeout(wait, async {
            loop {
                let changed = self.state.subscription_changed.notified();
                if self.state.token_owner.read().await.len() == expected {
                    return;
                }
                changed.await;
            }
        })
        .await
        .with_context(|| format!("wait for {expected} deterministic CLOB subscriptions"))?;
        Ok(())
    }

    pub async fn wait_for_active_connections(&self, expected: u64, wait: Duration) -> Result<()> {
        timeout(wait, async {
            loop {
                let changed = self.state.subscription_changed.notified();
                if self.active_connection_count() == expected {
                    return;
                }
                changed.await;
            }
        })
        .await
        .with_context(|| format!("wait for {expected} deterministic CLOB connections"))?;
        Ok(())
    }

    #[must_use]
    pub fn active_connection_count(&self) -> u64 {
        self.state.active_connections.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn connection_high_water(&self) -> u64 {
        self.state.connection_high_water.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn accepted_connection_count(&self) -> u64 {
        self.state.next_connection_id.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn refresh_handle(&self) -> DeterministicClobRefreshHandle {
        DeterministicClobRefreshHandle {
            state: Arc::clone(&self.state),
        }
    }

    pub async fn send_delta_batch(
        &self,
        tokens: &[&TokenId],
        timestamp_ms: u64,
        sequence: u64,
    ) -> Result<DeliveryStats> {
        let owners = self.state.token_owner.read().await;
        let mut grouped = HashMap::<u64, Vec<&TokenId>>::new();
        for token in tokens {
            if let Some(owner) = owners.get(token.as_str()) {
                grouped.entry(*owner).or_default().push(*token);
            }
        }
        drop(owners);
        let routes = {
            let connections = self.state.connections.read().await;
            grouped
                .into_iter()
                .filter_map(|(owner, owned_tokens)| {
                    connections
                        .get(&owner)
                        .map(|connection| (Arc::clone(connection), owned_tokens))
                })
                .collect::<Vec<_>>()
        };
        let mut delivered = DeliveryStats::default();
        for (connection, owned_tokens) in routes {
            let changes = owned_tokens
                .iter()
                .enumerate()
                .map(|(offset, token)| {
                    let offset = u64::try_from(offset).unwrap_or(u64::MAX);
                    let price = if sequence.saturating_add(offset).is_multiple_of(2) {
                        "0.49"
                    } else {
                        "0.48"
                    };
                    json!({
                        "asset_id": token.as_str(),
                        "price": price,
                        "size": "100",
                        "side": "BUY",
                        "hash": format!("delta-{sequence}-{offset}"),
                        "best_bid": price,
                        "best_ask": "0.51"
                    })
                })
                .collect::<Vec<_>>();
            let payload = json!({
                "event_type": "price_change",
                "market": WS_MARKET,
                "price_changes": changes,
                "timestamp": timestamp_ms.to_string(),
            })
            .to_string();
            delivered.encoded_bytes = delivered
                .encoded_bytes
                .saturating_add(u64::try_from(payload.len()).unwrap_or(u64::MAX));
            connection
                .tx
                .send(payload)
                .await
                .context("enqueue deterministic CLOB delta frame")?;
            delivered.events = delivered
                .events
                .saturating_add(u64::try_from(owned_tokens.len()).unwrap_or(u64::MAX));
        }
        Ok(delivered)
    }

    pub async fn shutdown(mut self) -> Result<()> {
        self.shutdown.cancel();
        let accept_result = (&mut self.accept_task)
            .await
            .context("join deterministic CLOB accept task");
        let keepalive_result = match self.keepalive_task.as_mut() {
            Some(task) => task
                .await
                .context("join deterministic CLOB keepalive task")?,
            None => Ok(()),
        };
        accept_result?;
        keepalive_result?;
        if self.active_connection_count() != 0 {
            bail!(
                "deterministic CLOB shutdown retained {} active connections",
                self.active_connection_count(),
            );
        }
        Ok(())
    }
}

impl DeterministicClobRefreshHandle {
    /// Await the exact token cohort and live, writable owners before starting an event budget.
    ///
    /// This control-plane deadline includes catalog discovery and subscription
    /// convergence. A count match is insufficient, and readiness is not a lease:
    /// every subsequent send rechecks ownership and fails closed on disconnect.
    pub async fn wait_for_token_owners(&self, tokens: &[TokenId], wait: Duration) -> Result<()> {
        let expected = tokens
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>();
        ensure!(
            !expected.is_empty() && expected.len() == tokens.len() && !wait.is_zero(),
            "exact CLOB phase=discovery requires a nonempty unique cohort and bounded wait"
        );
        let mut last_state = String::from("not observed");
        timeout(wait, async {
            loop {
                let changed = self.state.subscription_changed.notified();
                // Never acquire connection.tokens while holding token_owner: a
                // subscription writer takes those locks in the opposite order.
                let owners = self.state.token_owner.read().await;
                let connections = self.state.connections.read().await;
                let missing = expected
                    .iter()
                    .filter(|token| !owners.contains_key(token.as_str()))
                    .collect::<Vec<_>>();
                let unavailable = expected
                    .iter()
                    .filter(|token| {
                        owners.get(token.as_str()).is_some_and(|owner| {
                            connections.get(owner).is_none_or(|connection| {
                                connection.tx.is_closed() || connection.tx.capacity() == 0
                            })
                        })
                    })
                    .collect::<Vec<_>>();
                let unexpected = owners
                    .keys()
                    .filter(|token| !expected.contains(token.as_str()))
                    .collect::<BTreeSet<_>>();
                if missing.is_empty() && unavailable.is_empty() && unexpected.is_empty() {
                    return;
                }
                last_state = format!(
                    "expected={} owners={} missing_count={} missing={:?} unavailable_count={} unavailable={:?} unexpected_count={} unexpected={:?}",
                    expected.len(),
                    owners.len(),
                    missing.len(),
                    missing.iter().take(8).collect::<Vec<_>>(),
                    unavailable.len(),
                    unavailable.iter().take(8).collect::<Vec<_>>(),
                    unexpected.len(),
                    unexpected.iter().take(8).collect::<Vec<_>>(),
                );
                drop((missing, unavailable, unexpected));
                drop(connections);
                drop(owners);
                tokio::select! {
                    () = changed => {},
                    // Queue capacity can recover without a subscription change.
                    () = sleep(OWNER_READINESS_POLL) => {},
                }
            }
        })
        .await
        .with_context(|| format!("exact CLOB phase=discovery exceeded {wait:?}: {last_state}"))?;
        Ok(())
    }

    /// Pause periodic refreshes while installing exact report books.
    pub fn pause_keepalive(&self) {
        self.state.keepalive_enabled.store(false, Ordering::Release);
    }

    /// Resume current-time refreshes without replacing installed exact book levels.
    pub fn resume_keepalive(&self) {
        self.state.keepalive_enabled.store(true, Ordering::Release);
    }

    /// Enqueue one exact book within the caller's event deadline and return its wire timestamp.
    ///
    /// Subscription discovery belongs to `wait_for_token_owners`, not this
    /// event budget. The caller must still prove durable readback before the
    /// same deadline; queue admission is not a delivery acknowledgment.
    pub async fn send_snapshot(
        &self,
        token: &TokenId,
        bids: &[BookLevel],
        asks: &[BookLevel],
        generation: u64,
        deadline: Instant,
    ) -> Result<DateTime<Utc>> {
        timeout_at(deadline, async {
            ensure!(
                Instant::now() < deadline,
                "exact CLOB token {token} phase=send deadline already exhausted"
            );
            let connection_id = self
                .state
                .token_owner
                .read()
                .await
                .get(token.as_str())
                .copied()
                .with_context(|| format!("exact CLOB token {token} phase=send has no owner"))?;
            let connection = self
                .state
                .connections
                .read()
                .await
                .get(&connection_id)
                .map(Arc::clone)
                .with_context(|| {
                    format!("exact CLOB token {token} phase=send owner disconnected")
                })?;
            let permit = connection.tx.reserve().await.with_context(|| {
                format!("exact CLOB token {token} phase=send owner outbox closed")
            })?;
            let owners = self.state.token_owner.read().await;
            ensure!(
                owners.get(token.as_str()) == Some(&connection_id) && !connection.tx.is_closed(),
                "exact CLOB token {token} phase=send owner changed before enqueue"
            );
            let mut exact_books = self.state.exact_books.write().await;
            let generation = match exact_books.get(token.as_str()) {
                Some(previous) => generation.max(
                    previous
                        .generation
                        .checked_add(1)
                        .context("deterministic exact book generation exhausted")?,
                ),
                None => generation,
            };
            ensure!(
                Instant::now() < deadline,
                "exact CLOB token {token} phase=send deadline exhausted before enqueue"
            );
            exact_books.insert(
                token.as_str().to_owned(),
                ExactBookSnapshot {
                    bids: bids.to_vec(),
                    asks: asks.to_vec(),
                    generation,
                },
            );
            let sent_at = Utc::now();
            permit.send(exact_snapshot_payload(token, bids, asks, generation, sent_at).to_string());
            // Keep ownership and exact-state guards through enqueue so a
            // superseding subscription or periodic pulse cannot overtake it.
            drop(exact_books);
            drop(owners);
            Ok(sent_at)
        })
        .await
        .with_context(|| format!("exact CLOB token {token} phase=send exceeded delivery deadline"))?
        .with_context(|| format!("exact CLOB token {token} phase=send failed"))
    }
}

impl Drop for DeterministicClobServer {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.accept_task.abort();
        if let Some(task) = self.keepalive_task.as_ref() {
            task.abort();
        }
    }
}

async fn pulse_connections(state: &WsState, generation: u64) -> Result<()> {
    if !state.keepalive_enabled.load(Ordering::Acquire) {
        return Ok(());
    }
    let connections = state
        .connections
        .read()
        .await
        .values()
        .cloned()
        .collect::<Vec<_>>();
    for connection in connections {
        let mut tokens = connection
            .tokens
            .read()
            .await
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        tokens.sort_unstable();
        if tokens.is_empty() {
            continue;
        }
        // Refresh every subscribed token on every pulse. Rotating one token
        // per pulse makes freshness proportional to shard cardinality (for
        // example, 20 tokens at a 10-second pulse become 200 seconds old),
        // which violates the production book-age contract while the transport
        // itself still appears healthy.
        for token in tokens {
            // A normal reconnect can retire the copied connection between the
            // state read and this send. Its handler owns cleanup; the next
            // pulse observes only the replacement connection.
            let Ok(permit) = connection.tx.reserve().await else {
                break;
            };
            // Reserve bounded queue capacity before locking book state. Selecting
            // and enqueuing under the same short lock orders a pulse against exact
            // installs; an already-started generic pulse cannot overtake an install.
            let mut exact_books = state.exact_books.write().await;
            if !state.keepalive_enabled.load(Ordering::Acquire) {
                return Ok(());
            }
            let payload = if let Some(snapshot) = exact_books.get_mut(&token) {
                snapshot.generation = snapshot
                    .generation
                    .checked_add(1)
                    .context("deterministic exact book generation exhausted")?
                    .max(generation);
                exact_snapshot_payload(
                    &TokenId::new(&token),
                    &snapshot.bids,
                    &snapshot.asks,
                    snapshot.generation,
                    Utc::now(),
                )
            } else {
                live_snapshot_payload(&token, generation)
            };
            permit.send(payload.to_string());
            // Retain the lock through enqueue so an older snapshot cannot overtake an install.
            drop(exact_books);
        }
    }
    Ok(())
}

async fn handle_connection(
    stream: TcpStream,
    state: Arc<WsState>,
    shutdown: CancellationToken,
) -> Result<()> {
    let websocket = accept_async(stream)
        .await
        .context("accept deterministic CLOB WebSocket")?;
    let connection_id = state.next_connection_id.fetch_add(1, Ordering::Relaxed);
    let (tx, mut rx) = mpsc::channel(CONNECTION_OUTBOX_CAPACITY);
    let connection = Arc::new(ConnectionState {
        tokens: RwLock::new(HashSet::new()),
        tx,
    });
    state
        .connections
        .write()
        .await
        .insert(connection_id, Arc::clone(&connection));
    let active = state
        .active_connections
        .fetch_add(1, Ordering::AcqRel)
        .saturating_add(1);
    state
        .connection_high_water
        .fetch_max(active, Ordering::AcqRel);
    state.subscription_changed.notify_waiters();
    let result = async {
        let (mut writer, mut reader) = websocket.split();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                incoming = reader.next() => {
                    match incoming {
                        Some(Ok(Message::Text(text))) if text == "PING" => {
                            writer.send(Message::Text("PONG".into())).await
                                .context("write deterministic CLOB heartbeat")?;
                        }
                        Some(Ok(Message::Text(text))) => {
                            apply_subscription(connection_id, &text, &state, &connection).await?;
                        }
                        Some(Ok(Message::Close(_))) | None => break,
                        Some(Ok(_)) => {}
                        Some(Err(error)) => {
                            return Err(error).context("read deterministic CLOB frame");
                        }
                    }
                }
                outgoing = rx.recv() => {
                    let Some(outgoing) = outgoing else {
                        break;
                    };
                    writer.send(Message::Text(outgoing.into())).await
                        .context("write deterministic CLOB market frame")?;
                }
            }
        }
        Ok(())
    }
    .await;
    let tokens = connection.tokens.read().await.clone();
    let mut owners = state.token_owner.write().await;
    owners.retain(|token, owner| *owner != connection_id || !tokens.contains(token));
    drop(owners);
    state.connections.write().await.remove(&connection_id);
    state.active_connections.fetch_sub(1, Ordering::AcqRel);
    state.subscription_changed.notify_waiters();
    result
}

async fn apply_subscription(
    connection_id: u64,
    text: &str,
    state: &WsState,
    connection: &ConnectionState,
) -> Result<()> {
    let payload: Value = serde_json::from_str(text).context("decode CLOB subscription request")?;
    let tokens = payload
        .get("assets_ids")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if tokens.is_empty() {
        return Ok(());
    }
    let unsubscribe = payload.get("operation").and_then(Value::as_str) == Some("unsubscribe");
    let mut owned = connection.tokens.write().await;
    let mut owners = state.token_owner.write().await;
    for token in &tokens {
        if unsubscribe {
            owned.remove(token.as_str());
            if owners.get(token.as_str()) == Some(&connection_id) {
                owners.remove(token.as_str());
            }
        } else {
            owned.insert(token.clone());
            owners.insert(token.clone(), connection_id);
        }
    }
    drop(owners);
    drop(owned);
    state.subscription_changed.notify_waiters();
    if !unsubscribe {
        for token in tokens {
            let permit = connection
                .tx
                .reserve()
                .await
                .context("reserve deterministic CLOB initial snapshot")?;
            let exact_books = state.exact_books.read().await;
            let payload = exact_books.get(&token).map_or_else(
                || snapshot_payload(&token, connection_id),
                |snapshot| {
                    exact_snapshot_payload(
                        &TokenId::new(&token),
                        &snapshot.bids,
                        &snapshot.asks,
                        snapshot.generation,
                        Utc::now(),
                    )
                },
            );
            permit.send(payload.to_string());
            // Retain the lock through enqueue so a newer pulse cannot precede this snapshot.
            drop(exact_books);
        }
    }
    Ok(())
}

fn snapshot_payload(token: &str, generation: u64) -> Value {
    let timestamp_ms = 1_767_225_600_000_u64.saturating_add(generation);
    json!({
        "event_type": "book",
        "asset_id": token,
        "market": WS_MARKET,
        "bids": [
            {"price": "0.49", "size": "100"},
            {"price": "0.48", "size": "200"}
        ],
        "asks": [
            {"price": "0.51", "size": "100"},
            {"price": "0.52", "size": "200"}
        ],
        "timestamp": timestamp_ms.to_string(),
        "hash": format!("snapshot-{token}-{generation}"),
    })
}

fn live_snapshot_payload(token: &str, generation: u64) -> Value {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_millis());
    json!({
        "event_type": "book",
        "asset_id": token,
        "market": WS_MARKET,
        "bids": [
            {"price": "0.49", "size": "100"},
            {"price": "0.48", "size": "200"}
        ],
        "asks": [
            {"price": "0.51", "size": "100"},
            {"price": "0.52", "size": "200"}
        ],
        "timestamp": timestamp_ms.to_string(),
        "hash": format!("keepalive-{token}-{generation}"),
    })
}

fn exact_snapshot_payload(
    token: &TokenId,
    bids: &[BookLevel],
    asks: &[BookLevel],
    generation: u64,
    sent_at: DateTime<Utc>,
) -> Value {
    let timestamp_ms = sent_at.timestamp_millis();
    let levels = |levels: &[BookLevel]| {
        levels
            .iter()
            .map(|level| {
                json!({
                    "price": level.price_decimal().to_string(),
                    "size": level.size_decimal().to_string(),
                })
            })
            .collect::<Vec<_>>()
    };
    json!({
        "event_type": "book",
        "asset_id": token.as_str(),
        "market": WS_MARKET,
        "bids": levels(bids),
        "asks": levels(asks),
        "timestamp": timestamp_ms.to_string(),
        "hash": format!("exact-{token}-{generation}"),
    })
}

pub async fn measure_http_rtt(base_url: &str, samples: usize) -> Result<u64> {
    let client = Client::new();
    let mut observations = Vec::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        client
            .get(format!("{base_url}/ping"))
            .send()
            .await
            .context("probe ClickHouse RTT")?
            .error_for_status()
            .context("ClickHouse RTT probe returned failure")?;
        observations.push(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
    }
    observations.sort_unstable();
    observations
        .get(observations.len() / 2)
        .copied()
        .context("ClickHouse RTT requires at least one sample")
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        slice,
        sync::Arc,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use anyhow::{Context, Result, ensure};
    use chrono::Utc;
    use futures_util::{SinkExt, StreamExt};
    use quant_pivot_models::{
        domain::market::BookLevel,
        types::{Price, Shares, TokenId},
    };
    use rust_decimal_macros::dec;
    use serde_json::{Value, json};
    use tokio::{
        net::TcpStream,
        time::{Instant, sleep, timeout_at},
    };
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

    use super::{CONNECTION_OUTBOX_CAPACITY, DeterministicClobServer, ExactBookSnapshot};
    use crate::support::trade_policy_fixtures::FixtureBookTiming;

    impl ExactBookSnapshot {
        fn readiness_fixture() -> Self {
            Self {
                bids: vec![BookLevel::from_decimal_unchecked(
                    Price::new(dec!(0.42)),
                    Shares::new(dec!(12)),
                )],
                asks: vec![BookLevel::from_decimal_unchecked(
                    Price::new(dec!(0.44)),
                    Shares::new(dec!(14)),
                )],
                generation: 7,
            }
        }

        async fn receive_exact(
            &self,
            socket: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
            token: &TokenId,
            deadline: Instant,
        ) -> Result<u128> {
            loop {
                let frame = timeout_at(deadline, socket.next())
                    .await
                    .context("exact frame exceeded its event delivery deadline")?
                    .context("exact frame socket closed")??;
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value = serde_json::from_str(text.as_ref())?;
                if payload["asset_id"] == token.as_str()
                    && payload["hash"]
                        .as_str()
                        .is_some_and(|hash| hash.starts_with("exact-"))
                {
                    let (_, timestamp) = self.verify_frame(token.as_str(), &payload)?;
                    ensure!(
                        Instant::now() < deadline,
                        "exact frame arrived after its deadline"
                    );
                    return Ok(timestamp);
                }
            }
        }

        fn verify_frame(&self, token: &str, payload: &Value) -> Result<(u64, u128)> {
            ensure!(payload["event_type"] == "book" && payload["asset_id"] == token);
            for (side, expected) in [("bids", &self.bids), ("asks", &self.asks)] {
                let actual = payload[side].as_array().context("exact book levels")?;
                ensure!(actual.len() == expected.len(), "exact book depth changed");
                for (actual, expected) in actual.iter().zip(expected) {
                    ensure!(actual["price"] == expected.price_decimal().to_string());
                    ensure!(actual["size"] == expected.size_decimal().to_string());
                }
            }
            let generation = payload["hash"]
                .as_str()
                .and_then(|hash| hash.strip_prefix(&format!("exact-{token}-")))
                .context("exact book hash prefix")?
                .parse::<u64>()?;
            ensure!(generation >= self.generation, "manual generation regressed");
            let timestamp = payload["timestamp"]
                .as_str()
                .context("exact book timestamp")?
                .parse::<u128>()?;
            Ok((generation, timestamp))
        }
    }

    #[tokio::test]
    async fn discovery_precedes_delivery() -> Result<()> {
        let server = DeterministicClobServer::start().await?;
        let refresh = server.refresh_handle();
        let (mut socket, _) = connect_async(server.base_url()).await?;
        let expected = (1..=20)
            .map(|index| TokenId::new(format!("exact-cohort-{index}")))
            .collect::<Vec<_>>();
        let wrong = (1..=20)
            .map(|index| format!("old-cohort-{index}"))
            .collect::<Vec<_>>();
        socket
            .send(Message::Text(
                json!({"assets_ids": wrong}).to_string().into(),
            ))
            .await?;
        server
            .wait_for_subscriptions(20, Duration::from_secs(1))
            .await?;

        let readiness = refresh.wait_for_token_owners(&expected, Duration::from_secs(6));
        tokio::pin!(readiness);
        let delivery_budget = Duration::from_millis(FixtureBookTiming::DELIVERY_BUDGET_MS);
        let delayed_subscription = delivery_budget + Duration::from_millis(100);
        ensure!(
            timeout_at(Instant::now() + delayed_subscription, &mut readiness)
                .await
                .is_err(),
            "a same-sized cohort of wrong token identities must not become ready"
        );
        socket
            .send(Message::Text(
                json!({"assets_ids": wrong, "operation": "unsubscribe"})
                    .to_string()
                    .into(),
            ))
            .await?;
        let tokens = expected.iter().map(TokenId::as_str).collect::<Vec<_>>();
        socket
            .send(Message::Text(
                json!({"assets_ids": tokens}).to_string().into(),
            ))
            .await?;
        readiness.await?;
        let ready_at = Utc::now();

        let book = ExactBookSnapshot::readiness_fixture();
        let deadline = Instant::now() + delivery_budget;
        let sent_at = refresh
            .send_snapshot(
                &expected[0],
                &book.bids,
                &book.asks,
                book.generation,
                deadline,
            )
            .await?;
        let wire_timestamp = book
            .receive_exact(&mut socket, &expected[0], deadline)
            .await?;
        ensure!(
            sent_at >= ready_at,
            "wire time included subscription discovery"
        );
        ensure!(wire_timestamp == u128::try_from(sent_at.timestamp_millis())?);
        socket.close(None).await?;
        server.shutdown().await
    }

    #[tokio::test]
    async fn disconnected_owner_fails_closed() -> Result<()> {
        let server = DeterministicClobServer::start().await?;
        let refresh = server.refresh_handle();
        let token = TokenId::new("disconnected-exact-token");
        for invalid in [Vec::new(), vec![token.clone(), token.clone()]] {
            ensure!(
                refresh
                    .wait_for_token_owners(&invalid, Duration::from_secs(1))
                    .await
                    .is_err(),
                "empty or duplicate token cohorts must not be accepted"
            );
        }
        let (mut socket, _) = connect_async(server.base_url()).await?;
        socket
            .send(Message::Text(
                json!({"assets_ids": [token.as_str()]}).to_string().into(),
            ))
            .await?;
        refresh
            .wait_for_token_owners(slice::from_ref(&token), Duration::from_secs(1))
            .await?;
        socket.close(None).await?;
        server
            .wait_for_active_connections(0, Duration::from_secs(1))
            .await?;

        let readiness_error = refresh
            .wait_for_token_owners(slice::from_ref(&token), Duration::from_millis(100))
            .await
            .expect_err("disconnected owners cannot prove readiness");
        ensure!(format!("{readiness_error:#}").contains("phase=discovery"));
        let book = ExactBookSnapshot::readiness_fixture();
        let deadline =
            Instant::now() + Duration::from_millis(FixtureBookTiming::DELIVERY_BUDGET_MS);
        let error = timeout_at(
            Instant::now() + Duration::from_millis(100),
            refresh.send_snapshot(&token, &book.bids, &book.asks, book.generation, deadline),
        )
        .await
        .context("send must not wait for subscription discovery")?
        .expect_err("a disconnected owner must reject an exact send");
        let error = format!("{error:#}");
        ensure!(error.contains(token.as_str()) && error.contains("phase=send"));
        ensure!(
            !refresh
                .state
                .exact_books
                .read()
                .await
                .contains_key(token.as_str())
        );
        server.shutdown().await
    }

    #[tokio::test]
    async fn outbox_respects_delivery_deadline() -> Result<()> {
        let server = DeterministicClobServer::start().await?;
        let refresh = server.refresh_handle();
        let token = TokenId::new("blocked-exact-token");
        let (mut socket, _) = connect_async(server.base_url()).await?;
        socket
            .send(Message::Text(
                json!({"assets_ids": [token.as_str()]}).to_string().into(),
            ))
            .await?;
        refresh
            .wait_for_token_owners(slice::from_ref(&token), Duration::from_secs(1))
            .await?;
        timeout_at(Instant::now() + Duration::from_secs(1), socket.next())
            .await?
            .context("initial subscription snapshot")??;
        let owner = *server
            .state
            .token_owner
            .read()
            .await
            .get(token.as_str())
            .context("live token owner")?;
        let connection = server
            .state
            .connections
            .read()
            .await
            .get(&owner)
            .map(Arc::clone)
            .context("live connection")?;
        let reserved = timeout_at(
            Instant::now() + Duration::from_secs(1),
            connection.tx.reserve_many(CONNECTION_OUTBOX_CAPACITY),
        )
        .await??;
        let readiness_error = refresh
            .wait_for_token_owners(slice::from_ref(&token), Duration::from_millis(100))
            .await
            .expect_err("a full owner outbox cannot prove send readiness");
        ensure!(format!("{readiness_error:#}").contains("unavailable_count=1"));
        let book = ExactBookSnapshot::readiness_fixture();
        let budget = Duration::from_millis(FixtureBookTiming::DELIVERY_BUDGET_MS);
        let deadline = Instant::now() + budget;
        let error = refresh
            .send_snapshot(&token, &book.bids, &book.asks, book.generation, deadline)
            .await
            .expect_err("a blocked outbox must respect the event deadline");
        ensure!(Instant::now() >= deadline);
        ensure!(format!("{error:#}").contains("phase=send exceeded delivery deadline"));
        ensure!(
            !refresh
                .state
                .exact_books
                .read()
                .await
                .contains_key(token.as_str())
        );
        drop(reserved);

        refresh
            .wait_for_token_owners(slice::from_ref(&token), Duration::from_secs(1))
            .await?;
        let deadline = Instant::now() + budget;
        let sent_at = refresh
            .send_snapshot(&token, &book.bids, &book.asks, book.generation, deadline)
            .await?;
        let wire_timestamp = book.receive_exact(&mut socket, &token, deadline).await?;
        ensure!(wire_timestamp == u128::try_from(sent_at.timestamp_millis())?);
        socket.close(None).await?;
        server.shutdown().await
    }

    #[tokio::test]
    async fn keepalive_refreshes_books() -> Result<()> {
        let server = DeterministicClobServer::start_keepalive(Duration::from_millis(10)).await?;
        let (mut socket, _) = connect_async(server.base_url())
            .await
            .context("connect deterministic CLOB test client")?;
        socket
            .send(Message::Text(
                json!({"assets_ids": ["production-stack-token-a", "production-stack-token-b"]})
                    .to_string()
                    .into(),
            ))
            .await
            .context("subscribe deterministic CLOB test client")?;

        let deadline = Instant::now() + Duration::from_secs(1);
        let mut refreshed = BTreeMap::new();
        while refreshed.len() < 2 {
            let frame = timeout_at(deadline, socket.next())
                .await
                .context("wait for deterministic CLOB keepalive")?
                .context("deterministic CLOB socket closed before keepalive")?
                .context("read deterministic CLOB keepalive frame")?;
            let Message::Text(text) = frame else {
                continue;
            };
            let payload: Value = serde_json::from_str(text.as_ref())?;
            if payload["hash"]
                .as_str()
                .is_some_and(|hash| hash.starts_with("keepalive-"))
            {
                let token = payload["asset_id"]
                    .as_str()
                    .context("keepalive frame omitted asset_id")?;
                let generation = payload["hash"]
                    .as_str()
                    .and_then(|hash| hash.rsplit_once('-'))
                    .map(|(_, generation)| generation.to_owned())
                    .context("keepalive frame omitted its generation")?;
                refreshed.insert(token.to_owned(), generation);
            }
        }
        ensure!(
            refreshed.keys().cloned().collect::<Vec<_>>()
                == [
                    "production-stack-token-a".to_owned(),
                    "production-stack-token-b".to_owned(),
                ]
                && refreshed.values().all(|generation| {
                    generation == refreshed.values().next().expect("generation")
                }),
            "keepalive did not refresh every subscribed book: {refreshed:?}"
        );
        ensure!(
            server.active_connection_count() == 1
                && server.connection_high_water() == 1
                && server.accepted_connection_count() == 1,
            "single fixture client must own exactly one bounded connection"
        );

        socket.close(None).await?;
        server
            .wait_for_active_connections(0, Duration::from_secs(1))
            .await?;
        server.shutdown().await
    }

    #[tokio::test]
    async fn resumed_keepalive_preserves_exact() -> Result<()> {
        let server = DeterministicClobServer::start_keepalive(Duration::from_millis(20)).await?;
        let refresh = server.refresh_handle();
        refresh.pause_keepalive();
        let books = BTreeMap::from([
            (
                "exact-keepalive-a",
                ExactBookSnapshot {
                    bids: vec![BookLevel::from_decimal_unchecked(
                        Price::new(dec!(0.42)),
                        Shares::new(dec!(12)),
                    )],
                    asks: vec![BookLevel::from_decimal_unchecked(
                        Price::new(dec!(0.44)),
                        Shares::new(dec!(14)),
                    )],
                    generation: 90_000,
                },
            ),
            (
                "exact-keepalive-b",
                ExactBookSnapshot {
                    bids: vec![BookLevel::from_decimal_unchecked(
                        Price::new(dec!(0.61)),
                        Shares::new(dec!(23)),
                    )],
                    asks: vec![BookLevel::from_decimal_unchecked(
                        Price::new(dec!(0.65)),
                        Shares::new(dec!(27)),
                    )],
                    generation: 120_000,
                },
            ),
        ]);
        let tokens = books.keys().copied().collect::<Vec<_>>();
        let mut previous = BTreeMap::<String, (u64, u128)>::new();
        for connection_index in 0..2 {
            let not_before = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
            let (mut socket, _) = connect_async(server.base_url()).await?;
            socket
                .send(Message::Text(
                    json!({"assets_ids": tokens}).to_string().into(),
                ))
                .await?;
            server
                .wait_for_subscriptions(2, Duration::from_secs(1))
                .await?;
            let deadline = Instant::now() + Duration::from_secs(1);
            if connection_index == 0 {
                for _ in 0..2 {
                    let frame = timeout_at(deadline, socket.next())
                        .await?
                        .context("initial generic book")??;
                    let Message::Text(text) = frame else {
                        continue;
                    };
                    let payload: Value = serde_json::from_str(text.as_ref())?;
                    ensure!(
                        payload["hash"]
                            .as_str()
                            .is_some_and(|hash| hash.starts_with("snapshot-"))
                    );
                }
                for (token, book) in &books {
                    refresh
                        .send_snapshot(
                            &TokenId::new(*token),
                            &book.bids,
                            &book.asks,
                            book.generation,
                            deadline,
                        )
                        .await?;
                }
                refresh.resume_keepalive();
            }
            let mut counts = BTreeMap::from([(tokens[0], 0_u64), (tokens[1], 0_u64)]);
            let mut first_timestamps = BTreeMap::new();
            while counts.values().any(|count| *count < 4) {
                let frame = timeout_at(deadline, socket.next())
                    .await?
                    .context("exact keepalive frame")??;
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value = serde_json::from_str(text.as_ref())?;
                let token = payload["asset_id"].as_str().context("exact token")?;
                let book = books.get(token).context("unexpected exact token")?;
                let (generation, timestamp) = book.verify_frame(token, &payload)?;
                let received_at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
                ensure!(
                    timestamp >= not_before && timestamp <= received_at,
                    "exact pulse is not current-time"
                );
                let advanced = if let Some((last_generation, last_timestamp)) = previous.get(token)
                {
                    ensure!(generation >= *last_generation && timestamp >= *last_timestamp);
                    generation > *last_generation
                } else {
                    true
                };
                first_timestamps
                    .entry(token.to_owned())
                    .or_insert(timestamp);
                previous.insert(token.to_owned(), (generation, timestamp));
                *counts.get_mut(token).context("exact token counter")? += u64::from(advanced);
            }
            for (token, first_timestamp) in first_timestamps {
                let (generation, timestamp) =
                    previous.get(&token).context("observed exact pulse")?;
                ensure!(
                    *timestamp > first_timestamp,
                    "periodic exact timestamp did not advance"
                );
                ensure!(*generation >= books[token.as_str()].generation + 3);
            }
            ensure!(server.active_connection_count() == 1 && server.connection_high_water() == 1);
            ensure!(server.accepted_connection_count() == connection_index + 1);
            socket.close(None).await?;
            server
                .wait_for_active_connections(0, Duration::from_secs(1))
                .await?;
        }
        server.shutdown().await
    }

    #[tokio::test]
    async fn snapshot_survives_reconnect() -> Result<()> {
        let server = DeterministicClobServer::start().await?;
        let token = TokenId::new("production-stack-reconnect-token");
        let (mut first_socket, _) = connect_async(server.base_url()).await?;
        first_socket
            .send(Message::Text(
                json!({"assets_ids": [token.as_str()]}).to_string().into(),
            ))
            .await?;
        server
            .wait_for_subscriptions(1, Duration::from_secs(1))
            .await?;
        let refresh = server.refresh_handle();
        refresh
            .send_snapshot(
                &token,
                &[BookLevel::from_decimal_unchecked(
                    Price::new(dec!(0.42)),
                    Shares::new(dec!(12)),
                )],
                &[BookLevel::from_decimal_unchecked(
                    Price::new(dec!(0.44)),
                    Shares::new(dec!(14)),
                )],
                7,
                Instant::now() + Duration::from_secs(1),
            )
            .await?;
        first_socket.close(None).await?;
        server
            .wait_for_active_connections(0, Duration::from_secs(1))
            .await?;
        let refresh_token = token.clone();
        let readiness = tokio::spawn(async move {
            refresh
                .wait_for_token_owners(&[refresh_token], Duration::from_secs(1))
                .await
        });
        sleep(Duration::from_millis(50)).await;
        ensure!(
            !readiness.is_finished(),
            "owner readiness did not await reconnect"
        );

        let (mut second_socket, _) = connect_async(server.base_url()).await?;
        second_socket
            .send(Message::Text(
                json!({"assets_ids": [token.as_str()]}).to_string().into(),
            ))
            .await?;
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let frame = timeout_at(deadline, second_socket.next())
                .await
                .context("wait for exact snapshot after reconnect")?
                .context("reconnected deterministic CLOB socket closed")??;
            let Message::Text(text) = frame else {
                continue;
            };
            let payload: Value = serde_json::from_str(text.as_ref())?;
            if payload["hash"] == "exact-production-stack-reconnect-token-7" {
                ensure!(payload["bids"][0]["price"] == "0.42");
                ensure!(payload["asks"][0]["price"] == "0.44");
                break;
            }
        }
        readiness
            .await
            .context("join exact reconnect readiness")??;
        second_socket.close(None).await?;
        server
            .wait_for_active_connections(0, Duration::from_secs(1))
            .await?;
        server.shutdown().await
    }
}
