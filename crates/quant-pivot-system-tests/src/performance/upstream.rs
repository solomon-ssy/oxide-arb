use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
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
    time::{Instant, sleep, timeout},
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
const SNAPSHOT_SUBSCRIPTION_WAIT: Duration = Duration::from_secs(10);
const SNAPSHOT_SUBSCRIPTION_POLL: Duration = Duration::from_millis(25);

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
    /// Stop generic keepalive books before publishing exact report fixtures.
    pub fn pause_keepalive(&self) {
        self.state.keepalive_enabled.store(false, Ordering::Release);
    }

    /// Send one exact current-time book snapshot through the real WS ingress.
    pub async fn send_snapshot(
        &self,
        token: &TokenId,
        bids: &[BookLevel],
        asks: &[BookLevel],
        generation: u64,
    ) -> Result<()> {
        self.state.exact_books.write().await.insert(
            token.as_str().to_owned(),
            ExactBookSnapshot {
                bids: bids.to_vec(),
                asks: asks.to_vec(),
                generation,
            },
        );
        timeout(SNAPSHOT_SUBSCRIPTION_WAIT, async {
            loop {
                let connection_id = self
                    .state
                    .token_owner
                    .read()
                    .await
                    .get(token.as_str())
                    .copied();
                let connection = match connection_id {
                    Some(connection_id) => self
                        .state
                        .connections
                        .read()
                        .await
                        .get(&connection_id)
                        .cloned(),
                    None => None,
                };
                if let Some(connection) = connection
                    && connection
                        .tx
                        .send(exact_snapshot_payload(token, bids, asks, generation).to_string())
                        .await
                        .is_ok()
                {
                    return;
                }
                sleep(SNAPSHOT_SUBSCRIPTION_POLL).await;
            }
        })
        .await
        .with_context(|| format!("wait to refresh deterministic CLOB token {token}"))?;
        Ok(())
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
        let Some(token) = usize::try_from(generation)
            .ok()
            .and_then(|generation| tokens.get(generation % tokens.len()))
        else {
            continue;
        };
        let payload = live_snapshot_payload(token, generation).to_string();
        // A normal reconnect can retire the copied connection between the
        // state read and this send. Its handler owns cleanup; the next pulse
        // observes only the replacement connection.
        let _ = connection.tx.send(payload).await;
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
            let exact = state.exact_books.read().await.get(&token).cloned();
            let payload = exact.map_or_else(
                || snapshot_payload(&token, connection_id),
                |snapshot| {
                    exact_snapshot_payload(
                        &TokenId::new(&token),
                        &snapshot.bids,
                        &snapshot.asks,
                        snapshot.generation,
                    )
                },
            );
            connection
                .tx
                .send(payload.to_string())
                .await
                .context("enqueue deterministic CLOB initial snapshot")?;
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
) -> Value {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_millis());
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
    use std::{collections::BTreeSet, time::Duration};

    use anyhow::{Context, Result, ensure};
    use futures_util::{SinkExt, StreamExt};
    use quant_pivot_models::{
        domain::market::BookLevel,
        types::{Price, Shares, TokenId},
    };
    use rust_decimal_macros::dec;
    use serde_json::{Value, json};
    use tokio::time::{Instant, sleep, timeout_at};
    use tokio_tungstenite::{connect_async, tungstenite::Message};

    use super::DeterministicClobServer;

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
        let mut refreshed = BTreeSet::new();
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
                refreshed.insert(token.to_owned());
            }
        }
        ensure!(
            refreshed
                == BTreeSet::from([
                    "production-stack-token-a".to_owned(),
                    "production-stack-token-b".to_owned(),
                ]),
            "keepalive did not refresh every subscribed book: {refreshed:?}"
        );
        ensure!(
            server.active_connection_count() == 1 && server.connection_high_water() == 1,
            "single fixture client must own exactly one bounded connection"
        );

        socket.close(None).await?;
        server
            .wait_for_active_connections(0, Duration::from_secs(1))
            .await?;
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
        first_socket.close(None).await?;
        server
            .wait_for_active_connections(0, Duration::from_secs(1))
            .await?;

        let refresh = server.refresh_handle();
        let refresh_token = token.clone();
        let send = tokio::spawn(async move {
            refresh
                .send_snapshot(
                    &refresh_token,
                    &[BookLevel::from_decimal_unchecked(
                        Price::new(dec!(0.42)),
                        Shares::new(dec!(12)),
                    )],
                    &[BookLevel::from_decimal_unchecked(
                        Price::new(dec!(0.44)),
                        Shares::new(dec!(14)),
                    )],
                    7,
                )
                .await
        });
        sleep(Duration::from_millis(50)).await;
        ensure!(!send.is_finished(), "exact refresh did not await reconnect");

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
        send.await.context("join exact reconnect refresh")??;
        second_socket.close(None).await?;
        server
            .wait_for_active_connections(0, Duration::from_secs(1))
            .await?;
        server.shutdown().await
    }
}
