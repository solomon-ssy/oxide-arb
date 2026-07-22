use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use quant_pivot_api::gamma::GammaClient;
use quant_pivot_models::{
    config::GammaConfig,
    domain::market::{EventRegistryInfo, MarketRegistryInfo},
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
    task::JoinHandle,
    time::{Instant, timeout},
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

struct WsState {
    connections: RwLock<HashMap<u64, Arc<ConnectionState>>>,
    token_owner: RwLock<HashMap<String, u64>>,
    subscription_changed: Notify,
    next_connection_id: AtomicU64,
}

pub struct DeterministicClobServer {
    address: SocketAddr,
    state: Arc<WsState>,
    shutdown: CancellationToken,
    accept_task: JoinHandle<()>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DeliveryStats {
    pub events: u64,
    pub encoded_bytes: u64,
}

impl DeterministicClobServer {
    pub async fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("bind deterministic CLOB WebSocket upstream")?;
        let address = listener
            .local_addr()
            .context("read deterministic CLOB WebSocket address")?;
        let state = Arc::new(WsState {
            connections: RwLock::new(HashMap::new()),
            token_owner: RwLock::new(HashMap::new()),
            subscription_changed: Notify::new(),
            next_connection_id: AtomicU64::new(0),
        });
        let shutdown = CancellationToken::new();
        let accept_state = Arc::clone(&state);
        let accept_shutdown = shutdown.clone();
        let accept_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = accept_shutdown.cancelled() => break,
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else {
                            break;
                        };
                        let connection_state = Arc::clone(&accept_state);
                        let connection_shutdown = accept_shutdown.clone();
                        tokio::spawn(async move {
                            if let Err(error) = handle_connection(
                                stream,
                                connection_state,
                                connection_shutdown,
                            ).await {
                                tracing::debug!(%error, "deterministic CLOB connection closed");
                            }
                        });
                    }
                }
            }
        });
        Ok(Self {
            address,
            state,
            shutdown,
            accept_task,
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

    pub async fn shutdown(self) {
        self.shutdown.cancel();
        let _ = self.accept_task.await;
    }
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
                    Some(Err(error)) => return Err(error).context("read deterministic CLOB frame"),
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
    let tokens = connection.tokens.read().await.clone();
    let mut owners = state.token_owner.write().await;
    owners.retain(|token, owner| *owner != connection_id || !tokens.contains(token));
    drop(owners);
    state.connections.write().await.remove(&connection_id);
    state.subscription_changed.notify_waiters();
    Ok(())
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
            connection
                .tx
                .send(snapshot_payload(&token, connection_id).to_string())
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
