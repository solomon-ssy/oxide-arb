//! Per-connection WebSocket session loop.
//!
//! Owns the subscription set, drains the broadcaster's outbound queue into the
//! socket, processes client commands (`subscribe` / `unsubscribe` / `sync` /
//! `ping`), and enforces the heartbeat (server ping every 15s; disconnect after
//! 30s without a pong). Registers in the [`SessionRegistry`] for the lifetime of
//! the connection and deregisters on exit.

use actix_web::web;
use actix_ws::{Message, MessageStream, Session};
use chrono::{Duration as ChronoDuration, Utc};
use futures_util::StreamExt;
use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use oxide_arb_models::{
    domain::{
        ClientCommand, LivePnlView, MarketFilter, PageRequest, PositionView, RiskEngineStateView,
        SubscriptionKey, SyncSnapshot, TimeWindow, WsChannel, WsEnvelope,
    },
    enums::rbac::{Operation, ResourceType},
};

use crate::{
    state::AppState,
    ws::{SessionHandle, SessionRegistry},
};

/// Server ping cadence.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
/// Disconnect threshold without a client pong.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(30);
/// Per-session outbound queue capacity.
const OUTBOUND_CAPACITY: usize = 256;
/// Look-back for the `sync` recent-opportunities section.
const SYNC_RECENT_OPPS_HOURS: i64 = 24;
/// Maximum recent opportunities included in a `sync` snapshot.
const SYNC_RECENT_OPPS_LIMIT: u64 = 50;

/// Shared context handed to a session task.
pub struct SessionContext {
    pub state: web::Data<AppState>,
    pub registry: SessionRegistry,
    /// Authenticated subject (stable user id) used for per-channel authorization.
    pub user_id: String,
}

impl SessionContext {
    /// Whether the session may read `resource` (Casbin `Read`), mirroring the
    /// HTTP `resource_op` check so a WebSocket cannot bypass authorization.
    async fn can_read(&self, resource: ResourceType) -> bool {
        self.state
            .casbin
            .enforce(&self.user_id, resource.as_str(), Operation::Read.as_str())
            .await
            .unwrap_or(false)
    }
}

/// Run a single WebSocket session until it closes or times out.
pub async fn run(mut session: Session, mut msg_stream: MessageStream, ctx: SessionContext) {
    let subscriptions = Arc::new(RwLock::new(HashSet::<SubscriptionKey>::new()));
    let (outbound_tx, outbound_rx) = flume::bounded::<String>(OUTBOUND_CAPACITY);
    let session_id = ctx.registry.register(SessionHandle {
        outbound: outbound_tx,
        subscriptions: Arc::clone(&subscriptions),
    });

    // Push the connection snapshot immediately after auth (authorized readers).
    if ctx.can_read(ResourceType::System).await {
        let status = ctx.state.control.system_status().await;
        if let Ok(data) = serde_json::to_value(&status) {
            let _ = session
                .text(WsEnvelope::channel(WsChannel::SystemStatus, data).to_text())
                .await;
        }
    }

    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_pong = Instant::now();

    loop {
        tokio::select! {
            outbound = outbound_rx.recv_async() => {
                match outbound {
                    Ok(text) => {
                        if session.text(text).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            incoming = msg_stream.next() => {
                let Some(Ok(message)) = incoming else { break };
                match message {
                    Message::Text(text) => {
                        if let Some(reply) = handle_command(&ctx, &subscriptions, &text).await {
                            if session.text(reply).await.is_err() {
                                break;
                            }
                        }
                    }
                    Message::Ping(bytes) => {
                        if session.pong(&bytes).await.is_err() {
                            break;
                        }
                    }
                    Message::Pong(_) => last_pong = Instant::now(),
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            _ = heartbeat.tick() => {
                if last_pong.elapsed() > CLIENT_TIMEOUT {
                    tracing::debug!(session_id, "ws session timed out (no pong)");
                    break;
                }
                if session.ping(b"").await.is_err() {
                    break;
                }
            }
        }
    }

    ctx.registry.deregister(session_id);
    let _ = session.close(None).await;
}

/// Process one client command; returns an optional immediate reply.
///
/// A malformed command (bad JSON, unknown `action`, or unknown channel) is
/// answered with a structured `error` frame rather than silently dropped, so
/// strong typing of [`ClientCommand`] never costs client feedback.
async fn handle_command(
    ctx: &SessionContext,
    subscriptions: &Arc<RwLock<HashSet<SubscriptionKey>>>,
    raw: &str,
) -> Option<String> {
    let command: ClientCommand = match serde_json::from_str(raw) {
        Ok(command) => command,
        Err(err) => {
            return Some(
                WsEnvelope::error(
                    serde_json::json!({ "error": "invalid_command", "detail": err.to_string() }),
                )
                .to_text(),
            );
        }
    };
    match command {
        ClientCommand::Subscribe { channel, market_id } => {
            // Sensitive channels require the same read permission as their HTTP
            // route, so a WebSocket subscription cannot bypass authorization.
            if !ctx.can_read(channel.resource()).await {
                return Some(
                    WsEnvelope::error(
                        serde_json::json!({ "error": "forbidden", "channel": channel.as_str() }),
                    )
                    .to_text(),
                );
            }
            if let Ok(mut set) = subscriptions.write() {
                set.insert(SubscriptionKey::new(channel, market_id));
            }
            None
        }
        ClientCommand::Unsubscribe { channel, market_id } => {
            if let Ok(mut set) = subscriptions.write() {
                set.remove(&SubscriptionKey::new(channel, market_id));
            }
            None
        }
        ClientCommand::Sync => Some(sync_snapshot(ctx).await),
        ClientCommand::Ping => Some(WsEnvelope::pong().to_text()),
    }
}

/// Build the full-state snapshot for a `sync` command, including only the
/// sections the session is authorized to read. Every section is projected
/// through the same outbound `*View` types as its HTTP counterpart, so a `sync`
/// can never leak internal columns the REST routes strip.
async fn sync_snapshot(ctx: &SessionContext) -> String {
    let mut snapshot = SyncSnapshot::default();
    if ctx.can_read(ResourceType::System).await {
        snapshot.system_status = Some(ctx.state.control.system_status().await);
    }
    if ctx.can_read(ResourceType::Risk).await {
        let open_positions: Vec<PositionView> = ctx
            .state
            .positions
            .find_open()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(PositionView::from)
            .collect();
        snapshot.risk = Some(RiskEngineStateView::from(ctx.state.control.risk_snapshot()));
        snapshot.open_positions = Some(open_positions);
    }
    if ctx.can_read(ResourceType::Pnl).await {
        snapshot.pnl = Some(LivePnlView::from(&ctx.state.control.risk_snapshot()));
    }
    if ctx.can_read(ResourceType::Opportunity).await {
        let window = TimeWindow::new(
            Utc::now() - ChronoDuration::hours(SYNC_RECENT_OPPS_HOURS),
            Utc::now(),
        );
        let recent = ctx
            .state
            .evidence
            .detections_page(
                MarketFilter::default(),
                window,
                PageRequest::new(1, SYNC_RECENT_OPPS_LIMIT),
            )
            .await
            .map(|page| page.items)
            .unwrap_or_default();
        snapshot.recent_opportunities = Some(recent);
    }
    let data = serde_json::to_value(&snapshot).unwrap_or_else(|_| serde_json::json!({}));
    WsEnvelope::sync(data).to_text()
}
