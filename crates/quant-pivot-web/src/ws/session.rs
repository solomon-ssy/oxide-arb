//! Per-connection WebSocket session loop (Phase 0).

use actix_web::web;
use actix_ws::{Message, MessageStream, Session};
use futures_util::StreamExt;
use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use quant_pivot_models::{
    domain::{ClientCommand, SubscriptionKey, SyncSnapshot, WsChannel, WsEnvelope},
    enums::rbac::{Operation, ResourceType},
};

use crate::{
    state::AppState,
    ws::{SessionHandle, SessionRegistry},
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(30);
const OUTBOUND_CAPACITY: usize = 256;

pub struct SessionContext {
    pub state: web::Data<AppState>,
    pub registry: SessionRegistry,
    pub user_id: String,
}

impl SessionContext {
    async fn can_read(&self, resource: ResourceType) -> bool {
        self.state
            .casbin
            .enforce(&self.user_id, resource.as_str(), Operation::Read.as_str())
            .await
            .unwrap_or(false)
    }
}

pub async fn run(mut session: Session, mut msg_stream: MessageStream, ctx: SessionContext) {
    let subscriptions = Arc::new(RwLock::new(HashSet::<SubscriptionKey>::new()));
    let (outbound_tx, outbound_rx) = flume::bounded::<String>(OUTBOUND_CAPACITY);
    let session_id = ctx.registry.register(SessionHandle {
        outbound: outbound_tx,
        subscriptions: Arc::clone(&subscriptions),
    });

    if ctx.can_read(ResourceType::System).await {
        let status = ctx.state.control.system_status();
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
                        if let Some(reply) = handle_command(&ctx, &subscriptions, &text).await
                            && session.text(reply).await.is_err()
                        {
                            break;
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

async fn sync_snapshot(ctx: &SessionContext) -> String {
    let mut snapshot = SyncSnapshot::default();
    if ctx.can_read(ResourceType::System).await {
        snapshot.system_status = Some(ctx.state.control.system_status());
    }
    let data = serde_json::to_value(&snapshot).unwrap_or_else(|_| serde_json::json!({}));
    WsEnvelope::sync(data).to_text()
}
