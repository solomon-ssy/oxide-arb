//! Per-connection WebSocket session loop.

use std::time::{Duration, Instant};

use actix_web::web::Data;
use actix_ws::{Message, MessageStream, Session};
use bytestring::ByteString;
use futures_util::StreamExt;
use quant_pivot_models::{
    domain::{
        api::SystemStatusView,
        ws::{ClientCommand, SubscriptionKey, SyncSnapshot, WsChannel, WsEnvelope},
    },
    enums::rbac::{Operation, ResourceType, UserStatus},
    types::UserId,
};
use tokio::{sync::mpsc, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::{
    state::AppState,
    ws::{SessionId, SessionRegistration, SessionRegistry, SharedFrame},
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(30);
const OUTBOUND_CAPACITY: usize = 256;

pub struct SessionContext {
    pub state: Data<AppState>,
    pub registry: SessionRegistry,
    pub subject_id: UserId,
    pub user_id: String,
    pub family_id: String,
    pub access_jti: String,
    pub authorization_revision: u64,
    pub can_read_system: bool,
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
    let (outbound, mut outbound_rx) = mpsc::channel::<SharedFrame>(OUTBOUND_CAPACITY);
    let cancellation = CancellationToken::new();
    let hub_fail_closed = ctx.registry.fail_closed_token();
    let Some(session_id) = ctx
        .registry
        .register(SessionRegistration {
            outbound,
            subject: ctx.subject_id,
            family_id: ctx.family_id.clone(),
            can_read_system: ctx.can_read_system,
            cancellation: cancellation.clone(),
        })
        .await
    else {
        let _ = session.close(None).await;
        return;
    };

    if ctx.can_read_system {
        let status = control_plane_status(&ctx);
        if let Ok(data) = serde_json::to_value(&status) {
            let _ = session
                .text(frame(&WsEnvelope::channel(WsChannel::SystemStatus, data)))
                .await;
        }
    }

    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_pong = Instant::now();

    loop {
        tokio::select! {
            () = hub_fail_closed.cancelled() => break,
            () = cancellation.cancelled() => break,
            outbound = outbound_rx.recv() => {
                match outbound {
                    Some(text) => {
                        if session.text(text.text().clone()).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
            incoming = msg_stream.next() => {
                let Some(Ok(message)) = incoming else { break };
                match message {
                    Message::Text(text) => {
                        if let Some(reply) = handle_command(&ctx, session_id, &text).await
                            && session.text(reply).await.is_err()
                        {
                            break;
                        }
                    }
                    Message::Ping(bytes) if session.pong(&bytes).await.is_err() => break,
                    Message::Pong(_) => last_pong = Instant::now(),
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            _ = heartbeat.tick() => {
                if last_pong.elapsed() > CLIENT_TIMEOUT {
                    tracing::debug!(?session_id, "ws session timed out (no pong)");
                    break;
                }
                if !session_identity_active(&ctx).await {
                    tracing::debug!(?session_id, "ws session identity is no longer active");
                    break;
                }
                if session.ping(b"").await.is_err() {
                    break;
                }
            }
        }
    }

    ctx.registry.deregister(session_id).await;
    let _ = session.close(None).await;
}

async fn session_identity_active(ctx: &SessionContext) -> bool {
    if !ctx.state.casbin.is_healthy()
        || ctx.state.casbin.authorization_revision() != ctx.authorization_revision
        || ctx
            .state
            .jwt
            .is_revoked(&ctx.access_jti)
            .await
            .unwrap_or(true)
        || !ctx
            .state
            .jwt
            .family_active(&ctx.family_id)
            .await
            .unwrap_or(false)
    {
        return false;
    }
    let Ok(user_id) = ctx.user_id.parse() else {
        return false;
    };
    ctx.state
        .users
        .find_by_id(&user_id)
        .await
        .is_ok_and(|user| user.status == UserStatus::Active)
}

async fn handle_command(
    ctx: &SessionContext,
    session_id: SessionId,
    raw: &str,
) -> Option<ByteString> {
    let command: ClientCommand = match serde_json::from_str(raw) {
        Ok(command) => command,
        Err(err) => {
            return Some(frame(&WsEnvelope::error(
                serde_json::json!({ "error": "invalid_command", "detail": err.to_string() }),
            )));
        }
    };
    match command {
        ClientCommand::Subscribe { channel, market_id } => {
            if !ctx.can_read(channel.resource()).await {
                return Some(frame(&WsEnvelope::error(
                    serde_json::json!({ "error": "forbidden", "channel": channel.as_str() }),
                )));
            }
            if !ctx
                .registry
                .subscribe(session_id, SubscriptionKey::new(channel, market_id))
                .await
            {
                return Some(frame(&WsEnvelope::error(
                    serde_json::json!({ "error": "session_unavailable" }),
                )));
            }
            None
        }
        ClientCommand::Unsubscribe { channel, market_id } => {
            ctx.registry
                .unsubscribe(session_id, SubscriptionKey::new(channel, market_id))
                .await;
            None
        }
        ClientCommand::Sync => Some(sync_snapshot(ctx).await),
        ClientCommand::Ping => Some(frame(&WsEnvelope::pong())),
    }
}

async fn sync_snapshot(ctx: &SessionContext) -> ByteString {
    let mut snapshot = SyncSnapshot::default();
    if ctx.can_read(ResourceType::System).await {
        snapshot.system_status = Some(control_plane_status(ctx));
    }
    let data = serde_json::to_value(&snapshot).unwrap_or_else(|_| serde_json::json!({}));
    frame(&WsEnvelope::sync(data))
}

fn frame(envelope: &WsEnvelope) -> ByteString {
    ByteString::from(envelope.to_text())
}

fn control_plane_status(ctx: &SessionContext) -> SystemStatusView {
    SystemStatusView {
        runtime: ctx.state.control.system_status(),
        bootstrap: ctx.state.bootstrap.view(),
        capabilities: ctx.state.bootstrap.capability_snapshot(),
    }
}
