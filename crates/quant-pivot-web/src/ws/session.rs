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
    ws::{
        SESSION_REPLAY_CAPACITY, SessionId, SessionRegistration, SessionRegistry, SharedFrame,
        feedback::ResearchFeedbackFrame,
    },
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
        let status = ctx.control_plane_status();
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
                if !ctx.session_identity_active().await {
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

impl SessionContext {
    async fn session_identity_active(&self) -> bool {
        if !self.state.casbin.is_healthy()
            || self.state.casbin.authorization_revision() != self.authorization_revision
            || self
                .state
                .jwt
                .is_revoked(&self.access_jti)
                .await
                .unwrap_or(true)
            || !self
                .state
                .jwt
                .family_active(&self.family_id)
                .await
                .unwrap_or(false)
        {
            return false;
        }
        let Ok(user_id) = self.user_id.parse() else {
            return false;
        };
        self.state
            .users
            .find_by_id(&user_id)
            .await
            .is_ok_and(|user| user.status == UserStatus::Active)
    }
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
        ClientCommand::Subscribe {
            channel,
            market_id,
            after_revision,
        } => {
            if !ctx.can_read(channel.resource()).await {
                return Some(frame(&WsEnvelope::error(
                    serde_json::json!({ "error": "forbidden", "channel": channel.as_str() }),
                )));
            }
            let replay_cursor = match replay_cursor(channel, after_revision) {
                Ok(cursor) => cursor,
                Err(error) => {
                    return Some(frame(&WsEnvelope::error(
                        serde_json::json!({ "error": error, "channel": channel.as_str() }),
                    )));
                }
            };
            if !ctx
                .registry
                .subscribe(session_id, SubscriptionKey::new(channel, market_id))
                .await
            {
                return Some(frame(&WsEnvelope::error(
                    serde_json::json!({ "error": "session_unavailable" }),
                )));
            }
            if let Some(after_revision) = replay_cursor {
                return replay_feedback(ctx, session_id, after_revision).await;
            }
            None
        }
        ClientCommand::Unsubscribe { channel, market_id } => {
            ctx.registry
                .unsubscribe(session_id, SubscriptionKey::new(channel, market_id))
                .await;
            None
        }
        ClientCommand::Sync => Some((ctx).sync_snapshot().await),
        ClientCommand::Ping => Some(frame(&WsEnvelope::pong())),
    }
}

const fn replay_cursor(
    channel: WsChannel,
    after_revision: Option<i64>,
) -> Result<Option<i64>, &'static str> {
    match (channel, after_revision) {
        (WsChannel::ResearchFeedback, Some(revision)) if revision >= 0 => Ok(Some(revision)),
        (WsChannel::ResearchFeedback, Some(_)) => Err("invalid_after_revision"),
        (WsChannel::ResearchFeedback, None) => Err("missing_after_revision"),
        (_, Some(_)) => Err("unexpected_after_revision"),
        (_, None) => Ok(None),
    }
}

async fn replay_feedback(
    ctx: &SessionContext,
    session_id: SessionId,
    after_revision: i64,
) -> Option<ByteString> {
    let replay_limit = match u64::try_from(SESSION_REPLAY_CAPACITY.saturating_add(1)) {
        Ok(limit) => limit,
        Err(error) => {
            tracing::error!(%error, "feedback replay capacity does not fit repository limit");
            ctx.registry.close_session(session_id).await;
            return Some(frame(&WsEnvelope::error(
                serde_json::json!({ "error": "replay_unavailable" }),
            )));
        }
    };
    let entries = match ctx
        .state
        .feedback_outbox
        .list_outbox(after_revision, replay_limit)
        .await
    {
        Ok(entries) => entries,
        Err(error) => {
            tracing::warn!(%error, after_revision, "feedback replay query failed");
            ctx.registry.close_session(session_id).await;
            return Some(frame(&WsEnvelope::error(
                serde_json::json!({ "error": "replay_unavailable" }),
            )));
        }
    };
    if entries.len() > SESSION_REPLAY_CAPACITY {
        ctx.registry.close_session(session_id).await;
        return Some(frame(&WsEnvelope::error(serde_json::json!({
            "error": "replay_window_exceeded",
            "after_revision": after_revision,
        }))));
    }
    let frames = match entries
        .iter()
        .map(|entry| ResearchFeedbackFrame::try_from(entry).map(ByteString::from))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(frames) => frames,
        Err(error) => {
            tracing::error!(%error, after_revision, "feedback replay encoding failed");
            ctx.registry.close_session(session_id).await;
            return Some(frame(&WsEnvelope::error(
                serde_json::json!({ "error": "replay_unavailable" }),
            )));
        }
    };
    if ctx.registry.replay(session_id, frames).await {
        None
    } else {
        Some(frame(&WsEnvelope::error(
            serde_json::json!({ "error": "session_unavailable" }),
        )))
    }
}

impl SessionContext {
    async fn sync_snapshot(&self) -> ByteString {
        let mut snapshot = SyncSnapshot::default();
        if self.can_read(ResourceType::System).await {
            snapshot.system_status = Some((self).control_plane_status());
        }
        let data = serde_json::to_value(&snapshot).unwrap_or_else(|_| serde_json::json!({}));
        frame(&WsEnvelope::sync(data))
    }
}

fn frame(envelope: &WsEnvelope) -> ByteString {
    ByteString::from(envelope.to_text())
}

impl SessionContext {
    fn control_plane_status(&self) -> SystemStatusView {
        SystemStatusView {
            runtime: self.state.control.system_status(),
            capabilities: self.state.capabilities.capability_snapshot(),
        }
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::domain::ws::WsChannel;

    use super::replay_cursor;

    #[test]
    fn feedback_cursor_is_required() {
        assert_eq!(
            replay_cursor(WsChannel::ResearchFeedback, Some(42)),
            Ok(Some(42))
        );
        assert_eq!(
            replay_cursor(WsChannel::ResearchFeedback, None),
            Err("missing_after_revision")
        );
        assert_eq!(
            replay_cursor(WsChannel::ResearchFeedback, Some(-1)),
            Err("invalid_after_revision")
        );
    }

    #[test]
    fn other_channels_reject_cursor() {
        assert_eq!(replay_cursor(WsChannel::QuantReport, None), Ok(None));
        assert_eq!(
            replay_cursor(WsChannel::QuantReport, Some(1)),
            Err("unexpected_after_revision")
        );
    }
}
