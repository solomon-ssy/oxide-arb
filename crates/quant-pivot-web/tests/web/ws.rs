//! WebSocket integration tests (HTTP upgrade auth, framed commands, event fanout).

use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
    time::Duration,
};

use actix_http::{
    header::HeaderValue,
    ws::{Frame as WsFrame, Message as WsMessage, ProtocolError},
};
use actix_test::{TestServer, start as start_test_server};
use actix_web::{App, http::StatusCode, middleware::from_fn, test::TestRequest, web};
use futures_util::{SinkExt, StreamExt};
use quant_pivot_models::{
    domain::{CoreEvent, SubscriptionKey, SystemAlertEvent, WsChannel},
    enums::common::{AlertCategory, AlertLevel, AlertSource},
};
use quant_pivot_web::ws::SessionHandle;

use crate::{
    client,
    harness::{self, API_VERSION, TestEnv},
};

fn test_alert(key: &str, message: &str) -> CoreEvent {
    CoreEvent::Alert(SystemAlertEvent {
        idempotency_key: key.to_owned(),
        level: AlertLevel::Warning,
        category: AlertCategory::OperatorNotice,
        source: AlertSource::System,
        title: "Test alert".to_owned(),
        message: message.to_owned(),
        affects_trading: false,
        visible_toast: false,
        dedupe_secs: 60,
    })
}

const WS_KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";

fn ws_upgrade_request(uri: &str) -> TestRequest {
    ws_upgrade_request_with_api_version(uri, true)
}

/// Browser WebSocket handshakes cannot set `Accept-Api-Version`; regression guard.
fn ws_upgrade_request_without_api_version(uri: &str) -> TestRequest {
    ws_upgrade_request_with_api_version(uri, false)
}

fn ws_upgrade_request_with_api_version(uri: &str, with_api_version: bool) -> TestRequest {
    let mut req = TestRequest::get()
        .uri(uri)
        .insert_header(("Connection", "Upgrade"))
        .insert_header(("Upgrade", "websocket"))
        .insert_header(("Sec-WebSocket-Version", "13"))
        .insert_header(("Sec-WebSocket-Key", WS_KEY));
    if with_api_version {
        req = req.insert_header(API_VERSION);
    }
    req
}

fn start_ws_server(state: quant_pivot_web::AppState) -> TestServer {
    start_test_server(move || {
        App::new()
            .app_data(web::Data::new(state.clone()))
            .wrap(from_fn(quant_pivot_web::middleware::request_id))
            .wrap(from_fn(quant_pivot_web::middleware::operation_audit))
            .configure(quant_pivot_web::routes::configure)
    })
}

fn attach_api_version(server: &mut TestServer) {
    if let Some(headers) = server.client_headers() {
        headers.insert(
            actix_http::header::HeaderName::from_static("accept-api-version"),
            HeaderValue::from_static("v1"),
        );
    }
}

async fn connect_ws(
    server: &mut TestServer,
    token: &str,
) -> impl StreamExt<Item = Result<WsFrame, ProtocolError>>
+ SinkExt<WsMessage, Error = ProtocolError>
+ Unpin {
    server
        .ws_at(&format!("/api/ws?token={token}"))
        .await
        .expect("websocket connect")
}

async fn recv_text_json(
    session: &mut (impl StreamExt<Item = Result<WsFrame, ProtocolError>> + Unpin),
    timeout: Duration,
) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            remaining > Duration::ZERO,
            "timed out waiting for ws text frame"
        );
        let msg = tokio::time::timeout(remaining, session.next())
            .await
            .expect("timed out waiting for ws frame")
            .expect("ws stream ended")
            .expect("ws frame error");
        match msg {
            WsFrame::Text(text) => {
                let payload = std::str::from_utf8(&text).expect("utf-8 ws text");
                return serde_json::from_str(payload).expect("json envelope");
            }
            WsFrame::Ping(_) | WsFrame::Pong(_) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

async fn recv_until_type(
    session: &mut (impl StreamExt<Item = Result<WsFrame, ProtocolError>> + Unpin),
    expected: &str,
    timeout: Duration,
) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            remaining > Duration::ZERO,
            "no {expected} frame before timeout"
        );
        let envelope = recv_text_json(session, remaining).await;
        if envelope["type"] == expected {
            return envelope;
        }
    }
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ws_upgrade_rejects_missing_token() {
    let env = TestEnv::start().await;
    let res = harness::call(&env.state, ws_upgrade_request("/api/ws")).await;
    assert_eq!(res.status, StatusCode::UNAUTHORIZED);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ws_upgrade_rejects_invalid_token() {
    let env = TestEnv::start().await;
    let res = harness::call(
        &env.state,
        ws_upgrade_request("/api/ws?token=not-a-valid-jwt"),
    )
    .await;
    assert_eq!(res.status, StatusCode::UNAUTHORIZED);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ws_upgrade_succeeds_with_valid_access_token() {
    let env = TestEnv::start().await;
    let token = client::login(&env, "admin", "admin").await;
    let res = harness::call(
        &env.state,
        ws_upgrade_request(&format!("/api/ws?token={token}")),
    )
    .await;
    assert_eq!(res.status, StatusCode::SWITCHING_PROTOCOLS);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ws_upgrade_succeeds_without_api_version_header() {
    let env = TestEnv::start().await;
    let token = client::login(&env, "admin", "admin").await;
    let res = harness::call(
        &env.state,
        ws_upgrade_request_without_api_version(&format!("/api/ws?token={token}")),
    )
    .await;
    assert_eq!(res.status, StatusCode::SWITCHING_PROTOCOLS);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ws_ping_command_returns_pong_frame() {
    let env = TestEnv::start().await;
    let token = client::login(&env, "admin", "admin").await;
    let mut server = start_ws_server(env.state.clone());
    attach_api_version(&mut server);
    let mut session = connect_ws(&mut server, &token).await;

    session
        .send(WsMessage::Text(r#"{"action":"ping"}"#.into()))
        .await
        .unwrap();

    let envelope = recv_until_type(&mut session, "pong", Duration::from_secs(2)).await;
    assert!(envelope["data"].is_object() || envelope["data"].is_null());
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ws_sync_command_returns_snapshot_frame() {
    let env = TestEnv::start().await;
    let token = client::login(&env, "admin", "admin").await;
    let mut server = start_ws_server(env.state.clone());
    attach_api_version(&mut server);
    let mut session = connect_ws(&mut server, &token).await;

    session
        .send(WsMessage::Text(r#"{"action":"sync"}"#.into()))
        .await
        .unwrap();

    let envelope = recv_until_type(&mut session, "sync", Duration::from_secs(2)).await;
    assert!(envelope["data"]["system_status"].is_object());
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ws_subscribe_receives_fanned_alert_over_framed_connection() {
    let env = TestEnv::start().await;
    let token = client::login(&env, "admin", "admin").await;
    let mut server = start_ws_server(env.state.clone());
    attach_api_version(&mut server);
    let mut session = connect_ws(&mut server, &token).await;

    // The session pushes an initial `system.status` frame after auth; drain it so
    // the subscribe/alert assertions are not conflated with the welcome message.
    let _ = tokio::time::timeout(
        Duration::from_millis(500),
        recv_until_type(&mut session, "system.status", Duration::from_millis(500)),
    )
    .await;

    session
        .send(WsMessage::Text(
            r#"{"action":"subscribe","channel":"system.alert"}"#.into(),
        ))
        .await
        .unwrap();
    // Allow the server-side session task to apply the subscription before fan-out.
    tokio::time::sleep(Duration::from_millis(100)).await;

    env.state
        .events
        .publish(test_alert("test.ws_framed", "ws-framed-integration"));

    let envelope = recv_until_type(&mut session, "system.alert", Duration::from_secs(2)).await;
    assert_eq!(envelope["data"]["message"], "ws-framed-integration");
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ws_broadcaster_delivers_subscribed_core_event() {
    let env = TestEnv::start().await;
    let (outbound, rx) = flume::bounded::<String>(8);
    let subscriptions = Arc::new(RwLock::new(HashSet::from([SubscriptionKey::global(
        WsChannel::SystemAlert,
    )])));
    env.state.ws_sessions.register(SessionHandle {
        outbound,
        subscriptions,
    });

    env.state
        .events
        .publish(test_alert("test.ws_bus", "ws-bus-integration"));

    let text = tokio::time::timeout(Duration::from_secs(2), rx.recv_async())
        .await
        .expect("event should arrive within 2s")
        .expect("channel open");
    let envelope: serde_json::Value = serde_json::from_str(&text).expect("json envelope");
    assert_eq!(envelope["type"], "system.alert");
    assert_eq!(envelope["data"]["message"], "ws-bus-integration");
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ws_system_status_broadcast_without_subscribe() {
    let env = TestEnv::start().await;
    let token = client::login(&env, "admin", "admin").await;
    let mut server = start_ws_server(env.state.clone());
    attach_api_version(&mut server);
    let mut session = connect_ws(&mut server, &token).await;

    // Drain connect-time snapshot.
    let _ = recv_until_type(&mut session, "system.status", Duration::from_millis(500)).await;

    let status = env.state.control.system_status();
    env.state
        .events
        .publish(CoreEvent::SystemStatusChanged(status));
    let pushed = recv_until_type(&mut session, "system.status", Duration::from_secs(2)).await;
    assert_eq!(
        pushed["data"]["operational_phase"]["phase"],
        "catalog_warming"
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ws_subscribe_receives_multiple_system_status_events() {
    let env = TestEnv::start().await;
    let token = client::login(&env, "admin", "admin").await;
    let mut server = start_ws_server(env.state.clone());
    attach_api_version(&mut server);
    let mut session = connect_ws(&mut server, &token).await;

    session
        .send(WsMessage::Text(
            r#"{"action":"subscribe","channel":"system.status"}"#.into(),
        ))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let status = env.state.control.system_status();
    env.state
        .events
        .publish(CoreEvent::SystemStatusChanged(status.clone()));
    let first = recv_until_type(&mut session, "system.status", Duration::from_secs(2)).await;
    assert!(first["data"]["checked_at"].is_string());

    env.state
        .events
        .publish(CoreEvent::SystemStatusChanged(status));
    let second = recv_until_type(&mut session, "system.status", Duration::from_secs(2)).await;
    assert!(second["data"]["checked_at"].is_string());
}
