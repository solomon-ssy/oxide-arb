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
    domain::{
        CoreEvent, SubscriptionKey, SystemAlertEvent, SystemStatus, SystemStatusView, WsChannel,
        api::MarketBookView,
    },
    enums::common::{AlertCategory, AlertLevel, AlertSource},
    types::MarketId,
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

fn control_plane_status(env: &TestEnv, runtime: SystemStatus) -> SystemStatusView {
    SystemStatusView {
        runtime,
        bootstrap: env.state.bootstrap.view(),
        capabilities: env.state.bootstrap.capability_snapshot(),
    }
}

const WS_KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";
const WS_TICKET_PROTOCOL_PREFIX: &str = "qp-ticket.";

fn ws_upgrade_request(ticket: Option<&str>) -> TestRequest {
    ws_upgrade_request_with_api_version(ticket, true)
}

/// Browser WebSocket handshakes cannot set `Accept-Api-Version`; regression guard.
fn ws_upgrade_request_without_api_version(ticket: Option<&str>) -> TestRequest {
    ws_upgrade_request_with_api_version(ticket, false)
}

fn ws_upgrade_request_with_api_version(
    ticket: Option<&str>,
    with_api_version: bool,
) -> TestRequest {
    let mut req = TestRequest::get()
        .uri("/api/ws")
        .insert_header(("Connection", "Upgrade"))
        .insert_header(("Upgrade", "websocket"))
        .insert_header(("Sec-WebSocket-Version", "13"))
        .insert_header(("Sec-WebSocket-Key", WS_KEY))
        .insert_header(("Origin", "http://localhost:8080"));
    if with_api_version {
        req = req.insert_header(API_VERSION);
    }
    if let Some(ticket) = ticket {
        req = req.insert_header((
            "Sec-WebSocket-Protocol",
            format!("{WS_TICKET_PROTOCOL_PREFIX}{ticket}"),
        ));
    }
    req
}

async fn issue_ws_ticket(env: &TestEnv, access_token: &str) -> String {
    let response = client::post(env, "/api/ws/tickets", access_token, serde_json::json!({})).await;
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.json()["data"]["expires_in"], 30);
    response.json()["data"]["ticket"]
        .as_str()
        .expect("websocket ticket")
        .to_owned()
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
    ticket: &str,
) -> impl StreamExt<Item = Result<WsFrame, ProtocolError>>
+ SinkExt<WsMessage, Error = ProtocolError>
+ Unpin {
    let origin = server.url("").trim_end_matches('/').to_owned();
    if let Some(headers) = server.client_headers() {
        headers.insert(
            actix_http::header::SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_str(&format!("{WS_TICKET_PROTOCOL_PREFIX}{ticket}"))
                .expect("valid ticket protocol"),
        );
        headers.insert(
            actix_http::header::ORIGIN,
            HeaderValue::from_str(&origin).expect("valid test server origin"),
        );
    }
    server.ws_at("/api/ws").await.expect("websocket connect")
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
async fn ws_upgrade_rejects_missing_ticket() {
    let env = TestEnv::start().await;
    let res = harness::call(&env.state, ws_upgrade_request(None)).await;
    assert_eq!(res.status, StatusCode::UNAUTHORIZED);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ws_upgrade_rejects_query_access_token() {
    let env = TestEnv::start().await;
    let token = client::login(&env, "admin", "admin").await;
    let request = TestRequest::get()
        .uri(&format!("/api/ws?token={token}"))
        .insert_header(("Connection", "Upgrade"))
        .insert_header(("Upgrade", "websocket"))
        .insert_header(("Sec-WebSocket-Version", "13"))
        .insert_header(("Sec-WebSocket-Key", WS_KEY));
    let res = harness::call(&env.state, request).await;
    assert_eq!(res.status, StatusCode::UNAUTHORIZED);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ws_upgrade_rejects_unknown_ticket() {
    let env = TestEnv::start().await;
    let res = harness::call(&env.state, ws_upgrade_request(Some("unknown"))).await;
    assert_eq!(res.status, StatusCode::UNAUTHORIZED);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ws_upgrade_consumes_ticket_exactly_once() {
    let env = TestEnv::start().await;
    let token = client::login(&env, "admin", "admin").await;
    let ticket = issue_ws_ticket(&env, &token).await;
    let res = harness::call(&env.state, ws_upgrade_request(Some(&ticket))).await;
    assert_eq!(
        res.status,
        StatusCode::SWITCHING_PROTOCOLS,
        "upgrade response: {}",
        String::from_utf8_lossy(&res.raw_body)
    );
    assert_eq!(
        res.header("sec-websocket-protocol"),
        Some(format!("{WS_TICKET_PROTOCOL_PREFIX}{ticket}").as_str())
    );

    let replay = harness::call(&env.state, ws_upgrade_request(Some(&ticket))).await;
    assert_eq!(replay.status, StatusCode::UNAUTHORIZED);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ws_upgrade_rejects_ticket_from_an_old_authorization_revision() {
    let env = TestEnv::start().await;
    let token = client::login(&env, "admin", "admin").await;
    let ticket = issue_ws_ticket(&env, &token).await;

    env.state
        .casbin
        .reload()
        .await
        .expect("reload authorization policy");

    let response = harness::call(&env.state, ws_upgrade_request(Some(&ticket))).await;
    assert_eq!(response.status, StatusCode::UNAUTHORIZED);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ws_upgrade_succeeds_without_api_version_header() {
    let env = TestEnv::start().await;
    let token = client::login(&env, "admin", "admin").await;
    let ticket = issue_ws_ticket(&env, &token).await;
    let res = harness::call(
        &env.state,
        ws_upgrade_request_without_api_version(Some(&ticket)),
    )
    .await;
    assert_eq!(res.status, StatusCode::SWITCHING_PROTOCOLS);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ws_ping_command_returns_pong_frame() {
    let env = TestEnv::start().await;
    let token = client::login(&env, "admin", "admin").await;
    let ticket = issue_ws_ticket(&env, &token).await;
    let mut server = start_ws_server(env.state.clone());
    attach_api_version(&mut server);
    let mut session = connect_ws(&mut server, &ticket).await;

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
    let ticket = issue_ws_ticket(&env, &token).await;
    let mut server = start_ws_server(env.state.clone());
    attach_api_version(&mut server);
    let mut session = connect_ws(&mut server, &ticket).await;

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
    let ticket = issue_ws_ticket(&env, &token).await;
    let mut server = start_ws_server(env.state.clone());
    attach_api_version(&mut server);
    let mut session = connect_ws(&mut server, &ticket).await;

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
        subject: "test-user".to_owned(),
        family_id: "test-family".to_owned(),
        can_read_system: true,
        cancellation: tokio_util::sync::CancellationToken::new(),
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

fn book_update(market: &str) -> CoreEvent {
    CoreEvent::MarketBookUpdate {
        market_id: MarketId::new(market),
        view: Box::new(MarketBookView {
            market_id: MarketId::new(market),
            yes: None,
            no: None,
        }),
    }
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ws_market_scoped_book_update_reaches_only_matching_subscriber() {
    let env = TestEnv::start().await;

    let (watcher_tx, watcher_rx) = flume::bounded::<String>(8);
    env.state.ws_sessions.register(SessionHandle {
        outbound: watcher_tx,
        subscriptions: Arc::new(RwLock::new(HashSet::from([SubscriptionKey::scoped(
            WsChannel::MarketBookUpdate,
            MarketId::new("0xaaa"),
        )]))),
        subject: "test-user".to_owned(),
        family_id: "test-family-a".to_owned(),
        can_read_system: false,
        cancellation: tokio_util::sync::CancellationToken::new(),
    });

    let (other_tx, other_rx) = flume::bounded::<String>(8);
    env.state.ws_sessions.register(SessionHandle {
        outbound: other_tx,
        subscriptions: Arc::new(RwLock::new(HashSet::from([SubscriptionKey::scoped(
            WsChannel::MarketBookUpdate,
            MarketId::new("0xbbb"),
        )]))),
        subject: "test-user".to_owned(),
        family_id: "test-family-b".to_owned(),
        can_read_system: false,
        cancellation: tokio_util::sync::CancellationToken::new(),
    });

    env.state.events.publish(book_update("0xaaa"));

    let text = tokio::time::timeout(Duration::from_secs(2), watcher_rx.recv_async())
        .await
        .expect("scoped book update should arrive within 2s")
        .expect("channel open");
    let envelope: serde_json::Value = serde_json::from_str(&text).expect("json envelope");
    assert_eq!(envelope["type"], "market.book_update");
    assert_eq!(envelope["data"]["market_id"], "0xaaa");

    assert!(
        other_rx.try_recv().is_err(),
        "session scoped to a different market must not receive the frame"
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ws_market_resolved_reaches_global_subscriber() {
    let env = TestEnv::start().await;

    let (outbound, rx) = flume::bounded::<String>(8);
    env.state.ws_sessions.register(SessionHandle {
        outbound,
        subscriptions: Arc::new(RwLock::new(HashSet::from([SubscriptionKey::global(
            WsChannel::MarketResolved,
        )]))),
        subject: "test-user".to_owned(),
        family_id: "test-family".to_owned(),
        can_read_system: false,
        cancellation: tokio_util::sync::CancellationToken::new(),
    });

    env.state.events.publish(CoreEvent::MarketResolved {
        market_id: MarketId::new("0xccc"),
        outcome: true,
    });

    let text = tokio::time::timeout(Duration::from_secs(2), rx.recv_async())
        .await
        .expect("market.resolved should arrive within 2s")
        .expect("channel open");
    let envelope: serde_json::Value = serde_json::from_str(&text).expect("json envelope");
    assert_eq!(envelope["type"], "market.resolved");
    assert_eq!(envelope["data"]["market_id"], "0xccc");
    assert_eq!(envelope["data"]["outcome"], true);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ws_system_status_broadcast_without_subscribe() {
    let env = TestEnv::start().await;
    let token = client::login(&env, "admin", "admin").await;
    let ticket = issue_ws_ticket(&env, &token).await;
    let mut server = start_ws_server(env.state.clone());
    attach_api_version(&mut server);
    let mut session = connect_ws(&mut server, &ticket).await;

    // Drain connect-time snapshot.
    let _ = recv_until_type(&mut session, "system.status", Duration::from_millis(500)).await;

    let status = env.state.control.system_status();
    env.state
        .events
        .publish(CoreEvent::SystemStatusChanged(Box::new(
            control_plane_status(&env, status),
        )));
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
    let ticket = issue_ws_ticket(&env, &token).await;
    let mut server = start_ws_server(env.state.clone());
    attach_api_version(&mut server);
    let mut session = connect_ws(&mut server, &ticket).await;

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
        .publish(CoreEvent::SystemStatusChanged(Box::new(
            control_plane_status(&env, status.clone()),
        )));
    let first = recv_until_type(&mut session, "system.status", Duration::from_secs(2)).await;
    assert!(first["data"]["checked_at"].is_string());

    env.state
        .events
        .publish(CoreEvent::SystemStatusChanged(Box::new(
            control_plane_status(&env, status),
        )));
    let second = recv_until_type(&mut session, "system.status", Duration::from_secs(2)).await;
    assert!(second["data"]["checked_at"].is_string());
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn ws_system_status_lifecycle_phase_push() {
    use chrono::Utc;
    use quant_pivot_models::domain::{
        OperationalPhase,
        lifecycle::{MarketDataConnectivity, WsShardConnectivity},
        ports::runtime_control::CatalogState,
    };

    let env = TestEnv::start().await;
    let token = client::login(&env, "admin", "admin").await;
    let ticket = issue_ws_ticket(&env, &token).await;
    let mut server = start_ws_server(env.state.clone());
    attach_api_version(&mut server);
    let mut session = connect_ws(&mut server, &ticket).await;

    let warming = recv_until_type(&mut session, "system.status", Duration::from_millis(500)).await;
    assert_eq!(
        warming["data"]["operational_phase"]["phase"],
        "catalog_warming"
    );

    let mut status = env.state.control.system_status();
    status.catalog = CatalogState::Ready {
        markets: 42,
        synced_at: Utc::now(),
    };
    status.operational_phase = OperationalPhase::MarketDataConnecting;
    status.market_data = MarketDataConnectivity {
        ready: false,
        last_message_age_ms: None,
        ws_shards: WsShardConnectivity {
            total: 2,
            disconnected: 1,
            oldest_disconnected_secs: Some(3),
            connected_ratio_bps: 5_000,
        },
    };
    env.state
        .events
        .publish(CoreEvent::SystemStatusChanged(Box::new(
            control_plane_status(&env, status),
        )));

    let pushed = recv_until_type(&mut session, "system.status", Duration::from_secs(2)).await;
    assert_eq!(
        pushed["data"]["operational_phase"]["phase"],
        "market_data_connecting"
    );
    assert_eq!(pushed["data"]["catalog"]["state"], "ready");
    assert_eq!(pushed["data"]["market_data"]["ready"], false);
    assert_eq!(pushed["data"]["market_data"]["ws_shards"]["total"], 2);
    assert_eq!(
        pushed["data"]["market_data"]["ws_shards"]["disconnected"],
        1
    );
}
