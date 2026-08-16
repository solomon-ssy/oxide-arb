//! Public HTTP, authentication, authorization, read-model, and WebSocket
//! contracts exercised through the real production binary.

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use quant_pivot_models::domain::api::FeedbackCycleTriggerRequest;
use quant_pivot_system_tests::{
    production_stack::{ProductionStack, ProductionStackFixture},
    stack::BOOTSTRAP_ADMIN_PASSWORD,
};
use reqwest::{
    Client, Method, Response, StatusCode, header,
    header::{COOKIE, ORIGIN, SEC_WEBSOCKET_PROTOCOL, SET_COOKIE},
};
use serde_json::{Value, json};
use tokio::{net::TcpStream, time::Instant};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest, http::HeaderValue},
};

const API_VERSION: (&str, &str) = ("accept-api-version", "v1");
const BROWSER_ORIGIN: &str = "http://127.0.0.1:6099";

struct ApiClient {
    base_url: String,
    http: Client,
}

type ProductionSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct FeedbackSocket {
    socket: ProductionSocket,
}

struct WsTicket {
    protocol: String,
    ws_url: String,
}

struct TestUser {
    id: String,
    login: Login,
}

impl ApiClient {
    fn new(base_url: &str) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .context("build production-stack HTTP client")?;
        Ok(Self {
            base_url: base_url.to_owned(),
            http,
        })
    }

    async fn public_get(&self, path: &str) -> Result<Response> {
        self.http
            .get(format!("{}{path}", self.base_url))
            .send()
            .await
            .with_context(|| format!("GET {path}"))
    }

    async fn get(&self, path: &str, token: Option<&str>) -> Result<Response> {
        let mut request = self
            .http
            .get(format!("{}{path}", self.base_url))
            .header(API_VERSION.0, API_VERSION.1);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        request.send().await.with_context(|| format!("GET {path}"))
    }

    async fn post(&self, path: &str, token: Option<&str>, body: Value) -> Result<Response> {
        let mut request = self
            .http
            .post(format!("{}{path}", self.base_url))
            .header(API_VERSION.0, API_VERSION.1)
            .json(&body);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        request.send().await.with_context(|| format!("POST {path}"))
    }

    async fn post_governed(
        &self,
        path: &str,
        token: &str,
        acting_role: &str,
        body: Value,
    ) -> Result<Response> {
        self.http
            .post(format!("{}{path}", self.base_url))
            .header(API_VERSION.0, API_VERSION.1)
            .header("x-acting-role", acting_role)
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("POST {path}"))
    }

    async fn put(&self, path: &str, token: &str, body: Value) -> Result<Response> {
        self.http
            .put(format!("{}{path}", self.base_url))
            .header(API_VERSION.0, API_VERSION.1)
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("PUT {path}"))
    }

    async fn request(&self, method: Method, path: &str, token: &str) -> Result<Response> {
        self.http
            .request(method.clone(), format!("{}{path}", self.base_url))
            .header(API_VERSION.0, API_VERSION.1)
            .bearer_auth(token)
            .json(&json!({}))
            .send()
            .await
            .with_context(|| format!("{method} {path}"))
    }

    async fn login(&self, username: &str, password: &str) -> Result<Login> {
        let response = self
            .post(
                "/api/auth/login",
                None,
                json!({ "username": username, "password": password }),
            )
            .await?;
        let response = ensure_status(response, StatusCode::OK, "login").await?;
        let refresh_cookie = response
            .headers()
            .get(SET_COOKIE)
            .context("login response is missing refresh cookie")?
            .to_str()
            .context("refresh cookie is not valid ASCII")?
            .to_owned();
        let body = response
            .json::<Value>()
            .await
            .context("decode login response")?;
        let access_token = body["data"]["access_token"]
            .as_str()
            .context("login response is missing access_token")?
            .to_owned();
        Ok(Login {
            access_token,
            refresh_cookie,
        })
    }

    async fn refresh(&self, refresh_cookie: &str) -> Result<Response> {
        let cookie = refresh_cookie
            .split(';')
            .next()
            .context("refresh cookie has no name/value pair")?;
        self.http
            .post(format!("{}/api/auth/refresh", self.base_url))
            .header(API_VERSION.0, API_VERSION.1)
            .header(COOKIE, cookie)
            .header(ORIGIN, BROWSER_ORIGIN)
            .header("sec-fetch-site", "same-origin")
            .send()
            .await
            .context("refresh same-origin session")
    }

    async fn ws_ticket(&self, token: &str) -> Result<WsTicket> {
        let response = ensure_status(
            self.post("/api/ws/tickets", Some(token), json!({})).await?,
            StatusCode::OK,
            "issue WebSocket ticket",
        )
        .await?;
        let body = response.json::<Value>().await?;
        let ticket = body["data"]["ticket"]
            .as_str()
            .context("WebSocket ticket response is missing ticket")?;
        Ok(WsTicket {
            protocol: format!("qp-ticket.{ticket}"),
            ws_url: self.base_url.replacen("http://", "ws://", 1) + "/api/ws",
        })
    }
}

struct Login {
    access_token: String,
    refresh_cookie: String,
}

impl WsTicket {
    async fn connect(&self) -> Result<FeedbackSocket> {
        let mut request = self.ws_url.as_str().into_client_request()?;
        request
            .headers_mut()
            .insert(ORIGIN, HeaderValue::from_static(BROWSER_ORIGIN));
        request.headers_mut().insert(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_str(&self.protocol)?,
        );
        let (socket, response) = connect_async(request).await?;
        ensure!(
            response
                .headers()
                .get(header::SEC_WEBSOCKET_PROTOCOL)
                .and_then(|value| value.to_str().ok())
                == Some(self.protocol.as_str()),
            "WebSocket ticket protocol was not echoed"
        );
        Ok(FeedbackSocket { socket })
    }

    async fn rejected(&self) -> Result<()> {
        let mut request = self.ws_url.as_str().into_client_request()?;
        request
            .headers_mut()
            .insert(ORIGIN, HeaderValue::from_static(BROWSER_ORIGIN));
        request.headers_mut().insert(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_str(&self.protocol)?,
        );
        ensure!(
            connect_async(request).await.is_err(),
            "WebSocket ticket must fail closed"
        );
        Ok(())
    }
}

impl FeedbackSocket {
    async fn send(&mut self, body: Value) -> Result<()> {
        self.socket
            .send(Message::Text(body.to_string().into()))
            .await
            .context("send WebSocket command")
    }

    async fn next_json(&mut self, deadline: Instant) -> Result<Value> {
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            ensure!(!remaining.is_zero(), "WebSocket frame deadline elapsed");
            let message = tokio::time::timeout(remaining, self.socket.next())
                .await
                .context("WebSocket frame timeout")?
                .context("WebSocket closed before expected frame")??;
            match message {
                Message::Text(text) => {
                    return serde_json::from_str(&text).context("decode WS frame");
                }
                Message::Ping(payload) => {
                    self.socket
                        .send(Message::Pong(payload))
                        .await
                        .context("reply to WebSocket ping")?;
                }
                Message::Close(frame) => {
                    bail!("WebSocket closed before expected frame: {frame:?}");
                }
                _ => {}
            }
        }
    }

    async fn expect_error(&mut self, expected: &str) -> Result<Value> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let body = self.next_json(deadline).await?;
            if body["type"] == "error" {
                ensure!(
                    body["data"]["error"] == expected,
                    "unexpected WebSocket error frame: {body}"
                );
                return Ok(body);
            }
        }
    }

    async fn expect_sync(&mut self) -> Result<()> {
        self.send(json!({ "action": "sync" })).await?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let body = self.next_json(deadline).await?;
            ensure!(
                body["type"] != "error",
                "WebSocket sync failed closed unexpectedly: {body}"
            );
            if body["type"] == "sync" {
                return Ok(());
            }
        }
    }

    async fn subscribe(&mut self, after_revision: i64) -> Result<()> {
        self.send(json!({
            "action": "subscribe",
            "channel": "research.feedback",
            "after_revision": after_revision,
        }))
        .await
    }

    async fn feedback_until(&mut self, target_revision: i64) -> Result<Vec<Value>> {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut events = Vec::new();
        loop {
            let body = self.next_json(deadline).await?;
            ensure!(
                body["type"] != "error",
                "feedback replay failed closed unexpectedly: {body}"
            );
            if body["type"] != "research.feedback" {
                continue;
            }
            let revision = feedback_event_revision(&body)?;
            events.push(body);
            if revision >= target_revision {
                assert_strict_revisions(&events)?;
                return Ok(events);
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.socket.close(None).await.context("close WebSocket")
    }

    async fn wait_closed(&mut self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            ensure!(
                !remaining.is_zero(),
                "permission-drift WebSocket did not close"
            );
            match tokio::time::timeout(remaining, self.socket.next()).await {
                Ok(Some(Ok(Message::Close(_))) | None) => return Ok(()),
                Ok(Some(Ok(Message::Ping(payload)))) => {
                    self.socket.send(Message::Pong(payload)).await?;
                }
                Ok(Some(Ok(_))) => {}
                Ok(Some(Err(error))) => return Err(error.into()),
                Err(error) => return Err(error.into()),
            }
        }
    }
}

#[tokio::test]
async fn production_web_boundary_end() -> Result<()> {
    assert_feedback_fixture_shapes()?;
    let mut stack = ProductionStack::start(ProductionStackFixture::GovernedFeedback).await?;
    let artifacts = stack.run_dir().display().to_string();
    let result = Box::pin(exercise_web_boundary(&mut stack)).await;
    if let Err(error) = result {
        let shutdown = Box::pin(stack.stop(false)).await;
        if let Err(shutdown) = shutdown {
            bail!(
                "production web system contract failed; artifacts={artifacts}: {error:#}; shutdown failed: {shutdown:#}"
            );
        }
        bail!("production web system contract failed; artifacts={artifacts}: {error:#}");
    }
    Box::pin(stack.stop(true)).await
}

fn feedback_trigger_body(profile_id: &str, reason: &str, idempotency_key: &str) -> Value {
    json!({
        "profile_id": profile_id,
        "evaluation_mode": "conditional",
        "idempotency_key": idempotency_key,
        "parent_cycle_id": null,
        "reason": reason,
    })
}

fn assert_feedback_fixture_shapes() -> Result<()> {
    for body in [
        feedback_trigger_body(
            "crypto_price_15m",
            "operator_retrain",
            "web-feedback-valid-shape",
        ),
        feedback_trigger_body(
            "missing_profile",
            "operator_retrain",
            "web-feedback-missing-profile",
        ),
        feedback_trigger_body(
            "weather_forecast_24h",
            "w4_e03_operator_retrain",
            "web-feedback-ws-replay",
        ),
    ] {
        serde_json::from_value::<FeedbackCycleTriggerRequest>(body)
            .context("feedback trigger system-test fixture no longer matches the wire DTO")?;
    }
    Ok(())
}

#[test]
fn feedback_fixture_shapes() {
    assert_feedback_fixture_shapes().expect("feedback trigger fixtures match the wire DTO");
}

async fn exercise_web_boundary(stack: &mut ProductionStack) -> Result<()> {
    let api = ApiClient::new(stack.base_url())?;
    api.assert_public_probes().await?;
    api.assert_version_authentication_boundary().await?;
    api.assert_feedback_authentication().await?;

    let mut admin = api.login("admin", BOOTSTRAP_ADMIN_PASSWORD).await?;
    assert_hardened_refresh_cookie(&admin.refresh_cookie)?;
    let admin_id = assert_admin_identity(&api, &admin.access_token).await?;
    admin.refresh_cookie = assert_refresh_origin_boundary(&api, &admin.refresh_cookie).await?;

    let viewer = create_viewer(&api, &admin.access_token).await?;
    assert_authorization_boundary(&api, &viewer.login.access_token).await?;
    assert_read_models(&api, &admin.access_token, &viewer.login.access_token).await?;
    assert_factor_boundary(&api, &admin.access_token).await?;
    assert_feedback_mutations(&api, &admin.access_token, &viewer.login.access_token).await?;
    Box::pin(assert_feedback_ws(stack, &api, &admin, &admin_id)).await?;
    assert_permission_drift(&api, &admin.access_token, viewer).await?;
    Box::pin(assert_redis_restart(stack, &api, &admin)).await?;
    Ok(())
}

impl ApiClient {
    async fn assert_public_probes(&self) -> Result<()> {
        for (path, expected) in [("/health", "ok"), ("/startup", "started")] {
            let response =
                ensure_status(self.public_get(path).await?, StatusCode::OK, path).await?;
            let body = response.json::<Value>().await?;
            ensure!(
                body["data"]["status"] == expected,
                "unexpected {path} body: {body}"
            );
        }

        let ready =
            ensure_status(self.public_get("/ready").await?, StatusCode::OK, "/ready").await?;
        let ready = ready.json::<Value>().await?;
        ensure!(
            ready["data"]["status"] == "ready",
            "unexpected readiness: {ready}"
        );
        let checks = ready["data"]["checks"]
            .as_array()
            .context("readiness checks are not an array")?;
        for required in ["postgresql", "redis"] {
            ensure!(
                checks
                    .iter()
                    .any(|check| check["name"] == required && check["ok"] == true),
                "readiness is missing successful {required} check: {ready}"
            );
        }
        ensure!(
            checks.iter().any(|check| check["name"] == "catalog"),
            "readiness is missing the catalog warming signal: {ready}"
        );

        let metrics = ensure_status(
            self.public_get("/metrics").await?,
            StatusCode::OK,
            "/metrics",
        )
        .await?;
        ensure!(
            metrics
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with("text/plain")),
            "metrics must use Prometheus text content type"
        );
        Ok(())
    }
}

impl ApiClient {
    async fn assert_version_authentication_boundary(&self) -> Result<()> {
        let no_version = self
            .http
            .get(format!("{}/api/auth/me", self.base_url))
            .send()
            .await?;
        ensure!(
            no_version.status() == StatusCode::NOT_FOUND,
            "version guard must reject missing header"
        );

        ensure_status(
            self.get("/api/auth/me", None).await?,
            StatusCode::UNAUTHORIZED,
            "unauthenticated /api/auth/me",
        )
        .await?;

        let wrong = self
            .post(
                "/api/auth/login",
                None,
                json!({ "username": "admin", "password": "incorrect-password" }),
            )
            .await?;
        let wrong = ensure_status(wrong, StatusCode::UNAUTHORIZED, "wrong password").await?;
        let body = wrong.json::<Value>().await?;
        ensure!(
            body["message"] == "invalid credentials",
            "credential errors must be generic: {body}"
        );
        Ok(())
    }

    async fn assert_feedback_authentication(&self) -> Result<()> {
        for (response, operation) in [
            (
                self.get("/api/research/feedback-overview", None).await?,
                "unauthenticated feedback overview",
            ),
            (
                self.post("/api/ws/tickets", None, json!({})).await?,
                "unauthenticated WebSocket ticket",
            ),
        ] {
            let body = ensure_status(response, StatusCode::UNAUTHORIZED, operation)
                .await?
                .json::<Value>()
                .await?;
            ensure!(
                body["message"] == "unauthorized" && body["data"].is_null(),
                "{operation} leaked authentication detail: {body}"
            );
        }
        Ok(())
    }
}

fn assert_hardened_refresh_cookie(cookie: &str) -> Result<()> {
    for attribute in ["HttpOnly", "Secure", "SameSite=Strict", "Path=/api/auth"] {
        ensure!(
            cookie.contains(attribute),
            "refresh cookie is missing {attribute}: {cookie}"
        );
    }
    Ok(())
}

async fn assert_admin_identity(api: &ApiClient, admin: &str) -> Result<String> {
    let me = ensure_status(
        api.get("/api/auth/me", Some(admin)).await?,
        StatusCode::OK,
        "admin me",
    )
    .await?;
    let me = me.json::<Value>().await?;
    ensure!(
        me["data"]["user"]["username"] == "admin",
        "unexpected admin identity: {me}"
    );
    ensure!(
        me["data"]["roles"]
            .as_array()
            .is_some_and(|roles| roles.iter().any(|role| role["code"] == "super_admin")),
        "bootstrap admin is missing super_admin: {me}"
    );
    me["data"]["user"]["id"]
        .as_str()
        .map(str::to_owned)
        .context("admin identity is missing stable user id")
}

async fn assert_refresh_origin_boundary(api: &ApiClient, refresh_cookie: &str) -> Result<String> {
    let cookie = refresh_cookie
        .split(';')
        .next()
        .context("refresh cookie has no name/value pair")?;
    let missing_origin = api
        .http
        .post(format!("{}/api/auth/refresh", api.base_url))
        .header(API_VERSION.0, API_VERSION.1)
        .header(COOKIE, cookie)
        .send()
        .await?;
    ensure!(
        missing_origin.status() == StatusCode::UNAUTHORIZED,
        "refresh without Origin must fail closed"
    );

    let rotated = ensure_status(
        api.refresh(refresh_cookie).await?,
        StatusCode::OK,
        "same-origin refresh",
    )
    .await?;
    let rotated_cookie = rotated
        .headers()
        .get(SET_COOKIE)
        .context("rotated refresh response is missing Set-Cookie")?
        .to_str()
        .context("rotated refresh cookie is not valid ASCII")?
        .to_owned();
    assert_hardened_refresh_cookie(&rotated_cookie)?;
    Ok(rotated_cookie)
}

async fn create_viewer(api: &ApiClient, admin: &str) -> Result<TestUser> {
    let created = api
        .post(
            "/api/users",
            Some(admin),
            json!({
                "username": "production-stack-viewer",
                "password": "viewer-password",
                "nickname": "Production Stack Viewer"
            }),
        )
        .await?;
    let created = ensure_status(created, StatusCode::OK, "create viewer").await?;
    let created = created.json::<Value>().await?;
    let user_id = created["data"]["id"]
        .as_str()
        .context("created viewer is missing id")?;

    let roles = ensure_status(
        api.get("/api/roles", Some(admin)).await?,
        StatusCode::OK,
        "list roles",
    )
    .await?;
    let roles = roles.json::<Value>().await?;
    let viewer_id = roles["data"]
        .as_array()
        .context("role catalog is not an array")?
        .iter()
        .find(|role| role["code"] == "viewer")
        .and_then(|role| role["id"].as_str())
        .context("seeded viewer role is missing")?;
    ensure_status(
        api.put(
            &format!("/api/users/{user_id}/roles"),
            admin,
            json!({ "role_ids": [viewer_id] }),
        )
        .await?,
        StatusCode::OK,
        "assign viewer role",
    )
    .await?;
    let login = api
        .login("production-stack-viewer", "viewer-password")
        .await?;
    Ok(TestUser {
        id: user_id.to_owned(),
        login,
    })
}

async fn assert_authorization_boundary(api: &ApiClient, viewer: &str) -> Result<()> {
    ensure_status(
        api.get("/api/users?page=1&size=10", Some(viewer)).await?,
        StatusCode::FORBIDDEN,
        "viewer user administration",
    )
    .await?;
    ensure_status(
        api.get("/api/quant/reports?page=1&size=10", Some(viewer))
            .await?,
        StatusCode::OK,
        "viewer report catalog",
    )
    .await?;
    ensure_status(
        api.get("/api/_unregistered_probe", Some(viewer)).await?,
        StatusCode::FORBIDDEN,
        "unregistered protected route",
    )
    .await?;
    Ok(())
}

async fn assert_read_models(api: &ApiClient, admin: &str, viewer: &str) -> Result<()> {
    for path in [
        "/api/quant/reports?page=1&size=10",
        "/api/quant/intents?page=1&size=10",
        "/api/quant/reconciliations?page=1&size=10",
        "/api/research/training-datasets?page=1&size=10",
        "/api/research/model-specs?page=1&size=10",
        "/api/research/models?page=1&size=10",
        "/api/research/feedback-cycles?page=1&size=10",
        "/api/research/drift-reports?page=0&size=1000",
        "/api/research/model-route-activation-permits?page=1&size=10",
        "/api/research/factors?page=1&size=10",
    ] {
        let response =
            ensure_status(api.get(path, Some(admin)).await?, StatusCode::OK, path).await?;
        let body = response.json::<Value>().await?;
        ensure!(
            body["data"]["items"].is_array(),
            "{path} is not paginated: {body}"
        );
        ensure!(
            body["data"]["total"].is_number(),
            "{path} has no total: {body}"
        );
    }
    assert_model_gates(api, admin).await?;
    assert_fresh_boot_models(api, admin).await?;

    let feature_contract = ensure_status(
        api.get("/api/research/feature-contract", Some(admin))
            .await?,
        StatusCode::OK,
        "feature contract",
    )
    .await?;
    ensure!(feature_contract.json::<Value>().await?["data"].is_object());

    let feedback = ensure_status(
        api.get("/api/research/feedback-overview", Some(viewer))
            .await?,
        StatusCode::OK,
        "feedback overview",
    )
    .await?
    .json::<Value>()
    .await?;
    ensure!(
        feedback["data"]["revision"].is_number()
            && feedback["data"]["queue"]["queued"].as_u64() == Some(0)
            && feedback["data"]["queue"]["running"].as_u64() == Some(1)
            && feedback["data"]["queue"]["pending_outbox"].is_number()
            && feedback["data"]["queue"]["oldest_queued_at"].is_null()
            && feedback["data"]["queue"]["oldest_running_at"].is_string(),
        "feedback overview queue counts and oldest timestamps disagree: {feedback}"
    );
    let profiles = feedback["data"]["profiles"]
        .as_array()
        .context("feedback overview profiles are not an array")?;
    validate_feedback_profiles(profiles)
        .with_context(|| format!("invalid feedback overview profile registry: {feedback}"))?;
    ensure_status(
        api.get(
            "/api/research/feedback-cycles/00000000-0000-7000-8000-000000000001",
            Some(admin),
        )
        .await?,
        StatusCode::NOT_FOUND,
        "missing feedback cycle",
    )
    .await?;
    for (path, operation) in [
        (
            "/api/research/models/00000000-0000-7000-8000-000000000001?page=0&size=1000",
            "missing model detail",
        ),
        (
            "/api/research/factors/00000000-0000-7000-8000-000000000001?page=0&size=1000",
            "missing factor detail",
        ),
    ] {
        ensure_status(
            api.get(path, Some(admin)).await?,
            StatusCode::NOT_FOUND,
            operation,
        )
        .await?;
    }

    for path in [
        "/api/quant/structural/negrisk-events",
        "/api/quant/structural/execution-history/coverage",
        "/api/quant/structural/participant-concentration",
    ] {
        ensure_status(api.get(path, Some(admin)).await?, StatusCode::OK, path).await?;
    }

    let dashboard = ensure_status(
        api.get("/api/dashboard/overview?window=24h", Some(viewer))
            .await?,
        StatusCode::OK,
        "dashboard overview",
    )
    .await?;
    ensure!(
        dashboard
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok())
            == Some("private, no-store"),
        "dashboard must prohibit shared caching"
    );
    let dashboard = dashboard.json::<Value>().await?;
    ensure!(
        dashboard["data"]["revision"].is_string(),
        "dashboard has no revision: {dashboard}"
    );
    for section in [
        "authority",
        "account",
        "equity_curve",
        "latest_report",
        "report_lifecycle",
        "exposures",
        "data_quality",
        "subsystem_health",
        "action_inbox",
    ] {
        ensure!(
            dashboard["data"][section]["state"].is_string(),
            "dashboard section {section} is untagged: {dashboard}"
        );
    }
    Ok(())
}

async fn assert_fresh_boot_models(api: &ApiClient, admin: &str) -> Result<()> {
    let exchange_history = ensure_status(
        api.get("/api/system/exchange-history", Some(admin)).await?,
        StatusCode::OK,
        "exchange history progress",
    )
    .await?
    .json::<Value>()
    .await?;
    let history = exchange_history["data"]
        .as_object()
        .context("exchange history progress is not an object")?;
    for field in [
        "stage",
        "slo_status",
        "activation_from_block",
        "accepted_through_block",
        "target_block",
        "retention_from_block",
        "retention_accepted_from_block",
        "retention_through_block",
        "crypto_required_from_block",
        "weather_required_from_block",
        "unresolved_count",
        "quarantine_count",
        "projected_completion_at",
    ] {
        ensure!(
            history.contains_key(field),
            "exchange history progress omits `{field}`: {exchange_history}"
        );
    }

    let quarantines = ensure_status(
        api.get(
            "/api/system/exchange-history/quarantines?status=active&limit=20",
            Some(admin),
        )
        .await?,
        StatusCode::OK,
        "exchange-history quarantine page",
    )
    .await?
    .json::<Value>()
    .await?;
    ensure!(
        quarantines["data"]["items"].is_array()
            && (quarantines["data"]["next_after"].is_null()
                || quarantines["data"]["next_after"].is_string()),
        "quarantine page is not keyset-shaped: {quarantines}"
    );
    ensure_status(
        api.get(
            "/api/system/exchange-history/quarantines?limit=101",
            Some(admin),
        )
        .await?,
        StatusCode::BAD_REQUEST,
        "exchange-history quarantine limit",
    )
    .await?;

    let fresh_boot = ensure_status(
        api.get("/api/system/fresh-boot", Some(admin)).await?,
        StatusCode::OK,
        "fresh-boot progress",
    )
    .await?
    .json::<Value>()
    .await?;
    let data = fresh_boot["data"]
        .as_object()
        .context("fresh-boot progress is not an object")?;
    ensure!(
        data.get("run").is_none(),
        "fresh-boot response retained the superseded single-run contract: {fresh_boot}"
    );
    ensure!(
        data.get("observed_at").is_some_and(Value::is_string)
            && data.get("exchange_history").is_some_and(Value::is_object)
            && data.get("capability").is_some_and(Value::is_object)
            && data.get("profiles").is_some_and(Value::is_array),
        "fresh-boot response does not expose independent profile progress: {fresh_boot}"
    );
    ensure!(
        data["capability"]["state"].is_string()
            && data["capability"]["first_report_ready"].is_boolean()
            && data["capability"]["all_routes_ready"].is_boolean(),
        "fresh-boot capability summary is incomplete: {fresh_boot}"
    );
    if let Some(profiles) = data.get("profiles").and_then(Value::as_array) {
        for profile in profiles {
            ensure!(
                profile["run"]["route"].is_string()
                    && (profile["run"]["first_report_run_id"].is_null()
                        || profile["run"]["first_report_run_id"].is_string())
                    && (profile["run"]["first_report_id"].is_null()
                        || profile["run"]["first_report_id"].is_string()),
                "fresh-boot profile progress is incomplete: {profile}"
            );
            ensure!(
                profile.get("manual_report_ready").is_none()
                    && profile["run"].get("manual_report_ready_at").is_none(),
                "fresh-boot response retained removed manual-ready fields: {profile}"
            );
        }
    }
    Ok(())
}

fn validate_feedback_profiles(profiles: &[Value]) -> Result<()> {
    let mut actual = BTreeMap::new();
    for profile in profiles {
        ensure!(
            profile["minimum_coverage"].is_string(),
            "minimum_coverage is not a decimal string"
        );
        let profile_ref = &profile["profile_ref"];
        let id = profile_ref["id"]
            .as_str()
            .context("profile_ref.id is not a string")?;
        let version = profile_ref["version"]
            .as_u64()
            .context("profile_ref.version is not an unsigned integer")?;
        ensure!(
            profile_ref["content_hash"].is_string(),
            "profile_ref.content_hash is not a string"
        );
        let eligibility = profile["activation_eligibility"]
            .as_str()
            .context("activation_eligibility is not a string")?;
        let category = if profile["category"].is_null() {
            None
        } else {
            Some(
                profile["category"]
                    .as_str()
                    .context("profile category is neither null nor a string")?,
            )
        };
        ensure!(
            actual
                .insert(id, (version, eligibility, category))
                .is_none(),
            "duplicate profile_ref.id `{id}`"
        );
    }
    let expected = BTreeMap::from([
        (
            "crypto_price_15m",
            (4, "semi_auto_candidate", Some("crypto")),
        ),
        (
            "crypto_price_15m_bootstrap_trade",
            (2, "research_only", Some("crypto")),
        ),
        ("pooled_1h_control", (5, "research_only", None)),
        (
            "pooled_binary_1h_bootstrap_trade",
            (2, "research_only", None),
        ),
        (
            "weather_forecast_24h",
            (5, "semi_auto_candidate", Some("weather")),
        ),
        (
            "weather_forecast_24h_bootstrap_trade",
            (2, "research_only", Some("weather")),
        ),
    ]);
    ensure!(
        actual == expected,
        "profile registry mismatch: actual={actual:?}, expected={expected:?}"
    );
    Ok(())
}

async fn assert_model_gates(api: &ApiClient, admin: &str) -> Result<()> {
    let models = ensure_status(
        api.get("/api/research/models?page=1&size=100", Some(admin))
            .await?,
        StatusCode::OK,
        "complete model catalog",
    )
    .await?
    .json::<Value>()
    .await?;
    let items = models["data"]["items"]
        .as_array()
        .context("complete model catalog items are not an array")?;
    ensure!(!items.is_empty(), "complete model catalog is empty");
    for model in items {
        let model_version_id = model["model_version_id"]
            .as_str()
            .context("model catalog row has no model_version_id")?;
        let path =
            format!("/api/research/models/{model_version_id}/quality-gate?intent=route_activation");
        let gate = ensure_status(
            api.get(&path, Some(admin)).await?,
            StatusCode::OK,
            &format!("model {model_version_id} quality gate"),
        )
        .await?
        .json::<Value>()
        .await?;
        ensure!(
            gate["data"]["gates"].is_array(),
            "model {model_version_id} quality gate has no scorecard: {gate}"
        );
    }
    Ok(())
}

async fn assert_factor_boundary(api: &ApiClient, admin: &str) -> Result<()> {
    let factors = ensure_status(
        api.get("/api/research/factors?page=1&size=100", Some(admin))
            .await?,
        StatusCode::OK,
        "factor catalog",
    )
    .await?
    .json::<Value>()
    .await?;
    let factor_id = factors["data"]["items"]
        .as_array()
        .and_then(|items| items.first())
        .and_then(|item| item["factor_definition_id"].as_str())
        .context("browser fixture factor catalog is empty")?;

    ensure_status(
        api.get(
            "/api/research/factors/collinearity?lookback_secs=604800",
            Some(admin),
        )
        .await?,
        StatusCode::OK,
        "factor collinearity",
    )
    .await?;
    ensure_status(
        api.get(
            &format!("/api/research/factors/{factor_id}?page=0&size=1000"),
            Some(admin),
        )
        .await?,
        StatusCode::OK,
        "factor detail",
    )
    .await?;

    let factor_detail_path = format!("/api/research/factors/{factor_id}");
    for method in [Method::POST, Method::PUT, Method::DELETE] {
        for path in [
            "/api/research/factors",
            "/api/research/factors/collinearity",
            factor_detail_path.as_str(),
        ] {
            ensure_status(
                api.request(method.clone(), path, admin).await?,
                StatusCode::METHOD_NOT_ALLOWED,
                &format!("{method} {path} mutation absence"),
            )
            .await?;
        }
    }
    Ok(())
}

async fn assert_feedback_mutations(api: &ApiClient, admin: &str, viewer: &str) -> Result<()> {
    for acting_role in ["viewer", "super_admin"] {
        let denied = ensure_status(
            api.post_governed(
                "/api/research/feedback-cycles",
                viewer,
                acting_role,
                feedback_trigger_body(
                    "crypto_price_15m",
                    "operator_retrain",
                    "web-feedback-viewer-denied",
                ),
            )
            .await?,
            StatusCode::FORBIDDEN,
            &format!("viewer feedback trigger as {acting_role}"),
        )
        .await?
        .json::<Value>()
        .await?;
        ensure!(
            denied["message"] == "forbidden" && denied["data"].is_null(),
            "feedback denial leaked details: {denied}"
        );
    }

    ensure_status(
        api.post(
            "/api/research/feedback-cycles",
            Some(admin),
            feedback_trigger_body(
                "crypto_price_15m",
                "Invalid reason",
                "web-feedback-invalid-reason",
            ),
        )
        .await?,
        StatusCode::BAD_REQUEST,
        "invalid feedback trigger",
    )
    .await?;
    ensure_status(
        api.post(
            "/api/research/feedback-cycles",
            Some(admin),
            feedback_trigger_body(
                "missing_profile",
                "operator_retrain",
                "web-feedback-missing-profile",
            ),
        )
        .await?,
        StatusCode::NOT_FOUND,
        "missing feedback profile",
    )
    .await?;
    let missing_route = ensure_status(
        api.post_governed(
            "/api/research/feedback-cycles",
            admin,
            "super_admin",
            feedback_trigger_body(
                "pooled_1h_control",
                "operator_retrain",
                "web-feedback-missing-route",
            ),
        )
        .await?,
        StatusCode::CONFLICT,
        "feedback profile without an active serving route",
    )
    .await?
    .json::<Value>()
    .await?;
    ensure!(
        missing_route["message"]
            == "invalid feedback-cycle state: feedback profile pooled_1h_control has no active serving route Pooled"
            && missing_route["data"].is_null(),
        "missing feedback route lost its typed conflict envelope: {missing_route}"
    );
    ensure_status(
        api.post(
            "/api/research/feedback-cycles/00000000-0000-7000-8000-000000000001/cancel",
            Some(admin),
            json!({ "reason": "operator_cancelled" }),
        )
        .await?,
        StatusCode::NOT_FOUND,
        "missing feedback cancellation",
    )
    .await?;
    ensure_status(
        api.post(
            "/api/research/model-route-activation-permits/00000000-0000-7000-8000-000000000001/revoke",
            Some(admin),
            json!({
                "expected_revision": 0,
                "reason_code": "operator_revoked",
                "note": "withdraw exact authority",
            }),
        )
        .await?,
        StatusCode::NOT_FOUND,
        "missing feedback permit",
    )
    .await?;
    ensure_status(
        api.post_governed(
            "/api/research/model-route-activation-permits/00000000-0000-7000-8000-000000000001/revoke",
            admin,
            "super_admin",
            json!({
                "expected_revision": 1,
                "reason_code": "operator_revoked",
                "note": "invalid CAS revision",
            }),
        )
        .await?,
        StatusCode::BAD_REQUEST,
        "invalid feedback permit CAS revision",
    )
    .await?;
    Ok(())
}

async fn assert_feedback_ws(
    stack: &mut ProductionStack,
    api: &ApiClient,
    admin: &Login,
    admin_id: &str,
) -> Result<()> {
    let live = Box::pin(assert_feedback_live(api, admin, admin_id)).await?;
    let cancellation = Box::pin(assert_feedback_cancel(stack, api, admin, &live)).await?;
    Box::pin(assert_feedback_restart(
        stack,
        api,
        admin,
        &live,
        &cancellation,
    ))
    .await
}

struct FeedbackLiveEvidence {
    trigger_revision: i64,
}

struct FeedbackCancelEvidence {
    cycle_id: String,
    offline_revision: i64,
}

#[derive(PartialEq)]
struct FeedbackCancelSnapshot {
    cycle: Value,
    events: Vec<Value>,
}

impl FeedbackCancelSnapshot {
    async fn read(api: &ApiClient, admin: &str, cycle_id: &str) -> Result<Self> {
        let detail = ensure_status(
            api.get(
                &format!("/api/research/feedback-cycles/{cycle_id}"),
                Some(admin),
            )
            .await?,
            StatusCode::OK,
            "governed cancellation detail",
        )
        .await?
        .json::<Value>()
        .await?;
        let events = detail["data"]["timeline"]
            .as_array()
            .context("governed cancellation timeline is not an array")?
            .iter()
            .filter(|event| event["event_kind"] == "cancellation_requested")
            .cloned()
            .collect::<Vec<_>>();
        ensure!(
            events.len() == 1,
            "governed cancellation must retain exactly one WORM event: {events:?}"
        );
        Ok(Self {
            cycle: detail["data"]["cycle"].clone(),
            events,
        })
    }
}

async fn assert_feedback_live(
    api: &ApiClient,
    admin: &Login,
    admin_id: &str,
) -> Result<FeedbackLiveEvidence> {
    let initial_revision = feedback_revision(api, &admin.access_token).await?;
    ensure!(
        initial_revision >= 2,
        "browser fixture did not create durable feedback history"
    );

    let ticket = api.ws_ticket(&admin.access_token).await?;
    let mut socket = ticket.connect().await?;
    socket
        .send(json!({
            "action": "subscribe",
            "channel": "research.feedback.v2",
            "after_revision": 0,
        }))
        .await?;
    socket.expect_error("invalid_command").await?;
    socket
        .send(json!({
            "action": "subscribe",
            "channel": "research.feedback",
        }))
        .await?;
    socket.expect_error("missing_after_revision").await?;
    socket.subscribe(0).await?;
    let initial = socket.feedback_until(initial_revision).await?;
    ensure!(
        initial
            .iter()
            .all(|event| feedback_event_revision(event).is_ok()),
        "initial replay contains an invalid feedback payload"
    );
    socket.expect_sync().await?;
    ticket.rejected().await?;

    let reason = "w4_e03_operator_retrain";
    let first = trigger_feedback(api, &admin.access_token, reason).await?;
    let cycle_id = first["data"]["cycle"]["feedback_cycle_id"]
        .as_str()
        .context("trigger response is missing feedback cycle id")?;
    ensure!(
        first["data"]["trigger_replayed"] == false
            && first["data"]["cycle_reused"].is_boolean()
            && first["data"]
                .as_object()
                .is_some_and(|data| !data.contains_key("replayed")),
        "first governed trigger did not distinguish provenance replay from cadence convergence: {first}"
    );
    let first_trigger_revision = feedback_revision(api, &admin.access_token).await?;
    ensure!(
        first_trigger_revision > initial_revision,
        "governed trigger did not advance the durable outbox"
    );
    let live = socket.feedback_until(first_trigger_revision).await?;
    ensure!(
        live.iter()
            .any(|event| feedback_subject(event) == Some(cycle_id)),
        "live feedback event did not identify governed cycle {cycle_id}: {live:?}"
    );

    let replay = trigger_feedback(api, &admin.access_token, reason).await?;
    ensure!(
        replay["data"]["trigger_replayed"] == true
            && replay["data"]["cycle_reused"] == true
            && replay["data"]["cycle"]["feedback_cycle_id"] == cycle_id,
        "exact governed trigger did not replay its frozen cycle: {replay}"
    );
    assert_trigger_evidence(api, &admin.access_token, admin_id, cycle_id, reason).await?;
    let distinct = ensure_status(
        api.post_governed(
            "/api/research/feedback-cycles",
            &admin.access_token,
            "super_admin",
            feedback_trigger_body(
                "weather_forecast_24h",
                "w4_e03_drifted_reason",
                "web-feedback-ws-distinct",
            ),
        )
        .await?,
        StatusCode::ACCEPTED,
        "distinct feedback trigger provenance",
    )
    .await?
    .json::<Value>()
    .await?;
    ensure!(
        distinct["data"]["trigger_replayed"] == false
            && distinct["data"]["cycle_reused"] == true
            && distinct["data"]["cycle"]["feedback_cycle_id"] == cycle_id,
        "distinct trigger intent did not converge with new provenance: {distinct}"
    );
    let trigger_revision = feedback_revision(api, &admin.access_token).await?;
    ensure!(
        trigger_revision > first_trigger_revision,
        "distinct trigger intent did not append a durable provenance revision"
    );
    let distinct_live = socket.feedback_until(trigger_revision).await?;
    ensure!(
        distinct_live
            .iter()
            .any(|event| feedback_subject(event) == Some(cycle_id)),
        "distinct trigger provenance did not invalidate its canonical cycle: {distinct_live:?}"
    );
    assert_trigger_evidence(
        api,
        &admin.access_token,
        admin_id,
        cycle_id,
        "w4_e03_drifted_reason",
    )
    .await?;

    socket.close().await?;
    let duplicate_ticket = api.ws_ticket(&admin.access_token).await?;
    let mut duplicate = duplicate_ticket.connect().await?;
    duplicate.subscribe(initial_revision).await?;
    let replayed = duplicate.feedback_until(trigger_revision).await?;
    ensure!(
        replayed.iter().any(|event| feedback_event_revision(event)
            .is_ok_and(|revision| { revision == trigger_revision })),
        "after_revision replay did not reproduce the already-observed revision"
    );
    duplicate.close().await?;

    Ok(FeedbackLiveEvidence { trigger_revision })
}

async fn assert_feedback_cancel(
    stack: &ProductionStack,
    api: &ApiClient,
    admin: &Login,
    live: &FeedbackLiveEvidence,
) -> Result<FeedbackCancelEvidence> {
    let cancellation_cycle_id = stack
        .governed_cancellation_cycle_id()
        .context("GovernedFeedback fixture is missing its cancellation cycle")?
        .to_string();
    let cancellation_detail = ensure_status(
        api.get(
            &format!("/api/research/feedback-cycles/{cancellation_cycle_id}"),
            Some(&admin.access_token),
        )
        .await?,
        StatusCode::OK,
        "governed cancellation target",
    )
    .await?
    .json::<Value>()
    .await?;
    let cancellation_generation = cancellation_detail["data"]["cycle"]["generation"]
        .as_i64()
        .context("governed cancellation target is missing generation")?;
    ensure!(
        cancellation_detail["data"]["cycle"]["status"] == "running",
        "governed cancellation target lost its live-worker state: {cancellation_detail}"
    );
    let cancelled = ensure_status(
        api.post_governed(
            &format!("/api/research/feedback-cycles/{cancellation_cycle_id}/cancel"),
            &admin.access_token,
            "super_admin",
            json!({ "reason": "w4_e03_operator_cancelled" }),
        )
        .await?,
        StatusCode::ACCEPTED,
        "governed feedback cancellation",
    )
    .await?
    .json::<Value>()
    .await?;
    ensure!(
        cancelled["data"]["cycle"]["feedback_cycle_id"] == cancellation_cycle_id
            && cancelled["data"]["cycle"]["generation"].as_i64()
                == Some(cancellation_generation + 1)
            && cancelled["data"]["cycle"]["status"] == "running"
            && cancelled["data"]["replayed"] == false,
        "feedback cancellation did not apply one exact generation CAS: {cancelled}"
    );
    let offline_revision = feedback_revision(api, &admin.access_token).await?;
    ensure!(
        offline_revision > live.trigger_revision,
        "offline cancellation did not persist a newer outbox revision"
    );
    let cancelled_snapshot =
        FeedbackCancelSnapshot::read(api, &admin.access_token, &cancellation_cycle_id).await?;
    ensure!(
        cancelled_snapshot.cycle == cancelled["data"]["cycle"],
        "governed cancellation response differs from its durable cycle"
    );
    let cancel_replay = ensure_status(
        api.post_governed(
            &format!("/api/research/feedback-cycles/{cancellation_cycle_id}/cancel"),
            &admin.access_token,
            "super_admin",
            json!({ "reason": "w4_e03_operator_cancelled" }),
        )
        .await?,
        StatusCode::ACCEPTED,
        "governed cancellation exact replay",
    )
    .await?
    .json::<Value>()
    .await?;
    let replay_snapshot =
        FeedbackCancelSnapshot::read(api, &admin.access_token, &cancellation_cycle_id).await?;
    ensure!(
        cancel_replay["data"]["replayed"] == true
            && cancel_replay["data"]["cycle"] == cancelled_snapshot.cycle
            && replay_snapshot == cancelled_snapshot,
        "exact cancellation replay forked its target cycle or WORM event: {cancel_replay}"
    );
    let replay_revision = feedback_revision(api, &admin.access_token).await?;
    ensure!(
        replay_revision >= offline_revision,
        "feedback outbox revision moved backwards across cancellation replay"
    );
    if replay_revision > offline_revision {
        let ticket = api.ws_ticket(&admin.access_token).await?;
        let mut replay = ticket.connect().await?;
        replay.subscribe(offline_revision).await?;
        let concurrent = replay.feedback_until(replay_revision).await?;
        ensure!(
            concurrent
                .iter()
                .all(|event| feedback_subject(event) != Some(cancellation_cycle_id.as_str())),
            "exact cancellation replay appended another durable event for its subject: {concurrent:?}"
        );
        replay.close().await?;
        ticket.rejected().await?;
    }
    ensure_status(
        api.post_governed(
            &format!("/api/research/feedback-cycles/{cancellation_cycle_id}/cancel"),
            &admin.access_token,
            "super_admin",
            json!({ "reason": "w4_e03_drifted_cancel_reason" }),
        )
        .await?,
        StatusCode::CONFLICT,
        "governed cancellation immutable conflict",
    )
    .await?;

    Ok(FeedbackCancelEvidence {
        cycle_id: cancellation_cycle_id,
        offline_revision: replay_revision,
    })
}

async fn assert_feedback_restart(
    stack: &mut ProductionStack,
    api: &ApiClient,
    admin: &Login,
    live: &FeedbackLiveEvidence,
    cancellation: &FeedbackCancelEvidence,
) -> Result<()> {
    let restart_ticket = api.ws_ticket(&admin.access_token).await?;
    let mut interrupted = restart_ticket.connect().await?;
    interrupted.subscribe(cancellation.offline_revision).await?;
    interrupted.expect_sync().await?;
    stack.restart().await?;
    interrupted.wait_closed().await?;
    api.assert_public_probes().await?;
    ensure_status(
        api.get("/api/research/feedback-overview", Some(&admin.access_token))
            .await?,
        StatusCode::OK,
        "old access session after binary restart",
    )
    .await?;

    let recovery_ticket = api.ws_ticket(&admin.access_token).await?;
    let mut recovered = recovery_ticket.connect().await?;
    recovered.subscribe(live.trigger_revision).await?;
    let recovery = recovered
        .feedback_until(cancellation.offline_revision)
        .await?;
    ensure!(
        recovery.iter().all(|event| {
            feedback_event_revision(event).is_ok_and(|revision| revision > live.trigger_revision)
        }) && recovery
            .iter()
            .any(|event| feedback_subject(event) == Some(cancellation.cycle_id.as_str())),
        "restart replay did not recover strictly newer durable cycle events: {recovery:?}"
    );
    recovered.expect_sync().await?;
    recovered.close().await?;
    recovery_ticket.rejected().await?;
    Ok(())
}

async fn trigger_feedback(api: &ApiClient, admin: &str, reason: &str) -> Result<Value> {
    Ok(ensure_status(
        api.post_governed(
            "/api/research/feedback-cycles",
            admin,
            "super_admin",
            feedback_trigger_body("weather_forecast_24h", reason, "web-feedback-ws-replay"),
        )
        .await?,
        StatusCode::ACCEPTED,
        "governed feedback trigger",
    )
    .await?
    .json::<Value>()
    .await?)
}

async fn feedback_revision(api: &ApiClient, token: &str) -> Result<i64> {
    let body = ensure_status(
        api.get("/api/research/feedback-overview", Some(token))
            .await?,
        StatusCode::OK,
        "feedback overview revision",
    )
    .await?
    .json::<Value>()
    .await?;
    body["data"]["revision"]
        .as_i64()
        .context("feedback overview revision is not a signed integer")
}

fn feedback_event_revision(event: &Value) -> Result<i64> {
    event["data"]["revision"]
        .as_i64()
        .context("research.feedback event is missing revision")
}

fn feedback_subject(event: &Value) -> Option<&str> {
    event["data"]["subject_id"].as_str()
}

fn assert_strict_revisions(events: &[Value]) -> Result<()> {
    let revisions = events
        .iter()
        .map(feedback_event_revision)
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        revisions.iter().all(|revision| *revision > 0)
            && revisions.windows(2).all(|pair| pair[0] < pair[1]),
        "feedback revisions are duplicate or out of order: {revisions:?}"
    );
    Ok(())
}

async fn assert_trigger_evidence(
    api: &ApiClient,
    admin: &str,
    admin_id: &str,
    cycle_id: &str,
    reason: &str,
) -> Result<()> {
    let detail = ensure_status(
        api.get(
            &format!("/api/research/feedback-cycles/{cycle_id}"),
            Some(admin),
        )
        .await?,
        StatusCode::OK,
        "governed feedback detail",
    )
    .await?
    .json::<Value>()
    .await?;
    let provenance = detail["data"]["triggers"]
        .as_array()
        .context("feedback detail trigger provenance is not an array")?;
    let trigger_count = provenance
        .iter()
        .filter(|event| {
            event["trigger_family"] == "manual"
                && event["actor_user_id"] == admin_id
                && event["actor_label"] == "admin"
                && event["actor_role"] == "super_admin"
                && event["reason_code"] == reason
        })
        .count();
    ensure!(
        trigger_count == 1,
        "governed trigger actor/reason/idempotency provenance is invalid: {provenance:?}"
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let logs = ensure_status(
            api.get(
                &format!(
                    "/api/operation-logs?resource_type=materialization&resource_id={cycle_id}&page=1&size=20"
                ),
                Some(admin),
            )
            .await?,
            StatusCode::OK,
            "feedback operation log",
        )
        .await?
        .json::<Value>()
        .await?;
        let matching = logs["data"]["items"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|entry| entry["action"] == "feedback.cycle.trigger")
            .collect::<Vec<_>>();
        if matching.len() >= 2 {
            ensure!(
                matching.iter().all(|entry| {
                    entry["actor_user_id"] == admin_id
                        && entry["actor_username"] == "admin"
                        && entry["acting_role"] == "super_admin"
                        && entry["http_status"] == 202
                        && entry["outcome"] == "success"
                }),
                "governed trigger operation log lost actor/role/outcome: {matching:?}"
            );
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "governed trigger operation log did not flush before deadline: {logs}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn assert_permission_drift(api: &ApiClient, admin: &str, viewer: TestUser) -> Result<()> {
    let stale_ticket = api.ws_ticket(&viewer.login.access_token).await?;
    let live_ticket = api.ws_ticket(&viewer.login.access_token).await?;
    let mut socket = live_ticket.connect().await?;
    socket
        .subscribe(feedback_revision(api, &viewer.login.access_token).await?)
        .await?;
    socket.expect_sync().await?;

    ensure_status(
        api.put(
            &format!("/api/users/{}/roles", viewer.id),
            admin,
            json!({ "role_ids": [] }),
        )
        .await?,
        StatusCode::OK,
        "remove viewer roles",
    )
    .await?;
    socket.wait_closed().await?;
    let denied = ensure_status(
        api.get(
            "/api/research/feedback-overview",
            Some(&viewer.login.access_token),
        )
        .await?,
        StatusCode::FORBIDDEN,
        "permission-drift feedback read",
    )
    .await?
    .json::<Value>()
    .await?;
    ensure!(
        denied["message"] == "forbidden" && denied["data"].is_null(),
        "permission drift leaked authorization detail: {denied}"
    );
    stale_ticket.rejected().await?;

    let unauthorized_ticket = api.ws_ticket(&viewer.login.access_token).await?;
    let mut unauthorized = unauthorized_ticket.connect().await?;
    unauthorized.subscribe(0).await?;
    let error = unauthorized.expect_error("forbidden").await?;
    ensure!(
        error["data"]["channel"] == "research.feedback",
        "unauthorized WS error lost exact channel: {error}"
    );
    unauthorized.close().await?;
    Ok(())
}

async fn assert_redis_restart(
    stack: &ProductionStack,
    api: &ApiClient,
    admin: &Login,
) -> Result<()> {
    let stale_ticket = api.ws_ticket(&admin.access_token).await?;
    stack
        .with_redis_outage(|| async {
            let unavailable = ensure_status(
                api.get("/api/auth/me", Some(&admin.access_token)).await?,
                StatusCode::SERVICE_UNAVAILABLE,
                "protected request during Redis outage",
            )
            .await?
            .json::<Value>()
            .await?;
            ensure!(
                unavailable["message"] == "authentication temporarily unavailable"
                    && unavailable["data"].is_null(),
                "Redis outage lost its fail-closed authentication envelope: {unavailable}"
            );
            Ok(())
        })
        .await?;

    api.assert_public_probes().await?;
    let stale_access = ensure_status(
        api.get("/api/auth/me", Some(&admin.access_token)).await?,
        StatusCode::UNAUTHORIZED,
        "pre-restart access token after Redis state loss",
    )
    .await?
    .json::<Value>()
    .await?;
    ensure!(
        stale_access["message"] == "unauthorized" && stale_access["data"].is_null(),
        "lost refresh-family state leaked authentication detail: {stale_access}"
    );
    ensure_status(
        api.refresh(&admin.refresh_cookie).await?,
        StatusCode::UNAUTHORIZED,
        "pre-restart refresh token after Redis state loss",
    )
    .await?;
    stale_ticket.rejected().await?;

    let recovered = api.login("admin", BOOTSTRAP_ADMIN_PASSWORD).await?;
    assert_hardened_refresh_cookie(&recovered.refresh_cookie)?;
    assert_admin_identity(api, &recovered.access_token).await?;
    Ok(())
}

async fn ensure_status(
    response: Response,
    expected: StatusCode,
    operation: &str,
) -> Result<Response> {
    if response.status() == expected {
        return Ok(response);
    }
    let actual = response.status();
    let body = response.text().await.unwrap_or_default();
    bail!("{operation} returned {actual}, expected {expected}: {body}")
}
