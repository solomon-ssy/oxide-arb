//! Public HTTP, authentication, authorization, read-model, and WebSocket
//! contracts exercised through the real production binary.

use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use quant_pivot_system_tests::{
    production_stack::start_production_stack, stack::BOOTSTRAP_ADMIN_PASSWORD,
};
use reqwest::{
    Client, Response, StatusCode, header,
    header::{COOKIE, ORIGIN, SEC_WEBSOCKET_PROTOCOL, SET_COOKIE},
};
use serde_json::{Value, json};
use tokio::time::Instant;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest, http::HeaderValue},
};

const API_VERSION: (&str, &str) = ("accept-api-version", "v1");
const BROWSER_ORIGIN: &str = "http://127.0.0.1:6099";

struct ApiClient {
    base_url: String,
    http: Client,
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
}

struct Login {
    access_token: String,
    refresh_cookie: String,
}

#[tokio::test]
async fn production_web_boundary_end() -> Result<()> {
    let stack = start_production_stack().await?;
    let artifacts = stack.run_dir().display().to_string();
    let result = exercise_web_boundary(stack.base_url()).await;
    if let Err(error) = result {
        bail!("production web system contract failed; artifacts={artifacts}: {error:#}");
    }
    stack.stop(true).await
}

async fn exercise_web_boundary(base_url: &str) -> Result<()> {
    let api = ApiClient::new(base_url)?;
    api.assert_public_probes().await?;
    api.assert_version_authentication_boundary().await?;

    let admin = api.login("admin", BOOTSTRAP_ADMIN_PASSWORD).await?;
    assert_hardened_refresh_cookie(&admin.refresh_cookie)?;
    assert_admin_identity(&api, &admin.access_token).await?;
    assert_refresh_origin_boundary(&api, &admin.refresh_cookie).await?;

    let viewer = create_viewer(&api, &admin.access_token).await?;
    assert_authorization_boundary(&api, &viewer.access_token).await?;
    assert_read_models(&api, &admin.access_token, &viewer.access_token).await?;
    assert_websocket_contract(&api, base_url, &admin.access_token).await?;
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

async fn assert_admin_identity(api: &ApiClient, admin: &str) -> Result<()> {
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
    Ok(())
}

async fn assert_refresh_origin_boundary(api: &ApiClient, refresh_cookie: &str) -> Result<()> {
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

    let rotated = api
        .http
        .post(format!("{}/api/auth/refresh", api.base_url))
        .header(API_VERSION.0, API_VERSION.1)
        .header(COOKIE, cookie)
        .header(ORIGIN, BROWSER_ORIGIN)
        .header("sec-fetch-site", "same-origin")
        .send()
        .await?;
    ensure_status(rotated, StatusCode::OK, "same-origin refresh").await?;
    Ok(())
}

async fn create_viewer(api: &ApiClient, admin: &str) -> Result<Login> {
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
    api.login("production-stack-viewer", "viewer-password")
        .await
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

    let feature_contract = ensure_status(
        api.get("/api/research/feature-contract", Some(admin))
            .await?,
        StatusCode::OK,
        "feature contract",
    )
    .await?;
    ensure!(feature_contract.json::<Value>().await?["data"].is_object());

    for path in [
        "/api/quant/structural/negrisk-events",
        "/api/quant/structural/trade-tape/coverage",
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
        "research_readiness",
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

async fn assert_websocket_contract(api: &ApiClient, base_url: &str, admin: &str) -> Result<()> {
    let ticket = ensure_status(
        api.post("/api/ws/tickets", Some(admin), json!({})).await?,
        StatusCode::OK,
        "issue WebSocket ticket",
    )
    .await?
    .json::<Value>()
    .await?;
    let ticket = ticket["data"]["ticket"]
        .as_str()
        .context("WebSocket ticket is missing")?;
    let protocol = format!("qp-ticket.{ticket}");
    let ws_url = base_url.replacen("http://", "ws://", 1) + "/api/ws";
    let mut request = ws_url.into_client_request()?;
    request
        .headers_mut()
        .insert(ORIGIN, HeaderValue::from_static(BROWSER_ORIGIN));
    request
        .headers_mut()
        .insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_str(&protocol)?);
    let (mut socket, response) = connect_async(request).await?;
    ensure!(
        response
            .headers()
            .get(header::SEC_WEBSOCKET_PROTOCOL)
            .and_then(|value| value.to_str().ok())
            == Some(protocol.as_str()),
        "WebSocket ticket protocol was not echoed"
    );
    socket
        .send(Message::Text(
            json!({ "action": "ping" }).to_string().into(),
        ))
        .await?;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(!remaining.is_zero(), "WebSocket pong contract timed out");
        let message = tokio::time::timeout(remaining, socket.next())
            .await
            .context("WebSocket pong timeout")?
            .context("WebSocket closed before pong")??;
        if let Message::Text(text) = message {
            let body: Value = serde_json::from_str(&text)?;
            if body["type"] == "pong" {
                break;
            }
        }
    }
    socket.close(None).await?;

    let mut replay =
        (base_url.replacen("http://", "ws://", 1) + "/api/ws").into_client_request()?;
    replay
        .headers_mut()
        .insert(ORIGIN, HeaderValue::from_static(BROWSER_ORIGIN));
    replay
        .headers_mut()
        .insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_str(&protocol)?);
    ensure!(
        connect_async(replay).await.is_err(),
        "WebSocket ticket replay must fail closed"
    );
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
