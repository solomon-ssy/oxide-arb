//! Integration-test harness.
//!
//! Wires the real Postgres-backed repositories and the Redis-backed JWT
//! blacklist into the production [`AppState`], and serves requests through an
//! actix `App` that mirrors `spawn_web_server` (request-id middleware + the
//! full route table). The app itself is stateless — all state lives in Postgres
//! and Redis — so [`call`] builds a fresh service per request, which keeps the
//! helper's return type nameable while still sharing the external backends.

use std::{sync::Arc, time::Duration};

use actix_web::{
    App,
    body::to_bytes,
    http::{StatusCode, header::HeaderMap},
    middleware::from_fn,
    test, web,
};
use async_trait::async_trait;
use oxide_arb_error::auth::AuthError;
use oxide_arb_models::{config::JwtConfig, domain::UserInfo};
use oxide_arb_repository::postgres::{PgMenuRepository, PgUserRepository, PgUserRoleRepository};
use oxide_arb_web::{
    AppState,
    jwt::{JwtService, RedisTokenBlacklist, TokenBlacklist},
    middleware, routes,
};
use serde_json::Value;
use testcontainers::ContainerAsync;
use testcontainers_modules::{postgres::Postgres, redis::Redis};

use crate::common::{pg, redis};

/// The version header every `/api/auth/*` request must carry to match the `v1`
/// scope (see `ApiV1Guard`).
pub const API_VERSION: (&str, &str) = ("Accept-Api-Version", "v1");

const TEST_JWT_SECRET: &str = "oxide-arb-integration-test-secret-not-for-production";
const TEST_ISSUER: &str = "oxide-arb-test";

/// A live test environment: migrated Postgres + running Redis behind the
/// production `AppState`.
pub struct TestEnv {
    /// The state injected into the actix app.
    pub state: AppState,
    // Postgres container guard. `DatabaseConnection` clones inside the
    // repositories keep the connection pool alive, so the pool wrapper itself
    // need not be retained — only the container must outlive the test.
    _pg: ContainerAsync<Postgres>,
    // Redis container guard, taken to simulate an outage (see `kill_redis`).
    redis: Option<ContainerAsync<Redis>>,
}

impl TestEnv {
    /// Bring up Postgres + Redis and assemble the production `AppState`.
    pub async fn start() -> Self {
        let (pool, pg_container) = pg::setup_pg().await;
        let (redis_url, redis_container) = redis::setup_redis().await;

        let blacklist =
            Arc::new(RedisTokenBlacklist::from_url(&redis_url).expect("redis blacklist"));
        let jwt = Arc::new(JwtService::new(&jwt_config(), blacklist));

        let db = pool.connection().clone();
        let state = AppState::new(
            jwt,
            Arc::new(PgUserRepository::new(db.clone())),
            Arc::new(PgUserRoleRepository::new(db.clone())),
            Arc::new(PgMenuRepository::new(db)),
        );

        Self {
            state,
            _pg: pg_container,
            redis: Some(redis_container),
        }
    }

    /// Simulate a Redis outage by tearing down the container. The blacklist pool
    /// can no longer reach a server, so authn must fail closed (HTTP 503).
    pub async fn kill_redis(&mut self) {
        drop(self.redis.take());
        // Allow container teardown to complete so subsequent connects fail fast.
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Mint an already-expired access token signed with the harness key. Lets tests
/// exercise the expired-token branch deterministically, without sleeping. Signed
/// with the same secret/issuer the harness `JwtService` uses, so it passes
/// signature + issuer validation and fails only on `exp`.
pub fn expired_access_token(user: &UserInfo) -> String {
    let mut cfg = jwt_config();
    cfg.access_ttl_secs = -10;
    let service = JwtService::new(&cfg, Arc::new(NoopBlacklist));
    service
        .encode_access(user)
        .expect("encode expired access token")
        .token
}

fn jwt_config() -> JwtConfig {
    JwtConfig {
        secret: TEST_JWT_SECRET.to_owned(),
        issuer: TEST_ISSUER.to_owned(),
        access_ttl_secs: 900,
        refresh_ttl_secs: 604_800,
    }
}

/// A captured HTTP response (status + headers + raw body), normalized so that
/// errors surfaced by middleware are rendered exactly as the production app
/// renders them — via `ResponseError` — rather than panicking the test.
pub struct Resp {
    /// HTTP status code.
    pub status: StatusCode,
    headers: HeaderMap,
    body: web::Bytes,
}

impl Resp {
    /// A response header value as a string slice, if present and valid UTF-8.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }

    /// Parse the body as JSON (panics if the body is not valid JSON).
    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).expect("response body must be valid JSON")
    }
}

/// Build a fresh service over `state` and execute a single request.
///
/// Handlers convert their `Err` into a `ServiceResponse` internally, but a
/// middleware (`authn`) returning `Err` bubbles up as a raw service error. The
/// real server renders that through `ResponseError`; we do the same here so the
/// unified error envelope is asserted on the exact bytes a client would see.
pub async fn call(state: &AppState, req: test::TestRequest) -> Resp {
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state.clone()))
            .wrap(from_fn(middleware::request_id))
            .configure(routes::configure),
    )
    .await;

    match test::try_call_service(&app, req.to_request()).await {
        Ok(res) => {
            let status = res.status();
            let headers = res.headers().clone();
            let body = test::read_body(res).await;
            Resp {
                status,
                headers,
                body,
            }
        }
        Err(err) => {
            let response = err.error_response();
            let status = response.status();
            let headers = response.headers().clone();
            let body = to_bytes(response.into_body())
                .await
                .expect("read error response body");
            Resp {
                status,
                headers,
                body,
            }
        }
    }
}

/// No-op blacklist used solely to mint test tokens; never consulted during the
/// authn decode path (decode rejects expired/wrong-type tokens before any
/// revocation check runs).
struct NoopBlacklist;

#[async_trait]
impl TokenBlacklist for NoopBlacklist {
    async fn revoke(&self, _jti: &str, _ttl: Duration) -> Result<(), AuthError> {
        Ok(())
    }

    async fn is_revoked(&self, _jti: &str) -> Result<bool, AuthError> {
        Ok(false)
    }
}
