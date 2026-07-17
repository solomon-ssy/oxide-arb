//! Auth-only integration-test helpers (token minting, Redis outage simulation).

use std::time::Duration;

use chrono::Utc;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use quant_pivot_models::domain::UserInfo;
use quant_pivot_web::jwt::{Claims, TokenUse};
use uuid::Uuid;

use crate::harness::TestEnv;

const TEST_ISSUER: &str = "quant-pivot-test";
const TEST_AUDIENCE: &str = "quant-pivot-web-test";
const TEST_SIGNING_KEY: &[u8] = &[7; 32];

/// Mint an already-expired access token signed with the harness key.
pub fn expired_access_token(user: &UserInfo) -> String {
    let now = Utc::now().timestamp();
    let claims = Claims {
        jti: Uuid::now_v7().to_string(),
        sub: user.id.to_string(),
        iss: TEST_ISSUER.to_owned(),
        aud: TEST_AUDIENCE.to_owned(),
        iat: now - 60,
        nbf: now - 60,
        exp: now - 10,
        username: user.username.clone(),
        token_use: TokenUse::Access,
        family_id: Uuid::now_v7().to_string(),
        session_exp: now - 10,
        generation: 0,
    };
    let mut header = Header::new(Algorithm::HS256);
    header.typ = Some("at+jwt".to_owned());
    encode(
        &header,
        &claims,
        &EncodingKey::from_secret(TEST_SIGNING_KEY),
    )
    .expect("encode expired access token")
}

/// Simulate a Redis outage by tearing down the container.
pub async fn kill_redis(env: &mut TestEnv) {
    drop(env.take_redis());
    tokio::time::sleep(Duration::from_millis(300)).await;
}
