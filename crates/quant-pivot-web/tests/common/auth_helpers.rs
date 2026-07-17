//! Auth-only integration-test helpers (token minting, Redis outage simulation).

use std::{fs, time::Duration};

use chrono::Utc;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use quant_pivot_models::domain::UserInfo;
use quant_pivot_web::jwt::{Claims, TokenType};
use uuid::Uuid;

use crate::harness::TestEnv;

const TEST_ISSUER: &str = "quant-pivot-test";

/// Mint an already-expired access token signed with the harness key.
pub fn expired_access_token(user: &UserInfo) -> String {
    let now = Utc::now().timestamp();
    let claims = Claims {
        jti: Uuid::now_v7().to_string(),
        sub: user.id.to_string(),
        iss: TEST_ISSUER.to_owned(),
        iat: now - 60,
        nbf: now - 60,
        exp: now - 10,
        username: user.username.clone(),
        token_type: TokenType::Access,
        family_id: Uuid::now_v7().to_string(),
        session_exp: now - 10,
        generation: 0,
    };
    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some("test-2026-01".to_owned());
    let private_key = fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/jwt/test-ed25519-private.pem"
    ))
    .expect("test JWT private key");
    encode(
        &header,
        &claims,
        &EncodingKey::from_ed_pem(&private_key).expect("test Ed25519 key"),
    )
    .expect("encode expired access token")
}

/// Simulate a Redis outage by tearing down the container.
pub async fn kill_redis(env: &mut TestEnv) {
    drop(env.take_redis());
    tokio::time::sleep(Duration::from_millis(300)).await;
}
