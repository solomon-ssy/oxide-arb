//! Auth-only integration-test helpers (token minting, Redis outage simulation).

use std::{sync::Arc, time::Duration};

use oxide_arb_models::{config::JwtConfig, domain::UserInfo};
use oxide_arb_web::jwt::JwtService;

use crate::harness::{NoopBlacklist, TestEnv};

const TEST_JWT_SECRET: &str = "oxide-arb-integration-test-secret-not-for-production";
const TEST_ISSUER: &str = "oxide-arb-test";

/// Mint an already-expired access token signed with the harness key.
pub fn expired_access_token(user: &UserInfo) -> String {
    let mut cfg = jwt_config();
    cfg.access_ttl_secs = -10;
    let service = JwtService::new(&cfg, Arc::new(NoopBlacklist));
    service
        .encode_access(user)
        .expect("encode expired access token")
        .token
}

/// Simulate a Redis outage by tearing down the container.
pub async fn kill_redis(env: &mut TestEnv) {
    drop(env.take_redis());
    tokio::time::sleep(Duration::from_millis(300)).await;
}

fn jwt_config() -> JwtConfig {
    JwtConfig {
        secret: TEST_JWT_SECRET.to_owned(),
        issuer: TEST_ISSUER.to_owned(),
        access_ttl_secs: 900,
        refresh_ttl_secs: 604_800,
    }
}
