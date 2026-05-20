//! Integration tests that connect to real Polymarket APIs.
//!
//! These tests are gated behind the `integration` feature flag and require
//! network access. They perform read-only operations only (no orders).
//!
//! Run with: `cargo test -p oxide-arb-api --features integration -- --ignored`

#![cfg(feature = "integration")]

use oxide_arb_api::gamma::GammaClient;
use oxide_arb_models::config::GammaConfig;

#[tokio::test]
#[ignore]
async fn gamma_full_sync_fetches_events() {
    let config = GammaConfig::default();
    let client = GammaClient::new(config);

    let events = client.full_sync().await;
    assert!(events.is_ok(), "Full sync failed: {:?}", events.err());

    let events = events.unwrap();
    assert!(!events.is_empty(), "Expected at least one event from Gamma");
    println!("Fetched {} events from Gamma API", events.len());
}
