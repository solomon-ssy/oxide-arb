//! Live Polymarket CLOB WebSocket: subscribe and receive an orderbook snapshot.
//!
//! Run (requires outbound network):
//! ```bash
//! cargo test -p oxide-arb-api --features integration -- --ignored ws_book
//! ```
//!
//! Optional env:
//! - `OXIDE_ARB_TEST_TOKEN_ID` — decimal CLOB token id (skips Gamma discovery)

use oxide_arb_api::gamma::GammaClient;
use oxide_arb_api::ws::ClobWsManager;
use oxide_arb_models::config::{GammaConfig, PolymarketConfig, WebSocketConfig};
use oxide_arb_models::domain::pipeline::PipelineEvent;
use oxide_arb_models::types::TokenId;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

async fn resolve_token_id() -> TokenId {
    if let Ok(id) = std::env::var("OXIDE_ARB_TEST_TOKEN_ID") {
        return TokenId::new(id);
    }
    let client = GammaClient::new(GammaConfig::default());
    client
        .discover_active_token()
        .await
        .expect("discover active token from Gamma")
}

#[tokio::test]
#[ignore = "requires live Polymarket Gamma + CLOB WebSocket"]
async fn ws_receives_book_snapshot_for_subscribed_token() {
    let token = resolve_token_id().await;
    let shutdown = CancellationToken::new();
    let manager = ClobWsManager::new(
        &PolymarketConfig::default(),
        &WebSocketConfig::default(),
        shutdown.clone(),
        None,
        None,
    );
    manager.subscribe(std::slice::from_ref(&token));
    let events = manager.events();

    let result = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let event = events.recv_async().await.expect("event channel open");
            match event {
                PipelineEvent::BookSnapshot(cmd)
                    if cmd.asset_id == token
                        && (!cmd.bids.levels.is_empty() || !cmd.asks.levels.is_empty()) =>
                {
                    return;
                }
                PipelineEvent::ShardStatus { status, .. } => {
                    tracing::debug!(?status, "shard status");
                }
                _ => {}
            }
        }
    })
    .await;

    shutdown.cancel();
    result.expect("timed out waiting for BookSnapshot with depth");
}
