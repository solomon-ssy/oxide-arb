use quant_pivot_api::gamma::GammaClient;
use quant_pivot_models::config::GammaConfig;

#[tokio::test]
#[ignore = "requires live Polymarket Gamma API"]
async fn gamma_full_sync_fetches_events() {
    let client = GammaClient::new(GammaConfig::default());
    let events = client.full_sync().await.expect("full_sync");
    assert!(!events.is_empty());
}
