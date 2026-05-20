use oxide_arb_api::gamma::GammaClient;
use oxide_arb_models::config::GammaConfig;

#[tokio::test]
#[ignore = "requires network"]
async fn gamma_full_sync_fetches_events() {
    let client = GammaClient::new(GammaConfig::default());
    let events = client.full_sync().await.expect("full_sync");
    assert!(!events.is_empty());
}
