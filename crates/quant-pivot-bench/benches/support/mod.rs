use std::sync::Arc;

use chrono::Utc;
use quant_pivot_core::ingest::{data_plane_index::DataPlane, market_registry::MarketRegistry};
use quant_pivot_models::{
    domain::market::{MarketRegistryInfo, TokenInfo},
    enums::{
        catalog::CatalogFilterReasonSet,
        common::{CategorySet, MarketCategory, TickSize},
        market::MarketStatus,
    },
    types::{EventId, MarketId, TokenId, TokenKey},
};
use rust_decimal_macros::dec;

pub fn registered_data_plane(token: &str) -> (Arc<DataPlane>, TokenKey) {
    let data_plane = Arc::new(DataPlane::new());
    let registry = MarketRegistry::new(Arc::clone(&data_plane));
    let token_yes = TokenId::new(token);
    let token_no = TokenId::new(format!("{token}-no"));
    registry.register_market(MarketRegistryInfo {
        market_id: MarketId::new("bench-market"),
        event_id: EventId::new("bench-event"),
        token_yes: token_yes.clone(),
        token_no: token_no.clone(),
        question: "Benchmark?".to_owned(),
        slug: "benchmark".to_owned(),
        description: None,
        categories: CategorySet::from(MarketCategory::Other),
        status: MarketStatus::Active,
        filter_reasons: CatalogFilterReasonSet::default(),
        outcome: None,
        neg_risk: false,
        tick_size: TickSize::Hundredth,
        tokens: vec![
            TokenInfo {
                token_id: token_yes.clone(),
                outcome: "Yes".to_owned(),
                neg_risk: false,
            },
            TokenInfo {
                token_id: token_no,
                outcome: "No".to_owned(),
                neg_risk: false,
            },
        ],
        best_bid: None,
        best_ask: None,
        depth_usd: None,
        min_order_size: dec!(5),
        liquidity_usd: None,
        volume_24h: None,
        maker_rebate_schedule: None,
        start_date: None,
        end_date: None,
        resolved_at: None,
        created_at: Some(Utc::now()),
        updated_at: Utc::now(),
    });
    let key = data_plane.token_key(&token_yes).expect("registered token");
    (data_plane, key)
}
