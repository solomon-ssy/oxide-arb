use oxide_arb_algorithm::fee::FeeEstimator;
use oxide_arb_api::fees::FeeCalculator;
use oxide_arb_models::{
    enums::common::MarketCategory,
    types::{MarketId, Price, Shares, TokenId, Usd},
};
use std::sync::Arc;

pub struct CoreFeeEstimator(pub Arc<FeeCalculator>);

impl FeeEstimator for CoreFeeEstimator {
    fn estimate_fee(
        &self,
        shares: Shares,
        price: Price,
        category: MarketCategory,
        token_id: &TokenId,
    ) -> Usd {
        self.0.calculate(
            shares,
            price,
            category,
            &MarketId::new(token_id.as_str()),
            token_id,
        )
    }
}
