use oxide_arb_models::{
    enums::common::ExecutionMode,
    types::{MarketId, TokenId},
};

#[derive(Debug, Clone)]
pub struct RedeemRequest {
    pub condition_id: MarketId,
    pub market_id: MarketId,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub neg_risk: bool,
    pub execution_mode: ExecutionMode,
}

#[derive(Debug, Clone)]
pub struct RedeemOutcome {
    pub tx_hash: Option<String>,
    pub simulated: bool,
}

impl RedeemOutcome {
    #[must_use]
    pub const fn dry_run() -> Self {
        Self {
            tx_hash: None,
            simulated: true,
        }
    }

    #[must_use]
    pub fn paper(condition_id: &MarketId) -> Self {
        Self {
            tx_hash: Some(format!(
                "0xpaper{}",
                condition_id.as_str().trim_start_matches("0x")
            )),
            simulated: true,
        }
    }

    #[must_use]
    pub const fn live(tx_hash: String) -> Self {
        Self {
            tx_hash: Some(tx_hash),
            simulated: false,
        }
    }
}
