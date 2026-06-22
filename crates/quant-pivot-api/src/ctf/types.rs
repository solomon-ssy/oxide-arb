use quant_pivot_models::{
    enums::legacy::LegacyExecutionMode,
    runtime_config::ResolvedRedeemPlan,
    types::{MarketId, TokenId, Usd},
};

#[derive(Debug, Clone)]
pub struct RedeemRequest {
    pub condition_id: MarketId,
    pub market_id: MarketId,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub execution_mode: LegacyExecutionMode,
    /// Immutable redeem plan snapshotted on the position at fill time.
    pub plan: ResolvedRedeemPlan,
}

#[derive(Debug, Clone)]
pub struct RedeemOutcome {
    pub tx_hash: Option<String>,
    pub simulated: bool,
    /// Gas cost in USD when redeem was executed on-chain.
    pub gas_paid_usd: Option<Usd>,
}

impl RedeemOutcome {
    #[must_use]
    pub const fn dry_run() -> Self {
        Self {
            tx_hash: None,
            simulated: true,
            gas_paid_usd: Some(Usd::ZERO),
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
            gas_paid_usd: Some(Usd::ZERO),
        }
    }

    #[must_use]
    pub const fn live(tx_hash: String, gas_paid_usd: Option<Usd>) -> Self {
        Self {
            tx_hash: Some(tx_hash),
            simulated: false,
            gas_paid_usd,
        }
    }
}
