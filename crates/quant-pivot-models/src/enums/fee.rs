//! Fee authority enums.

crate::pg_enum! {
    type_name = "qp_fee_source",
    pub enum FeeSource {
        ClobMarketInfo => "clob_market_info",
        GammaFeeSchedule => "gamma_fee_schedule",
        CategoryDefault => "category_default",
    }
}

/// Fee liquidity role on the wire (not persisted to Postgres).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeLiquidityRole {
    Taker,
    Maker,
}
