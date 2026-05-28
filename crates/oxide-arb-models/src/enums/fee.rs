//! Fee authority enums.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeSource {
    ClobMarketInfo,
    GammaFeeSchedule,
    CategoryDefault,
}

impl FeeSource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClobMarketInfo => "clob_market_info",
            Self::GammaFeeSchedule => "gamma_fee_schedule",
            Self::CategoryDefault => "category_default",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeLiquidityRole {
    Taker,
    Maker,
}
