//! Treasury and hot wallet configuration.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct TreasuryConfig {
    #[serde(default = "default_target_balance")]
    pub target_balance_usd: Decimal,
    #[serde(default = "default_refill_threshold")]
    pub refill_threshold_usd: Decimal,
    #[serde(default = "default_sweep_threshold")]
    pub sweep_threshold_usd: Decimal,
    #[serde(default)]
    pub hot_wallet: HotWalletConfig,
}

impl Default for TreasuryConfig {
    fn default() -> Self {
        Self {
            target_balance_usd: default_target_balance(),
            refill_threshold_usd: default_refill_threshold(),
            sweep_threshold_usd: default_sweep_threshold(),
            hot_wallet: HotWalletConfig::default(),
        }
    }
}

const fn default_target_balance() -> Decimal {
    dec!(1000)
}
const fn default_refill_threshold() -> Decimal {
    dec!(600)
}
const fn default_sweep_threshold() -> Decimal {
    dec!(1400)
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct HotWalletConfig {
    #[serde(default)]
    pub address: String,
    #[serde(default = "default_poll_interval")]
    pub balance_poll_interval_secs: u64,
}

impl Default for HotWalletConfig {
    fn default() -> Self {
        Self {
            address: String::new(),
            balance_poll_interval_secs: default_poll_interval(),
        }
    }
}

const fn default_poll_interval() -> u64 {
    60
}
