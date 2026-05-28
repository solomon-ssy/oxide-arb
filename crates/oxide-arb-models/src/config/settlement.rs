//! Unified settlement configuration.

use super::{
    market_data::GammaConfig,
    polymarket::{OnchainConfig, PolymarketConfig},
};
use crate::{
    constants::POLYGON_CHAIN_ID,
    enums::common::{RedeemOutputAsset, RedeemRoute},
};
use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Clone, Default, Deserialize, Validate)]
pub struct SettlementConfig {
    #[serde(default)]
    pub oracle: SettlementOracleSection,
    #[serde(default)]
    pub lifecycle: SettlementLifecycleSection,
    #[serde(default)]
    pub contracts: SettlementContractsSection,
    #[serde(default)]
    pub redeem: SettlementRedeemSection,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct SettlementOracleSection {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default = "default_quorum")]
    pub voting_quorum: u8,
    #[serde(default = "default_cross_check_delay")]
    pub cross_check_delay_secs: u64,
    #[serde(default)]
    pub all_sources_down_strategy: AllSourcesDownStrategy,
    #[serde(default = "default_uma_endpoint")]
    pub uma_endpoint: String,
    #[serde(default = "default_uma_timeout")]
    pub uma_timeout_secs: u64,
}

impl Default for SettlementOracleSection {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            voting_quorum: default_quorum(),
            cross_check_delay_secs: default_cross_check_delay(),
            all_sources_down_strategy: AllSourcesDownStrategy::default(),
            uma_endpoint: default_uma_endpoint(),
            uma_timeout_secs: default_uma_timeout(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct SettlementLifecycleSection {
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
    #[serde(default = "default_retry_interval_secs")]
    pub retry_interval_secs: u64,
    #[serde(default = "default_max_redeem_attempts")]
    pub max_redeem_attempts: u32,
    #[serde(default = "default_dedup_window_secs")]
    pub dedup_window_secs: u64,
    #[serde(default = "default_true")]
    pub settle_fail_closed: bool,
}

impl Default for SettlementLifecycleSection {
    fn default() -> Self {
        Self {
            channel_capacity: default_channel_capacity(),
            retry_interval_secs: default_retry_interval_secs(),
            max_redeem_attempts: default_max_redeem_attempts(),
            dedup_window_secs: default_dedup_window_secs(),
            settle_fail_closed: default_true(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct SettlementContractsSection {
    #[serde(default = "default_ctf_address")]
    pub ctf_address: String,
    #[serde(default = "default_usdc_e_address")]
    pub usdc_e_address: String,
    #[serde(default = "default_standard_ctf_exchange")]
    pub standard_ctf_exchange: String,
    #[serde(default = "default_neg_risk_ctf_exchange")]
    pub neg_risk_ctf_exchange: String,
    #[serde(default)]
    pub neg_risk_adapter: Option<String>,
    #[serde(default)]
    pub ctf_collateral_adapter: Option<String>,
    #[serde(default)]
    pub neg_risk_collateral_adapter: Option<String>,
}

impl Default for SettlementContractsSection {
    fn default() -> Self {
        Self {
            ctf_address: default_ctf_address(),
            usdc_e_address: default_usdc_e_address(),
            standard_ctf_exchange: default_standard_ctf_exchange(),
            neg_risk_ctf_exchange: default_neg_risk_ctf_exchange(),
            neg_risk_adapter: None,
            ctf_collateral_adapter: None,
            neg_risk_collateral_adapter: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct SettlementRedeemSection {
    #[serde(default)]
    pub route: RedeemRoute,
    #[serde(default)]
    pub output_asset: RedeemOutputAsset,
    #[serde(default)]
    pub holder_address: Option<String>,
    #[serde(default)]
    pub proxy_safe_address: Option<String>,
    #[serde(default = "default_redeem_gas_limit")]
    pub gas_limit: u64,
}

impl Default for SettlementRedeemSection {
    fn default() -> Self {
        Self {
            route: RedeemRoute::default(),
            output_asset: RedeemOutputAsset::default(),
            holder_address: None,
            proxy_safe_address: None,
            gas_limit: default_redeem_gas_limit(),
        }
    }
}

impl SettlementConfig {
    #[must_use]
    pub fn default_onchain_config(&self) -> OnchainConfig {
        PolymarketConfig::default().onchain
    }

    #[must_use]
    pub fn default_gamma_config(&self) -> GammaConfig {
        GammaConfig::default()
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AllSourcesDownStrategy {
    ManualAck,
    #[default]
    ConservativeReject,
}

const fn default_enabled() -> bool {
    true
}

const fn default_quorum() -> u8 {
    2
}

const fn default_cross_check_delay() -> u64 {
    120
}

fn default_uma_endpoint() -> String {
    "https://api.uma.xyz".into()
}

const fn default_uma_timeout() -> u64 {
    10
}

const fn default_channel_capacity() -> usize {
    256
}

const fn default_retry_interval_secs() -> u64 {
    60
}

const fn default_max_redeem_attempts() -> u32 {
    5
}

const fn default_dedup_window_secs() -> u64 {
    30
}

const fn default_true() -> bool {
    true
}

const fn default_redeem_gas_limit() -> u64 {
    500_000
}

fn default_ctf_address() -> String {
    "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045".into()
}

fn default_usdc_e_address() -> String {
    "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174".into()
}

fn default_standard_ctf_exchange() -> String {
    "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E".into()
}

fn default_neg_risk_ctf_exchange() -> String {
    "0xC5d563A36AE78145C45a50134d48A1215220f80a".into()
}

#[must_use]
pub const fn expected_chain_id() -> u64 {
    POLYGON_CHAIN_ID
}
