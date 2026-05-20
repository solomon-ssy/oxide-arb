//! Settlement oracle configuration — `[settlement_oracle]` in TOML.
//!
//! Governs 3-source voting (Gamma + CTF on-chain + UMA) for endgame resolution
//! verification. Quorum defaults to 2-of-3 per ADR-001.

use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct SettlementOracleConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Minimum agreeing sources (default 2 = 2-of-3).
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

impl Default for SettlementOracleConfig {
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

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AllSourcesDownStrategy {
    ManualAck,
    #[default]
    ConservativeReject,
}
