//! Settlement runtime configuration (`settlement` section).
//!
//! Operational settlement tunables only: the oracle voting policy, lifecycle
//! retry/dedup behaviour, and the on-chain redeem route. Contract addresses
//! are immutable chain facts and live in [`crate::constants`]; the settlement
//! channel capacity is structural and lives in `config::SettlementDeployConfig`.

use crate::enums::common::{RedeemOutputAsset, RedeemRoute};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Settlement operational tunables (hot-reloadable).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SettlementRuntimeConfig {
    /// Multi-source resolution oracle policy.
    pub oracle: SettlementOracleConfig,
    /// Settlement lifecycle (retry / dedup) policy.
    pub lifecycle: SettlementLifecycleConfig,
    /// On-chain redemption route and parameters.
    pub redeem: SettlementRedeemConfig,
}

/// Multi-source resolution oracle policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SettlementOracleConfig {
    /// Whether the post-settlement oracle cross-check audit runs. Disabling
    /// skips the audit only — settlement itself still requires a resolution
    /// verdict. Default: `true`.
    pub enabled: bool,
    /// Sources that must agree before a resolution verdict is accepted.
    /// Default: `2` (of Gamma / CTF / UMA).
    pub voting_quorum: u8,
    /// Delay (seconds) before the post-settlement cross-check re-queries
    /// sources. Default: `120`.
    pub cross_check_delay_secs: u64,
    /// Behaviour when every oracle source is unavailable. Default:
    /// `conservative_reject` (fail-closed; never settle blind).
    pub all_sources_down_strategy: AllSourcesDownStrategy,
    /// UMA optimistic-oracle API endpoint. Default: `https://api.uma.xyz`.
    pub uma_endpoint: String,
    /// UMA request timeout (seconds). Default: `10`.
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

/// Settlement lifecycle (retry / dedup) policy.
///
/// Settlement error handling is fail-closed by design (errors halt the
/// affected position, never skip it) — that invariant is not a tunable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SettlementLifecycleConfig {
    /// Interval (seconds) between retry sweeps over failed settlements.
    /// Default: `60`.
    pub retry_interval_secs: u64,
    /// Maximum redeem attempts per position before terminal failure (operator
    /// alert + manual intervention). Default: `5`.
    pub max_redeem_attempts: u32,
    /// Window (seconds) for deduplicating settlement triggers for the same
    /// market. Caution: shrinking it mid-flight admits duplicate triggers for
    /// markets settled within the old window. Default: `30`.
    pub dedup_window_secs: u64,
}

impl Default for SettlementLifecycleConfig {
    fn default() -> Self {
        Self {
            retry_interval_secs: default_retry_interval_secs(),
            max_redeem_attempts: default_max_redeem_attempts(),
            dedup_window_secs: default_dedup_window_secs(),
        }
    }
}

/// On-chain redemption route and parameters.
///
/// Contract addresses for every route are compiled in (`crate::constants`);
/// only the route selection and gas / holder parameters are configurable.
/// The whole section is money-critical: it controls on-chain redemption of
/// settled positions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
#[schemars(extend("x-money-critical" = true))]
pub struct SettlementRedeemConfig {
    /// Active redemption route. `disabled` blocks Live redemption (fail-closed:
    /// Live mode validation requires an explicit route). Default: `disabled`.
    #[schemars(with = "String")]
    pub route: RedeemRoute,
    /// Output asset for adapter routes. Default: `usdc_e`.
    #[schemars(with = "String")]
    pub output_asset: RedeemOutputAsset,
    /// Token holder address when it differs from the signer (e.g. proxy
    /// wallet). `None` uses the signer address. Default: `None`.
    pub holder_address: Option<String>,
    /// Gnosis Safe address for the `proxy_safe` route. Required by Live-mode
    /// validation when that route is selected. Default: `None`.
    pub proxy_safe_address: Option<String>,
    /// Gas limit for redeem transactions. Default: `500000`.
    pub gas_limit: u64,
}

impl Default for SettlementRedeemConfig {
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

/// Behaviour when every oracle source is unavailable.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AllSourcesDownStrategy {
    /// Park the settlement until an operator acknowledges it.
    ManualAck,
    /// Reject the settlement attempt (retried by the lifecycle sweep).
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

const fn default_retry_interval_secs() -> u64 {
    60
}

const fn default_max_redeem_attempts() -> u32 {
    5
}

const fn default_dedup_window_secs() -> u64 {
    30
}

const fn default_redeem_gas_limit() -> u64 {
    500_000
}
