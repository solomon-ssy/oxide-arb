//! Settlement runtime configuration (`settlement` section).
//!
//! Operational settlement tunables only: the oracle voting policy, lifecycle
//! retry/dedup behaviour, and the on-chain redeem routing policy. Contract
//! addresses are immutable chain facts and live in [`crate::constants`]; the
//! settlement channel capacity is structural and lives in
//! `config::SettlementDeployConfig`.

use crate::{
    enums::common::{
        NegRiskRedeemRoute, RedeemResolutionSource, ResolvedRedeemRoute, StandardRedeemRoute,
    },
    types::MarketId,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Settlement operational tunables (hot-reloadable).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SettlementRuntimeConfig {
    /// Multi-source resolution oracle policy.
    pub oracle: SettlementOracleConfig,
    /// Settlement lifecycle (retry / dedup) policy.
    pub lifecycle: SettlementLifecycleConfig,
    /// On-chain redemption routing policy (per-market class + overrides).
    pub redeem: RedeemRoutingPolicy,
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
    #[schemars(extend("x-format" = "integer"))]
    pub voting_quorum: u8,
    /// Delay (seconds) before the post-settlement cross-check re-queries
    /// sources. Default: `120`.
    #[schemars(extend("x-format" = "integer"))]
    pub cross_check_delay_secs: u64,
    /// Behaviour when every oracle source is unavailable. Default:
    /// `conservative_reject` (fail-closed; never settle blind).
    #[schemars(extend("x-enum-id" = "all_sources_down_strategy"))]
    pub all_sources_down_strategy: AllSourcesDownStrategy,
    /// UMA optimistic-oracle API endpoint. Default: `https://api.uma.xyz`.
    pub uma_endpoint: String,
    /// UMA request timeout (seconds). Default: `10`.
    #[schemars(extend("x-format" = "integer"))]
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
    #[schemars(extend("x-format" = "integer"))]
    pub retry_interval_secs: u64,
    /// Maximum redeem attempts per position before terminal failure (operator
    /// alert + manual intervention). Default: `5`.
    #[schemars(extend("x-format" = "integer"))]
    pub max_redeem_attempts: u32,
    /// Window (seconds) for deduplicating settlement triggers for the same
    /// market. Caution: shrinking it mid-flight admits duplicate triggers for
    /// markets settled within the old window. Default: `30`.
    #[schemars(extend("x-format" = "integer"))]
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

/// Money-critical on-chain redemption routing policy.
///
/// Resolves a per-market redeem plan from class defaults (`standard` /
/// `neg_risk`) and optional per-market overrides. Positions snapshot the
/// resolved plan at fill time; settlement never re-reads live config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
#[schemars(extend("x-money-critical" = true))]
pub struct RedeemRoutingPolicy {
    /// Standard (`neg_risk = false`) markets. `None` = class unsupported in Live.
    pub standard: Option<StandardRedeemPolicy>,
    /// Neg-risk markets. `None` = class unsupported in Live.
    pub neg_risk: Option<NegRiskRedeemPolicy>,
    /// Per-market overrides; wins over class policy. Keys are Polymarket
    /// `condition_id` values (`MarketId`).
    #[schemars(with = "HashMap<String, String>")]
    pub overrides: HashMap<MarketId, RedeemClassPolicy>,
    /// Gas limit for redeem transactions. Default: `500000`.
    #[schemars(extend("x-format" = "integer"))]
    pub gas_limit: u64,
}

impl Default for RedeemRoutingPolicy {
    fn default() -> Self {
        Self {
            standard: Some(StandardRedeemPolicy::default()),
            neg_risk: Some(NegRiskRedeemPolicy::default()),
            overrides: HashMap::new(),
            gas_limit: default_redeem_gas_limit(),
        }
    }
}

/// Redeem policy for standard (non-neg-risk) markets.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct StandardRedeemPolicy {
    /// On-chain redeem path for standard (non-neg-risk) markets in Live mode.
    #[schemars(with = "String", extend("x-enum-id" = "standard_redeem_route"))]
    pub route: StandardRedeemRoute,
    /// Token holder when it differs from the signer EOA. `None` uses the signer.
    pub holder_address: Option<String>,
}

/// Redeem policy for neg-risk markets.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct NegRiskRedeemPolicy {
    /// On-chain redeem path for neg-risk markets in Live mode.
    #[schemars(with = "String", extend("x-enum-id" = "neg_risk_redeem_route"))]
    pub route: NegRiskRedeemRoute,
    /// Token holder when it differs from the signer EOA. `None` uses the signer.
    pub holder_address: Option<String>,
}

/// Per-market override redeem policy (variant must match market `neg_risk`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RedeemClassPolicy {
    Standard(StandardRedeemPolicy),
    NegRisk(NegRiskRedeemPolicy),
}

impl RedeemClassPolicy {
    #[must_use]
    pub const fn expects_neg_risk(&self) -> bool {
        matches!(self, Self::NegRisk(_))
    }

    #[must_use]
    pub fn holder_address(&self) -> Option<&str> {
        match self {
            Self::Standard(policy) => policy.holder_address.as_deref(),
            Self::NegRisk(policy) => policy.holder_address.as_deref(),
        }
    }

    #[must_use]
    pub fn route(&self) -> ResolvedRedeemRoute {
        match self {
            Self::Standard(policy) => policy.route.into(),
            Self::NegRisk(policy) => policy.route.into(),
        }
    }
}

/// Resolved redeem execution plan (snapshotted on position at fill time).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedRedeemPlan {
    pub route: ResolvedRedeemRoute,
    pub holder_address: Option<String>,
    pub neg_risk: bool,
    pub gas_limit: u64,
    pub resolution: RedeemResolutionSource,
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
