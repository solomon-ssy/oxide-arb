//! Settlement structural configuration (`[settlement]`, deploy).
//!
//! Only the settlement request channel capacity is structural. Operational
//! settlement tunables (oracle policy, retries, redeem route) are runtime
//! configuration (`runtime_config::SettlementRuntimeConfig`); contract
//! addresses are compiled-in chain facts (`crate::constants`).

use serde::Deserialize;

/// Settlement structural parameters.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SettlementDeployConfig {
    /// Settlement lifecycle channel topology.
    pub lifecycle: SettlementLifecycleDeployConfig,
}

/// Settlement lifecycle channel topology.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SettlementLifecycleDeployConfig {
    /// Bounded capacity of the market-settlement request channel between the
    /// data pipeline and the settlement worker. Default: `256`.
    pub channel_capacity: usize,
}

impl Default for SettlementLifecycleDeployConfig {
    fn default() -> Self {
        Self {
            channel_capacity: default_channel_capacity(),
        }
    }
}

const fn default_channel_capacity() -> usize {
    256
}
