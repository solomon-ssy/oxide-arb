//! Pre-defined seed plans for bootstrap migrations.

use super::SeedPlan;
use super::risk_engine_state::RiskEngineStateSeed;
use super::runtime_config::RuntimeConfigSeed;

/// Trading bootstrap v1: risk engine singleton + runtime configuration defaults.
///
/// Applied by `m20250601_000015_seed_trading_bootstrap`. Contains:
/// - `risk_engine_state` (order 10): singleton row with `InsertIfAbsent`
/// - `runtime_config` (order 20): one row per `RuntimeConfigKey` with `InsertKeyIfAbsent`
pub fn trading_bootstrap_v1() -> SeedPlan {
    SeedPlan::new(
        "trading_bootstrap_v1",
        vec![Box::new(RiskEngineStateSeed), Box::new(RuntimeConfigSeed)],
    )
}
