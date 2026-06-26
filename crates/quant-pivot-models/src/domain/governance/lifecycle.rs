//! Operator-facing runtime lifecycle — authoritative for header UI and order-submission gates.

use serde::{Deserialize, Serialize};

use crate::enums::execution::KillSwitchState;

/// Milliseconds without a CLOB websocket message before market data is stale.
pub const WS_MARKET_DATA_STALE_THRESHOLD_MS: u64 = 30_000;

/// Operator lifecycle phase (catalog, market data, breaker, control factors).
///
/// Single source of truth for readiness — clients must not re-derive phase
/// from raw subsystems.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum OperationalPhase {
    /// First Gamma catalog sync has not completed; detection is gated off.
    CatalogWarming,
    /// Catalog is ready but no fresh CLOB websocket book message has arrived yet.
    MarketDataConnecting,
    /// Catalog ready, market data fresh, breaker nominal, no degrade reasons.
    Operational,
    /// Runtime degradation after readiness prerequisites are met.
    Degraded {
        reasons: Vec<OperationalDegradeReason>,
    },
    /// Risk halt / execution kill switch latched.
    Halted,
}

/// Why the runtime is in the `Degraded` phase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationalDegradeReason {
    BreakerOpen,
    BreakerHalfOpen,
    MarketDataStale,
    MarketDataCoverageDegraded,
    SubsystemUnhealthy {
        name: String,
    },
    /// Kill-switch tightened (`report_only_forced` / `exit_only`); reports may
    /// continue but new entries are blocked (05.1 §6).
    KillSwitchTightened {
        state: KillSwitchState,
    },
}

/// CLOB websocket connectivity snapshot for operator dashboards.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataConnectivity {
    /// True when global traffic is fresh and every active WS transport shard is connected.
    pub ready: bool,
    pub last_message_age_ms: Option<u64>,
    pub ws_shards: WsShardConnectivity,
}

/// Aggregated per-shard websocket connection counters (display only).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WsShardConnectivity {
    pub total: u32,
    pub disconnected: u32,
    pub oldest_disconnected_secs: Option<u64>,
    /// Percentage of spawned WS transport shards that are currently connected.
    pub connected_ratio_bps: u32,
}

impl OperationalDegradeReason {
    /// Whether this degrade reason alone still permits report generation.
    #[must_use]
    pub const fn allows_report_generation(&self) -> bool {
        matches!(self, Self::KillSwitchTightened { .. })
    }
}

impl OperationalPhase {
    /// Whether order submission is permitted by the current lifecycle phase.
    #[must_use]
    pub const fn allows_order_submission(&self) -> bool {
        matches!(self, Self::Operational)
    }

    /// Whether report generation may run (catalog + market data ready, and any
    /// degrade reasons are kill-switch tightening only — not infra outage).
    #[must_use]
    pub fn allows_report_generation(&self) -> bool {
        match self {
            Self::Operational => true,
            Self::Degraded { reasons } => {
                !reasons.is_empty()
                    && reasons
                        .iter()
                        .all(OperationalDegradeReason::allows_report_generation)
            }
            Self::CatalogWarming | Self::MarketDataConnecting | Self::Halted => false,
        }
    }
}

impl MarketDataConnectivity {
    /// Build readiness from the last websocket message age and shard summary.
    #[must_use]
    pub fn from_parts(last_message_age_ms: Option<u64>, ws_shards: WsShardConnectivity) -> Self {
        let traffic_fresh =
            last_message_age_ms.is_some_and(|age| age < WS_MARKET_DATA_STALE_THRESHOLD_MS);
        let coverage_ready = ws_shards.total == 0 || ws_shards.disconnected == 0;
        let ready = traffic_fresh && coverage_ready;
        Self {
            ready,
            last_message_age_ms,
            ws_shards,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::OperationalDegradeReason;

    use super::{
        MarketDataConnectivity, OperationalPhase, WS_MARKET_DATA_STALE_THRESHOLD_MS,
        WsShardConnectivity,
    };

    #[test]
    fn operational_phase_allows_submission_only_when_operational() {
        assert!(!OperationalPhase::CatalogWarming.allows_order_submission());
        assert!(!OperationalPhase::MarketDataConnecting.allows_order_submission());
        assert!(OperationalPhase::Operational.allows_order_submission());
        assert!(!OperationalPhase::Degraded { reasons: vec![] }.allows_order_submission());
        assert!(!OperationalPhase::Halted.allows_order_submission());
    }

    #[test]
    fn market_data_ready_when_message_fresh() {
        let fresh = MarketDataConnectivity::from_parts(
            Some(WS_MARKET_DATA_STALE_THRESHOLD_MS - 1),
            WsShardConnectivity {
                total: 10,
                disconnected: 0,
                oldest_disconnected_secs: None,
                connected_ratio_bps: 10_000,
            },
        );
        assert!(fresh.ready);

        let degraded = MarketDataConnectivity::from_parts(
            Some(WS_MARKET_DATA_STALE_THRESHOLD_MS - 1),
            WsShardConnectivity {
                total: 10,
                disconnected: 5,
                oldest_disconnected_secs: Some(12),
                connected_ratio_bps: 5_000,
            },
        );
        assert!(!degraded.ready);

        let stale = MarketDataConnectivity::from_parts(
            Some(WS_MARKET_DATA_STALE_THRESHOLD_MS),
            WsShardConnectivity {
                total: 0,
                disconnected: 0,
                oldest_disconnected_secs: None,
                connected_ratio_bps: 10_000,
            },
        );
        assert!(!stale.ready);
    }

    #[test]
    fn kill_switch_tightened_degraded_still_allows_reports() {
        let phase = OperationalPhase::Degraded {
            reasons: vec![OperationalDegradeReason::KillSwitchTightened {
                state: crate::enums::execution::KillSwitchState::ReportOnlyForced,
            }],
        };
        assert!(phase.allows_report_generation());
        assert!(!phase.allows_order_submission());
    }

    #[test]
    fn operational_phase_serde_round_trip() {
        let phase = OperationalPhase::Degraded {
            reasons: vec![super::OperationalDegradeReason::MarketDataStale],
        };
        let json = serde_json::to_string(&phase).expect("serialize");
        let decoded: OperationalPhase = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(phase, decoded);
    }
}
