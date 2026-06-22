//! Authoritative operator lifecycle evaluation shared by system status,
//! health checks, and Live preflight.

use oxide_arb_api::ws::ClobWsManager;
use oxide_arb_models::{
    domain::{
        CatalogStatusPort, MarketDataConnectivity, OperationalDegradeReason, OperationalPhase,
        WsShardConnectivity,
    },
    enums::risk::BreakerStateName,
};

/// Build lifecycle inputs from a precomputed snapshot (shared assembly point).
#[must_use]
pub fn lifecycle_inputs<'a>(
    catalog: &'a dyn CatalogStatusPort,
    ws_manager: &'a ClobWsManager,
    snap: &'a LifecycleSnapshot,
) -> LifecycleInputs<'a> {
    LifecycleInputs {
        catalog,
        ws_manager,
        breaker_state: snap.breaker_state,
        control_factor_snapshot_expired: snap.control_factor_snapshot_expired,
        control_factor_live_warn: snap.control_factor_live_warn,
        unhealthy_subsystems: &snap.unhealthy,
    }
}

/// Inputs for a lock-free lifecycle evaluation (no I/O).
pub struct LifecycleInputs<'a> {
    pub catalog: &'a dyn CatalogStatusPort,
    pub ws_manager: &'a ClobWsManager,
    pub breaker_state: BreakerStateName,
    pub control_factor_snapshot_expired: bool,
    pub control_factor_live_warn: bool,
    /// Subsystem names currently failing health probes (empty during warmup skips).
    pub unhealthy_subsystems: &'a [String],
}

/// Cached control-plane inputs shared by status assembly and health checks.
#[derive(Debug, Clone)]
pub struct LifecycleSnapshot {
    pub unhealthy: Vec<String>,
    pub breaker_state: BreakerStateName,
    pub control_factor_snapshot_expired: bool,
    pub control_factor_live_warn: bool,
}

/// Evaluate the operator-facing lifecycle phase from live subsystem handles.
#[must_use]
pub fn evaluate_lifecycle(
    inputs: &LifecycleInputs<'_>,
) -> (OperationalPhase, MarketDataConnectivity) {
    let shard_summary = inputs.ws_manager.shard_health();
    let market_data = MarketDataConnectivity::from_parts(
        inputs.ws_manager.last_message_age_ms(),
        WsShardConnectivity {
            total: u32::try_from(shard_summary.total).unwrap_or(u32::MAX),
            disconnected: u32::try_from(shard_summary.disconnected).unwrap_or(u32::MAX),
            oldest_disconnected_secs: shard_summary.oldest_disconnected_secs,
            connected_ratio_bps: shard_summary.connected_ratio_bps,
        },
    );

    if inputs.breaker_state == BreakerStateName::Halted {
        return (OperationalPhase::Halted, market_data);
    }

    if !inputs.catalog.is_ready() {
        return (OperationalPhase::CatalogWarming, market_data);
    }

    if !market_data.ready {
        if market_data.last_message_age_ms.is_some() {
            let reason = if market_data.ws_shards.disconnected > 0 {
                OperationalDegradeReason::MarketDataCoverageDegraded
            } else {
                OperationalDegradeReason::MarketDataStale
            };
            return (
                OperationalPhase::Degraded {
                    reasons: vec![reason],
                },
                market_data,
            );
        }
        return (OperationalPhase::MarketDataConnecting, market_data);
    }

    let mut reasons = Vec::new();
    match inputs.breaker_state {
        BreakerStateName::Open => reasons.push(OperationalDegradeReason::BreakerOpen),
        BreakerStateName::HalfOpen => reasons.push(OperationalDegradeReason::BreakerHalfOpen),
        BreakerStateName::Closed | BreakerStateName::Recovered | BreakerStateName::Halted => {}
    }
    if inputs.control_factor_snapshot_expired {
        reasons.push(OperationalDegradeReason::ControlFactorSnapshotExpired);
    }
    if inputs.control_factor_live_warn {
        reasons.push(OperationalDegradeReason::ControlFactorLiveWarn);
    }
    for name in inputs.unhealthy_subsystems {
        reasons.push(OperationalDegradeReason::SubsystemUnhealthy { name: name.clone() });
    }

    let phase = if reasons.is_empty() {
        OperationalPhase::Operational
    } else {
        OperationalPhase::Degraded { reasons }
    };

    (phase, market_data)
}

/// Cached unhealthy subsystem names from the latest health tick (optional input).
#[derive(Default)]
pub struct LatestUnhealthySubsystems {
    inner: parking_lot::Mutex<Vec<String>>,
}

impl LatestUnhealthySubsystems {
    pub fn replace(&self, names: Vec<String>) {
        *self.inner.lock() = names;
    }

    pub fn snapshot(&self) -> Vec<String> {
        self.inner.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::{LifecycleInputs, evaluate_lifecycle};
    use chrono::Utc;
    use oxide_arb_api::ws::ClobWsManager;
    use oxide_arb_models::domain::governance::lifecycle::WS_MARKET_DATA_STALE_THRESHOLD_MS;
    use oxide_arb_models::{
        config::{PolymarketConfig, WebSocketConfig},
        domain::{CatalogState, OperationalDegradeReason, OperationalPhase},
        enums::risk::BreakerStateName,
    };

    struct CatalogGate {
        ready: bool,
    }

    impl oxide_arb_models::domain::CatalogStatusPort for CatalogGate {
        fn catalog_state(&self) -> CatalogState {
            if self.ready {
                CatalogState::Ready {
                    markets: 1,
                    synced_at: Utc::now(),
                }
            } else {
                CatalogState::Warming
            }
        }

        fn is_ready(&self) -> bool {
            self.ready
        }
    }

    fn ws_manager() -> ClobWsManager {
        ClobWsManager::new(
            &PolymarketConfig::default(),
            &WebSocketConfig::default(),
            tokio_util::sync::CancellationToken::new(),
            None,
            None,
        )
    }

    #[test]
    fn warming_when_catalog_not_ready() {
        let ws = ws_manager();
        let catalog = CatalogGate { ready: false };
        let (phase, _) = evaluate_lifecycle(&LifecycleInputs {
            catalog: &catalog,
            ws_manager: &ws,
            breaker_state: BreakerStateName::Closed,
            control_factor_snapshot_expired: false,
            control_factor_live_warn: false,
            unhealthy_subsystems: &[],
        });
        assert_eq!(phase, OperationalPhase::CatalogWarming);
    }

    #[test]
    fn connecting_when_catalog_ready_without_ws_message() {
        let ws = ws_manager();
        let catalog = CatalogGate { ready: true };
        let (phase, market_data) = evaluate_lifecycle(&LifecycleInputs {
            catalog: &catalog,
            ws_manager: &ws,
            breaker_state: BreakerStateName::Closed,
            control_factor_snapshot_expired: false,
            control_factor_live_warn: false,
            unhealthy_subsystems: &[],
        });
        assert_eq!(phase, OperationalPhase::MarketDataConnecting);
        assert!(!market_data.ready);
    }

    #[test]
    fn operational_when_prerequisites_met() {
        let ws = ws_manager();
        ws.seed_test_connectivity();
        let catalog = CatalogGate { ready: true };
        let (phase, market_data) = evaluate_lifecycle(&LifecycleInputs {
            catalog: &catalog,
            ws_manager: &ws,
            breaker_state: BreakerStateName::Closed,
            control_factor_snapshot_expired: false,
            control_factor_live_warn: false,
            unhealthy_subsystems: &[],
        });
        assert_eq!(phase, OperationalPhase::Operational);
        assert!(market_data.ready);
    }

    #[test]
    fn degraded_when_market_data_stale_after_prior_message() {
        let ws = ws_manager();
        ws.seed_test_stale_connectivity(WS_MARKET_DATA_STALE_THRESHOLD_MS);
        let catalog = CatalogGate { ready: true };
        let (phase, _) = evaluate_lifecycle(&LifecycleInputs {
            catalog: &catalog,
            ws_manager: &ws,
            breaker_state: BreakerStateName::Closed,
            control_factor_snapshot_expired: false,
            control_factor_live_warn: false,
            unhealthy_subsystems: &[],
        });
        assert!(matches!(
            phase,
            OperationalPhase::Degraded {
                reasons: ref r
            } if r.contains(&OperationalDegradeReason::MarketDataStale)
        ));
    }

    #[test]
    fn degraded_when_breaker_open_with_fresh_market_data() {
        let ws = ws_manager();
        ws.seed_test_connectivity();
        let catalog = CatalogGate { ready: true };
        let (phase, _) = evaluate_lifecycle(&LifecycleInputs {
            catalog: &catalog,
            ws_manager: &ws,
            breaker_state: BreakerStateName::Open,
            control_factor_snapshot_expired: false,
            control_factor_live_warn: false,
            unhealthy_subsystems: &[],
        });
        assert!(matches!(phase, OperationalPhase::Degraded { .. }));
    }
}
