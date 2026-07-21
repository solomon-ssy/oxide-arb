//! Operator lifecycle phase derivation (catalog, market data, kill-switch).

use quant_pivot_models::{
    domain::governance::lifecycle::{OperationalDegradeReason, OperationalPhase},
    enums::execution::KillSwitchState,
};

/// Derive the dashboard [`OperationalPhase`] from infra readiness and the
/// operational kill-switch state machine.
///
/// Priority: catalog warmup → market-data connecting → kill-switch impact →
/// nominal operational. Tightened kill-switch states (`report_only_forced`,
/// `exit_only`) surface as [`OperationalPhase::Degraded`] so reports may
/// continue; full execution freezes map to [`OperationalPhase::Halted`].
#[must_use]
pub fn operational_phase_from_readiness(
    kill_switch: KillSwitchState,
    catalog_ready: bool,
    market_ready: bool,
) -> OperationalPhase {
    if !catalog_ready {
        return OperationalPhase::CatalogWarming;
    }
    if !market_ready {
        return OperationalPhase::MarketDataConnecting;
    }
    match kill_switch {
        KillSwitchState::Closed => OperationalPhase::Operational,
        KillSwitchState::ReportOnlyForced | KillSwitchState::ExitOnly => {
            OperationalPhase::Degraded {
                reasons: vec![OperationalDegradeReason::KillSwitchTightened { state: kill_switch }],
            }
        }
        KillSwitchState::ExecutionHalted | KillSwitchState::EmergencyHalted => {
            OperationalPhase::Halted
        }
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{
        domain::governance::lifecycle::{OperationalDegradeReason, OperationalPhase},
        enums::execution::KillSwitchState,
    };

    use super::operational_phase_from_readiness;

    #[test]
    fn closed_with_ready_infra_is_operational() {
        assert_eq!(
            operational_phase_from_readiness(KillSwitchState::Closed, true, true),
            OperationalPhase::Operational
        );
    }

    #[test]
    fn tightened_kill_switch_states_are_degraded_not_halted() {
        for state in [KillSwitchState::ReportOnlyForced, KillSwitchState::ExitOnly] {
            assert_eq!(
                operational_phase_from_readiness(state, true, true),
                OperationalPhase::Degraded {
                    reasons: vec![OperationalDegradeReason::KillSwitchTightened { state }],
                }
            );
        }
    }

    #[test]
    fn execution_freeze_states_are_halted() {
        for state in [
            KillSwitchState::ExecutionHalted,
            KillSwitchState::EmergencyHalted,
        ] {
            assert_eq!(
                operational_phase_from_readiness(state, true, true),
                OperationalPhase::Halted
            );
        }
    }

    #[test]
    fn catalog_warming_takes_priority_over_kill_switch() {
        assert_eq!(
            operational_phase_from_readiness(KillSwitchState::EmergencyHalted, false, true),
            OperationalPhase::CatalogWarming
        );
    }
}
