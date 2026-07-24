//! Edge-triggered health alert dispatch — suppresses startup noise and emits
//! recovery notifications when subsystems return to healthy.

use std::{collections::HashMap, fmt::Display};

use chrono::Utc;
use parking_lot::Mutex;
use quant_pivot_models::{
    domain::governance::{
        HealthReport, OperationalPhase, SubsystemCheckStatus, SubsystemHealth,
        WS_MARKET_DATA_STALE_THRESHOLD_MS,
    },
    enums::{
        common::{AlertCategory, AlertLevel, AlertSource},
        quant::QuantRuntimeMode,
    },
};

use crate::{
    observability::alert_dispatcher::{Alert, AlertDispatcher},
    service::system_status_nudge::SystemStatusNudge,
};

/// Prior probe outcome per subsystem name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeOutcome {
    Healthy,
    Unhealthy,
    Skipped,
}

impl From<&SubsystemCheckStatus> for ProbeOutcome {
    fn from(status: &SubsystemCheckStatus) -> Self {
        match status {
            SubsystemCheckStatus::Healthy => Self::Healthy,
            SubsystemCheckStatus::Unhealthy => Self::Unhealthy,
            SubsystemCheckStatus::Skipped { .. } => Self::Skipped,
        }
    }
}

/// Tracks subsystem probe transitions for alert edge dispatch.
#[derive(Default)]
pub struct HealthAlertState {
    last: Mutex<HashMap<String, ProbeOutcome>>,
}

impl HealthAlertState {
    /// Process a completed health tick against the current lifecycle phase.
    pub async fn on_report(
        &self,
        report: &HealthReport,
        phase: &OperationalPhase,
        quant_runtime_mode: QuantRuntimeMode,
        alerts: &AlertDispatcher,
        nudge: &SystemStatusNudge,
    ) {
        if matches!(
            phase,
            OperationalPhase::CatalogWarming | OperationalPhase::MarketDataConnecting
        ) {
            return;
        }

        let transitions = {
            let mut last = self.last.lock();
            let mut collected = Vec::new();
            for check in &report.checks {
                let current = ProbeOutcome::from(&check.status);
                let previous = last.insert(check.name.clone(), current);
                collected.push((previous, current, check.clone()));
            }
            drop(last);
            collected
        };

        let mut nudge_needed = false;
        for (previous, current, check) in transitions {
            match (previous, current) {
                (Some(ProbeOutcome::Healthy) | None, ProbeOutcome::Unhealthy) => {
                    dispatch_unhealthy_alert(&check, phase, quant_runtime_mode, alerts).await;
                }
                (Some(ProbeOutcome::Unhealthy), ProbeOutcome::Healthy) => {
                    dispatch_recovery_alert(&check.name, alerts).await;
                    nudge_needed = true;
                }
                _ => {}
            }
        }

        if nudge_needed {
            nudge.nudge();
        }
    }
}

async fn dispatch_unhealthy_alert(
    check: &SubsystemHealth,
    phase: &OperationalPhase,
    quant_runtime_mode: QuantRuntimeMode,
    alerts: &AlertDispatcher,
) {
    let detail = check.detail.as_deref().unwrap_or("subsystem unhealthy");
    let affects_trading = check.name == "websocket"
        && quant_runtime_mode == QuantRuntimeMode::AutoExecution
        && matches!(
            phase,
            OperationalPhase::Operational | OperationalPhase::Degraded { .. }
        );

    alerts
        .dispatch(
            Alert::new(
                format!("health.unhealthy.{}", check.name),
                AlertLevel::Critical,
                AlertCategory::Infrastructure,
                AlertSource::HealthChecker,
                "Health check unhealthy",
                format!("unhealthy subsystem {}: {}", check.name, detail),
                Utc::now(),
            )
            .with_affects_trading(affects_trading),
        )
        .await;
}

async fn dispatch_recovery_alert(subsystem: &str, alerts: &AlertDispatcher) {
    alerts
        .dispatch(
            Alert::new(
                format!("health.recovered.{subsystem}"),
                AlertLevel::Info,
                AlertCategory::Infrastructure,
                AlertSource::HealthChecker,
                "Health check recovered",
                format!("subsystem {subsystem} is healthy again"),
                Utc::now(),
            )
            .with_affects_trading(false)
            .with_visible_toast(false),
        )
        .await;
}

/// Whether websocket probes should be skipped for the current lifecycle phase.
#[must_use]
pub const fn ws_probe_skipped(phase: &OperationalPhase) -> Option<&'static str> {
    match phase {
        OperationalPhase::CatalogWarming => Some("catalog_warming"),
        OperationalPhase::MarketDataConnecting => Some("market_data_connecting"),
        _ => None,
    }
}

/// Evaluate websocket probe outcome when not skipped.
#[must_use]
pub fn evaluate_ws_probe(
    last_message_age_ms: Option<u64>,
    shards: impl Display,
) -> SubsystemHealth {
    match last_message_age_ms {
        Some(age_ms) if age_ms < WS_MARKET_DATA_STALE_THRESHOLD_MS => {
            SubsystemHealth::healthy("websocket", Some(age_ms))
        }
        Some(age_ms) => SubsystemHealth::unhealthy(
            "websocket",
            Some(age_ms),
            format!("no message for {age_ms}ms; {shards}"),
        ),
        None => SubsystemHealth::unhealthy("websocket", None, format!("never connected; {shards}")),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use chrono::Utc;
    use quant_pivot_models::{
        domain::governance::{
            HealthReport, OperationalPhase, SubsystemCheckStatus, SubsystemHealth,
            WS_MARKET_DATA_STALE_THRESHOLD_MS,
        },
        enums::quant::QuantRuntimeMode,
    };

    use super::{HealthAlertState, ProbeOutcome, evaluate_ws_probe, ws_probe_skipped};
    use crate::{
        observability::alert_dispatcher::AlertDispatcher,
        service::system_status_nudge::SystemStatusNudge,
    };

    #[test]
    fn ws_skipped_warming_phases() {
        assert_eq!(
            ws_probe_skipped(&OperationalPhase::CatalogWarming),
            Some("catalog_warming")
        );
        assert_eq!(
            ws_probe_skipped(&OperationalPhase::MarketDataConnecting),
            Some("market_data_connecting")
        );
        assert!(ws_probe_skipped(&OperationalPhase::Operational).is_none());
    }

    #[test]
    fn ws_probe_healthy_fresh() {
        let check = evaluate_ws_probe(
            Some(WS_MARKET_DATA_STALE_THRESHOLD_MS - 1),
            "all 1 WS shards connected",
        );
        assert!(check.is_healthy());
    }

    #[test]
    fn probe_outcome_from_status() {
        assert_eq!(
            ProbeOutcome::from(&SubsystemCheckStatus::Skipped { reason: "x".into() }),
            ProbeOutcome::Skipped
        );
    }

    #[tokio::test]
    async fn recovery_alert_healthy_transition() {
        let recordings = Arc::new(Mutex::new(Vec::new()));
        let alerts = Arc::new(AlertDispatcher::with_recordings(Arc::clone(&recordings)));
        let state = HealthAlertState::default();
        {
            let mut last = state.last.lock();
            last.insert("websocket".into(), ProbeOutcome::Unhealthy);
        }

        let report = HealthReport::from_checks(
            vec![SubsystemHealth::healthy("websocket", Some(1))],
            Utc::now(),
        );
        let nudge = SystemStatusNudge::default();
        state
            .on_report(
                &report,
                &OperationalPhase::Operational,
                QuantRuntimeMode::ReportOnly,
                &alerts,
                &nudge,
            )
            .await;

        let recovered = {
            let guard = recordings.lock().expect("recordings lock");
            guard
                .iter()
                .any(|alert| alert.idempotency_key == "health.recovered.websocket")
        };
        assert!(recovered);
    }
}
