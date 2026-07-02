//! Periodic + nudged `system.status` WS producer for dashboard clients.
//!
//! [`SystemStatusPublisher`] already fans out on governed control-plane mutations
//! (mode / kill-switch). This task covers **infra lifecycle** transitions that
//! do not touch those surfaces: catalog warmup → ready, CLOB shard connectivity,
//! and market-data readiness. It listens to [`SystemStatusNudge`] (pipeline /
//! Gamma / health recovery) and to [`CatalogReadiness`] watch transitions, and
//! runs a faster tick while `operational_phase` is still in a startup bucket.

use crate::{
    app::{AppContext, task_id::TaskId, task_registry::AppRunner},
    governance::SystemStatusPublisher,
    service::{catalog_readiness::CatalogReadiness, system_status_nudge::SystemStatusNudge},
};
use quant_pivot_models::{
    domain::{OperationalPhase, SystemStatus, ports::runtime_control::CatalogState},
    enums::quant::QuantRuntimeMode,
};
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

/// Faster cadence while catalog / market-data are still warming.
const STARTUP_TICK: Duration = Duration::from_secs(3);
/// Steady-state poll for shard / active-market drift after operational.
const STEADY_TICK: Duration = Duration::from_secs(30);

/// Dedup key for operator-visible lifecycle fields (ignores `checked_at` / uptime).
#[derive(Debug, Clone, PartialEq, Eq)]
struct StatusFingerprint {
    operational_phase: OperationalPhase,
    catalog: CatalogFingerprint,
    market_data_ready: bool,
    ws_total: u32,
    ws_disconnected: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CatalogFingerprint {
    Warming,
    Ready { markets: u64 },
}

impl From<&CatalogState> for CatalogFingerprint {
    fn from(catalog: &CatalogState) -> Self {
        match catalog {
            CatalogState::Warming => Self::Warming,
            CatalogState::Ready { markets, .. } => Self::Ready { markets: *markets },
        }
    }
}

fn status_fingerprint(status: &SystemStatus) -> StatusFingerprint {
    StatusFingerprint {
        operational_phase: status.operational_phase.clone(),
        catalog: CatalogFingerprint::from(&status.catalog),
        market_data_ready: status.market_data.ready,
        ws_total: status.market_data.ws_shards.total,
        ws_disconnected: status.market_data.ws_shards.disconnected,
    }
}

const fn is_startup_phase(phase: &OperationalPhase) -> bool {
    matches!(
        phase,
        OperationalPhase::CatalogWarming | OperationalPhase::MarketDataConnecting
    )
}

/// Samples live system status and publishes `CoreEvent::SystemStatusChanged` when
/// the operator-visible snapshot changes or on startup-phase periodic ticks.
pub struct SystemStatusBroadcaster {
    publisher: Arc<SystemStatusPublisher>,
    nudge: SystemStatusNudge,
    catalog: Arc<CatalogReadiness>,
    last_fingerprint: Option<StatusFingerprint>,
}

impl SystemStatusBroadcaster {
    #[must_use]
    pub const fn new(
        publisher: Arc<SystemStatusPublisher>,
        nudge: SystemStatusNudge,
        catalog: Arc<CatalogReadiness>,
    ) -> Self {
        Self {
            publisher,
            nudge,
            catalog,
            last_fingerprint: None,
        }
    }

    /// Run until `shutdown` is cancelled.
    pub async fn run(mut self, shutdown: CancellationToken) {
        let mut catalog_rx = self.catalog.subscribe();

        loop {
            let tick_delay = self.tick_delay();
            tokio::select! {
                biased;

                () = shutdown.cancelled() => {
                    tracing::info!("system status broadcaster shutting down");
                    return;
                }

                () = self.nudge.notified() => {
                    self.maybe_publish();
                }

                result = catalog_rx.changed() => {
                    if result.is_ok() {
                        self.maybe_publish();
                    }
                }

                () = sleep(tick_delay) => {
                    self.maybe_publish();
                }
            }
        }
    }

    fn tick_delay(&self) -> Duration {
        self.last_fingerprint.as_ref().map_or(STARTUP_TICK, |fp| {
            if is_startup_phase(&fp.operational_phase) {
                STARTUP_TICK
            } else {
                STEADY_TICK
            }
        })
    }

    /// Publish when the operator-visible fingerprint changed since the last push.
    fn maybe_publish(&mut self) {
        let status = self.current_status();
        let fingerprint = status_fingerprint(&status);
        if self.last_fingerprint.as_ref() == Some(&fingerprint) {
            return;
        }
        self.last_fingerprint = Some(fingerprint);
        self.publisher.publish();
    }

    fn current_status(&self) -> SystemStatus {
        // `publish()` reads the same snapshot; peek once for dedup only.
        self.publisher
            .peek()
            .unwrap_or_else(|| SystemStatus::bootstrap(QuantRuntimeMode::ReportOnly))
    }
}

impl AppContext {
    /// Spawn the lifecycle `system.status` producer (runs with the web plane).
    pub fn register_system_status_broadcaster(&self, runner: &mut AppRunner) {
        let broadcaster = SystemStatusBroadcaster::new(
            Arc::clone(&self.governance.status_publisher),
            self.data.status_nudge.clone(),
            Arc::clone(&self.data.catalog),
        );
        runner.spawn(TaskId::SystemStatusBroadcaster, move |token| {
            broadcaster.run(token)
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{CatalogFingerprint, SystemStatusBroadcaster, status_fingerprint};
    use crate::{
        governance::{SystemStatusPublisher, operational_phase_from_readiness},
        service::catalog_readiness::CatalogReadiness,
        service::system_status_nudge::SystemStatusNudge,
    };
    use async_trait::async_trait;
    use chrono::Utc;
    use quant_pivot_error::QuantResult;
    use quant_pivot_models::{
        domain::{
            CatalogStatusPort, CoreEvent, CoreEventPublisher, OperationalPhase,
            QuantModeTransitionReport, RuntimeControlPort, SystemStatus,
            lifecycle::{MarketDataConnectivity, WsShardConnectivity},
            ports::runtime_control::CatalogState,
        },
        enums::{execution::KillSwitchState, quant::QuantRuntimeMode},
    };
    use std::{sync::Arc, time::Duration};
    use tokio_util::sync::CancellationToken;

    struct CatalogAwareControl {
        catalog: Arc<CatalogReadiness>,
    }

    #[async_trait]
    impl RuntimeControlPort for CatalogAwareControl {
        fn quant_runtime_mode(&self) -> QuantRuntimeMode {
            QuantRuntimeMode::ReportOnly
        }

        async fn switch_quant_mode(
            &self,
            _target: QuantRuntimeMode,
            _actor: &str,
            _reason: &str,
        ) -> QuantResult<QuantModeTransitionReport> {
            unimplemented!()
        }

        fn system_status(&self) -> SystemStatus {
            let catalog = self.catalog.catalog_state();
            let operational_phase = operational_phase_from_readiness(
                KillSwitchState::Closed,
                catalog.is_ready(),
                false,
            );
            SystemStatus {
                quant_runtime_mode: QuantRuntimeMode::ReportOnly,
                uptime_secs: 0,
                active_markets: 0,
                catalog,
                operational_phase,
                market_data: MarketDataConnectivity {
                    ready: false,
                    last_message_age_ms: None,
                    ws_shards: WsShardConnectivity {
                        total: 0,
                        disconnected: 0,
                        oldest_disconnected_secs: None,
                        connected_ratio_bps: 0,
                    },
                },
                kill_switch: SystemStatus::bootstrap(QuantRuntimeMode::ReportOnly).kill_switch,
                execution_recovery: SystemStatus::bootstrap(QuantRuntimeMode::ReportOnly)
                    .execution_recovery,
                checked_at: Utc::now(),
            }
        }

        async fn health(&self) -> quant_pivot_models::domain::HealthReport {
            quant_pivot_models::domain::HealthReport::from_checks(Vec::new(), Utc::now())
        }
    }

    #[test]
    fn fingerprint_ignores_checked_at_and_uptime() {
        let mut first = SystemStatus::bootstrap(QuantRuntimeMode::ReportOnly);
        first.uptime_secs = 1;
        let mut second = first.clone();
        second.uptime_secs = 99;
        assert_eq!(status_fingerprint(&first), status_fingerprint(&second));
    }

    #[test]
    fn fingerprint_tracks_catalog_ready_transition() {
        let mut warming = SystemStatus::bootstrap(QuantRuntimeMode::ReportOnly);
        warming.catalog = CatalogState::Warming;

        let mut ready = warming.clone();
        ready.catalog = CatalogState::Ready {
            markets: 42,
            synced_at: chrono::Utc::now(),
        };
        ready.operational_phase = OperationalPhase::MarketDataConnecting;

        assert_ne!(status_fingerprint(&warming), status_fingerprint(&ready));
        assert_eq!(
            status_fingerprint(&ready).catalog,
            CatalogFingerprint::Ready { markets: 42 }
        );
    }

    #[test]
    fn fingerprint_tracks_ws_shard_connectivity() {
        let mut status = SystemStatus::bootstrap(QuantRuntimeMode::ReportOnly);
        status.market_data = MarketDataConnectivity {
            ready: false,
            last_message_age_ms: None,
            ws_shards: WsShardConnectivity {
                total: 2,
                disconnected: 2,
                oldest_disconnected_secs: None,
                connected_ratio_bps: 0,
            },
        };
        let warming = status_fingerprint(&status);

        status.market_data.ws_shards.disconnected = 1;
        status.market_data.ws_shards.connected_ratio_bps = 5_000;
        let partial = status_fingerprint(&status);

        assert_ne!(warming, partial);
    }

    #[tokio::test]
    async fn catalog_ready_nudge_publishes_system_status_changed() {
        let catalog = Arc::new(CatalogReadiness::new());
        let (events, event_rx) = CoreEventPublisher::bounded(8);
        let status_publisher = SystemStatusPublisher::new(events);
        let control = Arc::new(CatalogAwareControl {
            catalog: Arc::clone(&catalog),
        });
        status_publisher.register(control as Arc<dyn RuntimeControlPort>);

        let nudge = SystemStatusNudge::default();
        let shutdown = CancellationToken::new();
        let broadcaster = SystemStatusBroadcaster::new(
            Arc::clone(&status_publisher),
            nudge.clone(),
            Arc::clone(&catalog),
        );
        let shutdown_task = shutdown.clone();
        tokio::spawn(async move {
            broadcaster.run(shutdown_task).await;
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        catalog.mark_ready(12, Utc::now());
        nudge.nudge();

        let event = tokio::time::timeout(Duration::from_secs(2), event_rx.recv_async())
            .await
            .expect("event within 2s")
            .expect("channel open");
        match event {
            CoreEvent::SystemStatusChanged(status) => {
                assert_eq!(
                    status.operational_phase,
                    OperationalPhase::MarketDataConnecting
                );
                assert!(status.catalog.is_ready());
            }
            other => panic!("expected SystemStatusChanged, got {other:?}"),
        }

        shutdown.cancel();
    }
}
