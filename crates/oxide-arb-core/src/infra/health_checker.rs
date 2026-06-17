use crate::{
    control::{
        factor_snapshot::FactorSnapshotStore,
        status::{SystemStatusNudge, lifecycle_snapshot_from_parts},
    },
    infra::health_alert_state::{HealthAlertState, evaluate_ws_probe, ws_probe_skipped},
    service::{
        catalog_readiness::CatalogReadiness,
        runtime_lifecycle::{LatestUnhealthySubsystems, evaluate_lifecycle, lifecycle_inputs},
    },
};
use chrono::Utc;
use num_traits::ToPrimitive;
use oxide_arb_api::{clob::ClobClient, ws::ClobWsManager};
use oxide_arb_models::{
    domain::system::HealthReport,
    domain::{OperationalPhase, SubsystemHealth},
    enums::common::ExecutionMode,
};
use oxide_arb_risk::engine::RiskEngine;
use oxide_arb_storage::{clickhouse::ClickHousePool, postgres::PostgresPool};
use std::{sync::Arc, time::Instant};

use crate::bridge::{execution_mode::ExecutionModeHandle, risk_metrics::CoreRiskMetrics};

/// Construction dependencies for [`HealthChecker`].
pub struct HealthCheckerDeps {
    pub pg_pool: Arc<PostgresPool>,
    pub ch_pool: Arc<ClickHousePool>,
    pub ws_manager: Arc<ClobWsManager>,
    pub catalog: Arc<CatalogReadiness>,
    pub risk_engine: Arc<RiskEngine>,
    pub metrics: Arc<CoreRiskMetrics>,
    pub factor_store: Arc<FactorSnapshotStore>,
    pub clob_client: Option<Arc<ClobClient>>,
    pub mode: ExecutionModeHandle,
    pub unhealthy_subsystems: Arc<LatestUnhealthySubsystems>,
    pub alert_state: Arc<HealthAlertState>,
}

pub struct HealthChecker {
    pg_pool: Arc<PostgresPool>,
    ch_pool: Arc<ClickHousePool>,
    ws_manager: Arc<ClobWsManager>,
    catalog: Arc<CatalogReadiness>,
    risk_engine: Arc<RiskEngine>,
    metrics: Arc<CoreRiskMetrics>,
    factor_store: Arc<FactorSnapshotStore>,
    clob_client: Option<Arc<ClobClient>>,
    mode: ExecutionModeHandle,
    unhealthy_subsystems: Arc<LatestUnhealthySubsystems>,
    alert_state: Arc<HealthAlertState>,
}

impl HealthChecker {
    pub fn new(deps: HealthCheckerDeps) -> Self {
        Self {
            pg_pool: deps.pg_pool,
            ch_pool: deps.ch_pool,
            ws_manager: deps.ws_manager,
            catalog: deps.catalog,
            risk_engine: deps.risk_engine,
            metrics: deps.metrics,
            factor_store: deps.factor_store,
            clob_client: deps.clob_client,
            mode: deps.mode,
            unhealthy_subsystems: deps.unhealthy_subsystems,
            alert_state: deps.alert_state,
        }
    }

    fn lifecycle_phase(&self) -> OperationalPhase {
        let snap = lifecycle_snapshot_from_parts(
            self.risk_engine.as_ref(),
            self.metrics.as_ref(),
            self.mode.current(),
            self.factor_store.as_ref(),
            self.unhealthy_subsystems.as_ref(),
        );
        evaluate_lifecycle(&lifecycle_inputs(
            self.catalog.as_ref(),
            self.ws_manager.as_ref(),
            &snap,
        ))
        .0
    }

    pub async fn check_all(&self) -> HealthReport {
        let phase = self.lifecycle_phase();
        let (pg, ch, open_orders) = tokio::join!(
            self.check_postgres(),
            self.check_clickhouse(),
            self.check_open_orders_invariant(),
        );
        let ws = self.check_ws(&phase);
        let checks = vec![pg, ch, ws, open_orders];
        let report = HealthReport::from_checks(checks, Utc::now());

        let unhealthy = report
            .checks
            .iter()
            .filter(|check| check.counts_toward_overall() && !check.is_healthy())
            .map(|check| check.name.clone())
            .collect();
        self.unhealthy_subsystems.replace(unhealthy);

        report
    }

    /// Run probes, update unhealthy-subsystem cache, and edge-dispatch alerts.
    pub async fn check_all_and_notify(
        &self,
        alerts: &crate::observability::alert_dispatcher::AlertDispatcher,
        nudge: &SystemStatusNudge,
    ) -> HealthReport {
        let report = self.check_all().await;
        let phase = self.lifecycle_phase();
        self.alert_state
            .on_report(&report, &phase, self.mode.current(), alerts, nudge)
            .await;
        report
    }

    async fn check_postgres(&self) -> SubsystemHealth {
        let start = Instant::now();
        match self.pg_pool.health_check().await {
            Ok(()) => SubsystemHealth::healthy(
                "postgres",
                Some(ToPrimitive::to_u64(&start.elapsed().as_millis()).unwrap_or(u64::MAX)),
            ),
            Err(error) => SubsystemHealth::unhealthy(
                "postgres",
                Some(ToPrimitive::to_u64(&start.elapsed().as_millis()).unwrap_or(u64::MAX)),
                error.to_string(),
            ),
        }
    }

    async fn check_clickhouse(&self) -> SubsystemHealth {
        let start = Instant::now();
        match self.ch_pool.health_check().await {
            Ok(()) => SubsystemHealth::healthy(
                "clickhouse",
                Some(ToPrimitive::to_u64(&start.elapsed().as_millis()).unwrap_or(u64::MAX)),
            ),
            Err(error) => SubsystemHealth::unhealthy(
                "clickhouse",
                Some(ToPrimitive::to_u64(&start.elapsed().as_millis()).unwrap_or(u64::MAX)),
                error.to_string(),
            ),
        }
    }

    fn check_ws(&self, phase: &OperationalPhase) -> SubsystemHealth {
        if let Some(reason) = ws_probe_skipped(phase) {
            return SubsystemHealth::skipped("websocket", reason);
        }
        let shards = self.ws_manager.shard_health();
        if shards.disconnected > 0 {
            return SubsystemHealth::unhealthy("websocket", None, shards.to_string());
        }
        evaluate_ws_probe(self.ws_manager.last_message_age_ms(), shards.to_string())
    }

    async fn check_open_orders_invariant(&self) -> SubsystemHealth {
        if self.mode.current() != ExecutionMode::Live {
            return SubsystemHealth::skipped("clob_open_orders", "skipped_outside_live_mode");
        }

        let Some(clob_client) = &self.clob_client else {
            return SubsystemHealth::skipped("clob_open_orders", "skipped_without_clob_client");
        };

        let start = Instant::now();
        let latency = || ToPrimitive::to_u64(&start.elapsed().as_millis()).unwrap_or(u64::MAX);
        match clob_client.get_open_orders().await {
            Ok(orders) if orders.is_empty() => {
                SubsystemHealth::healthy("clob_open_orders", Some(latency()))
            }
            Ok(orders) => SubsystemHealth::unhealthy(
                "clob_open_orders",
                Some(latency()),
                format!("FOK-only invariant violated: {} open orders", orders.len()),
            ),
            Err(error) => {
                SubsystemHealth::unhealthy("clob_open_orders", Some(latency()), error.to_string())
            }
        }
    }
}
