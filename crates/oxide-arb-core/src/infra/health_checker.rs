use crate::bridge::execution_mode::ExecutionModeHandle;
use chrono::Utc;
use num_traits::ToPrimitive;
use oxide_arb_api::{clob::ClobClient, ws::ClobWsManager};
use oxide_arb_models::{
    domain::system::{HealthReport, SubsystemHealth},
    enums::common::ExecutionMode,
};
use oxide_arb_storage::{clickhouse::ClickHousePool, postgres::PostgresPool};
use std::{sync::Arc, time::Instant};

pub struct HealthChecker {
    pg_pool: Arc<PostgresPool>,
    ch_pool: Arc<ClickHousePool>,
    ws_manager: Arc<ClobWsManager>,
    clob_client: Option<Arc<ClobClient>>,
    mode: ExecutionModeHandle,
}

impl HealthChecker {
    pub const fn new(
        pg_pool: Arc<PostgresPool>,
        ch_pool: Arc<ClickHousePool>,
        ws_manager: Arc<ClobWsManager>,
        clob_client: Option<Arc<ClobClient>>,
        mode: ExecutionModeHandle,
    ) -> Self {
        Self {
            pg_pool,
            ch_pool,
            ws_manager,
            clob_client,
            mode,
        }
    }

    pub async fn check_all(&self) -> HealthReport {
        let (pg, ch, open_orders) = tokio::join!(
            self.check_postgres(),
            self.check_clickhouse(),
            self.check_open_orders_invariant(),
        );
        let ws = self.check_ws();

        let checks = vec![pg, ch, ws, open_orders];
        let overall = checks.iter().all(|c| c.healthy);
        HealthReport {
            overall_healthy: overall,
            checks,
            checked_at: Utc::now(),
        }
    }

    async fn check_postgres(&self) -> SubsystemHealth {
        let start = Instant::now();
        match self.pg_pool.health_check().await {
            Ok(()) => SubsystemHealth {
                name: "postgres".into(),
                healthy: true,
                latency_ms: Some(
                    ToPrimitive::to_u64(&start.elapsed().as_millis()).unwrap_or(u64::MAX),
                ),
                detail: None,
            },
            Err(e) => SubsystemHealth {
                name: "postgres".into(),
                healthy: false,
                latency_ms: Some(
                    ToPrimitive::to_u64(&start.elapsed().as_millis()).unwrap_or(u64::MAX),
                ),
                detail: Some(e.to_string()),
            },
        }
    }

    async fn check_clickhouse(&self) -> SubsystemHealth {
        let start = Instant::now();
        match self.ch_pool.health_check().await {
            Ok(()) => SubsystemHealth {
                name: "clickhouse".into(),
                healthy: true,
                latency_ms: Some(
                    ToPrimitive::to_u64(&start.elapsed().as_millis()).unwrap_or(u64::MAX),
                ),
                detail: None,
            },
            Err(e) => SubsystemHealth {
                name: "clickhouse".into(),
                healthy: false,
                latency_ms: Some(
                    ToPrimitive::to_u64(&start.elapsed().as_millis()).unwrap_or(u64::MAX),
                ),
                detail: Some(e.to_string()),
            },
        }
    }

    fn check_ws(&self) -> SubsystemHealth {
        let ws_healthy_threshold_ms: u64 = 30_000;
        match self.ws_manager.last_message_age_ms() {
            Some(age_ms) if age_ms < ws_healthy_threshold_ms => SubsystemHealth {
                name: "websocket".into(),
                healthy: true,
                latency_ms: Some(age_ms),
                detail: None,
            },
            Some(age_ms) => SubsystemHealth {
                name: "websocket".into(),
                healthy: false,
                latency_ms: Some(age_ms),
                detail: Some(format!("no message for {age_ms}ms")),
            },
            None => SubsystemHealth {
                name: "websocket".into(),
                healthy: false,
                latency_ms: None,
                detail: Some("never connected".into()),
            },
        }
    }

    async fn check_open_orders_invariant(&self) -> SubsystemHealth {
        if self.mode.current() != ExecutionMode::Live {
            return SubsystemHealth {
                name: "clob_open_orders".into(),
                healthy: true,
                latency_ms: None,
                detail: Some("skipped outside Live mode".into()),
            };
        }

        let Some(clob_client) = &self.clob_client else {
            return SubsystemHealth {
                name: "clob_open_orders".into(),
                healthy: true,
                latency_ms: None,
                detail: Some("skipped without ClobClient".into()),
            };
        };

        let start = Instant::now();
        match clob_client.get_open_orders().await {
            Ok(orders) if orders.is_empty() => SubsystemHealth {
                name: "clob_open_orders".into(),
                healthy: true,
                latency_ms: Some(
                    ToPrimitive::to_u64(&start.elapsed().as_millis()).unwrap_or(u64::MAX),
                ),
                detail: None,
            },
            Ok(orders) => SubsystemHealth {
                name: "clob_open_orders".into(),
                healthy: false,
                latency_ms: Some(
                    ToPrimitive::to_u64(&start.elapsed().as_millis()).unwrap_or(u64::MAX),
                ),
                detail: Some(format!(
                    "FOK-only invariant violated: {} open orders",
                    orders.len()
                )),
            },
            Err(error) => SubsystemHealth {
                name: "clob_open_orders".into(),
                healthy: false,
                latency_ms: Some(
                    ToPrimitive::to_u64(&start.elapsed().as_millis()).unwrap_or(u64::MAX),
                ),
                detail: Some(error.to_string()),
            },
        }
    }
}
