//! Report-generation readiness derived from operator lifecycle phase.

use std::sync::Arc;

use quant_pivot_api::ws::ClobWsManager;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{
        CatalogStatusPort,
        governance::lifecycle::{MarketDataConnectivity, OperationalPhase, WsShardConnectivity},
    },
    runtime_config::RuntimeConfig,
};

use crate::service::catalog_readiness::CatalogReadiness;

/// Sync port for report builder readiness gates.
pub trait ReportReadinessGate: Send + Sync {
    /// Current operator lifecycle phase used for report admission.
    fn operational_phase(&self) -> OperationalPhase;

    /// Whether the runtime is below the report-generation threshold.
    fn is_system_degraded(&self) -> bool {
        !matches!(self.operational_phase(), OperationalPhase::Operational)
    }

    /// Fail closed when empty reports are disabled but the runtime is degraded.
    fn ensure_degraded_empty_allowed(&self, config: &RuntimeConfig) -> QuantResult<()> {
        if self.is_system_degraded() && !config.reports.publish_empty_reports {
            return Err(QuantError::config(
                "report generation blocked: runtime degraded and publish_empty_reports=false",
            ));
        }
        Ok(())
    }
}

/// Catalog + websocket readiness used to gate report generation.
pub struct DefaultReportReadinessGate {
    catalog: Arc<CatalogReadiness>,
    ws_manager: Arc<ClobWsManager>,
}

impl DefaultReportReadinessGate {
    /// Wire the gate from boot-time catalog and websocket handles.
    #[must_use]
    pub const fn new(catalog: Arc<CatalogReadiness>, ws_manager: Arc<ClobWsManager>) -> Self {
        Self {
            catalog,
            ws_manager,
        }
    }
}

impl ReportReadinessGate for DefaultReportReadinessGate {
    fn operational_phase(&self) -> OperationalPhase {
        if !self.catalog.is_ready() {
            return OperationalPhase::CatalogWarming;
        }
        let shards = self.ws_manager.shard_health();
        let ws_shards = WsShardConnectivity {
            total: u32::try_from(shards.total).unwrap_or(u32::MAX),
            disconnected: u32::try_from(shards.disconnected).unwrap_or(u32::MAX),
            oldest_disconnected_secs: shards.oldest_disconnected_secs,
            connected_ratio_bps: shards.connected_ratio_bps,
        };
        let market_data =
            MarketDataConnectivity::from_parts(self.ws_manager.last_message_age_ms(), ws_shards);
        if !market_data.ready {
            return OperationalPhase::MarketDataConnecting;
        }
        OperationalPhase::Operational
    }
}
