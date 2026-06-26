//! Web-facing runtime control ports (dependency inversion).

use crate::{
    domain::{
        ReadinessReport,
        data_plane::DataQualitySnapshot,
        governance::system::{HealthReport, SystemStatus},
        market::book::BookSnapshot,
    },
    enums::quant::QuantRuntimeMode,
    runtime_config::RuntimeConfig,
    types::TokenId,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::control::ControlError;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, sync::Arc};

/// Gamma catalog warmup state for operator dashboards.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum CatalogState {
    Warming,
    Ready {
        markets: u64,
        synced_at: DateTime<Utc>,
    },
}

impl CatalogState {
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        matches!(self, Self::Ready { .. })
    }
}

/// Catalog readiness surface (dependency-inverted).
pub trait CatalogStatusPort: Send + Sync {
    fn catalog_state(&self) -> CatalogState;
    fn is_ready(&self) -> bool;
}

/// Outcome of a successful governed quant runtime mode transition.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct QuantModeTransitionReport {
    pub from: QuantRuntimeMode,
    pub to: QuantRuntimeMode,
}

#[async_trait]
pub trait RuntimeControlPort: Send + Sync {
    fn quant_runtime_mode(&self) -> QuantRuntimeMode;

    async fn switch_quant_mode(
        &self,
        target: QuantRuntimeMode,
        reason: &str,
    ) -> Result<QuantModeTransitionReport, ControlError>;

    fn system_status(&self) -> SystemStatus;

    async fn health(&self) -> HealthReport;
}

#[async_trait]
pub trait RuntimeConfigPort: Send + Sync {
    fn current(&self) -> Arc<RuntimeConfig>;
    fn preflight(&self, candidate: &RuntimeConfig) -> Result<(), ControlError>;
    async fn apply(&self, config: RuntimeConfig) -> Result<(), ControlError>;
}

#[async_trait]
pub trait MarketDataPort: Send + Sync {
    fn book(
        &self,
        yes_token: &TokenId,
        no_token: &TokenId,
    ) -> (Option<Arc<BookSnapshot>>, Option<Arc<BookSnapshot>>);

    fn subscribed_tokens(&self, token_ids: &[TokenId]) -> HashSet<TokenId>;

    async fn subscribe(&self, token_ids: Vec<TokenId>) -> Result<(), ControlError>;

    async fn unsubscribe(&self, token_ids: Vec<TokenId>) -> Result<(), ControlError>;
}

pub trait MetricsScrapePort: Send + Sync {
    fn gather_prometheus(&self) -> String;
}

/// Read-only data-quality observability surface (dependency-inverted).
pub trait DataQualityPort: Send + Sync {
    /// Aggregate classification of the live book plane at call time.
    fn snapshot(&self) -> DataQualitySnapshot;
}

#[async_trait]
pub trait ReadinessPort: Send + Sync {
    async fn check(&self) -> ReadinessReport;
}
