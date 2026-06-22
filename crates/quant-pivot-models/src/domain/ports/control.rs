//! Web-facing runtime control ports (dependency inversion).

use crate::{
    domain::{
        ReadinessReport,
        governance::system::{HealthReport, SystemStatus},
        market::book::BookSnapshot,
    },
    enums::quant::QuantRuntimeMode,
    runtime_config::RuntimeConfig,
    types::TokenId,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantError;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, sync::Arc};
use thiserror::Error;

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

#[derive(Debug, Error)]
pub enum RuntimeControlError {
    #[error("precondition failed: {0}")]
    Precondition(String),
    #[error("control operation failed: {0}")]
    Engine(String),
}

impl From<QuantError> for RuntimeControlError {
    fn from(error: QuantError) -> Self {
        Self::Engine(error.to_string())
    }
}

#[async_trait]
pub trait RuntimeControlPort: Send + Sync {
    fn quant_runtime_mode(&self) -> QuantRuntimeMode;

    async fn switch_quant_mode(
        &self,
        target: QuantRuntimeMode,
        reason: &str,
    ) -> Result<QuantModeTransitionReport, RuntimeControlError>;

    fn system_status(&self) -> SystemStatus;

    async fn health(&self) -> HealthReport;
}

#[async_trait]
pub trait RuntimeConfigPort: Send + Sync {
    fn current(&self) -> Arc<RuntimeConfig>;
    fn preflight(&self, candidate: &RuntimeConfig) -> Result<(), RuntimeControlError>;
    async fn apply(&self, config: RuntimeConfig) -> Result<(), RuntimeControlError>;
}

#[async_trait]
pub trait MarketDataPort: Send + Sync {
    fn book(
        &self,
        yes_token: &TokenId,
        no_token: &TokenId,
    ) -> (Option<Arc<BookSnapshot>>, Option<Arc<BookSnapshot>>);

    fn subscribed_tokens(&self, token_ids: &[TokenId]) -> HashSet<TokenId>;

    async fn subscribe(&self, token_ids: Vec<TokenId>) -> Result<(), RuntimeControlError>;

    async fn unsubscribe(&self, token_ids: Vec<TokenId>) -> Result<(), RuntimeControlError>;
}

pub trait MetricsScrapePort: Send + Sync {
    fn gather_prometheus(&self) -> String;
}

#[async_trait]
pub trait ReadinessPort: Send + Sync {
    async fn check(&self) -> ReadinessReport;
}
