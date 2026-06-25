//! Reserved-capital reader port.
//!
//! The DB aggregation lives in the repository (`ReservedCapitalRepository`); this
//! port adapts it into the account subsystem's `QuantResult` error channel and
//! keeps the venue provider testable with a stub.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::QuantResult;
use quant_pivot_models::types::Usd;
use quant_pivot_repository::traits::ReservedCapitalRepository;

/// Capital locked by in-flight order intents at decision time.
#[async_trait]
pub trait ReservedCapitalReader: Send + Sync {
    /// Total reserved USD (zero when nothing is locked).
    async fn sum_locked(&self) -> QuantResult<Usd>;
}

/// Repository-backed reader.
pub struct RepoReservedCapitalReader {
    repo: Arc<dyn ReservedCapitalRepository>,
}

impl RepoReservedCapitalReader {
    #[must_use]
    pub const fn new(repo: Arc<dyn ReservedCapitalRepository>) -> Self {
        Self { repo }
    }
}

#[async_trait]
impl ReservedCapitalReader for RepoReservedCapitalReader {
    async fn sum_locked(&self) -> QuantResult<Usd> {
        Ok(self.repo.sum_locked_usd().await?)
    }
}
