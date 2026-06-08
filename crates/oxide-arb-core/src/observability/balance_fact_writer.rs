//! Postgres fact writer for collateral balance observations.

use chrono::{DateTime, Utc};
use oxide_arb_error::OxideError;
use oxide_arb_models::{
    domain::NewBalanceSnapshot,
    enums::fact::BalanceSnapshotSource,
    types::{BalanceSnapshotId, Usd},
};
use oxide_arb_repository::{postgres::PgFactDataRepository, traits::BalanceSnapshotRepository};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct BalanceFactObservation {
    pub holder_address: String,
    pub internal_available_usd: Usd,
    pub internal_reserved_usd: Usd,
    pub external_available_usd: Usd,
    pub external_locked_usd: Usd,
    pub block_number: Option<i64>,
    pub reconciliation_report_id: Option<i64>,
    pub observed_at: DateTime<Utc>,
}

pub struct BalanceFactWriter {
    repo: Arc<PgFactDataRepository>,
}

impl BalanceFactWriter {
    pub const fn new(repo: Arc<PgFactDataRepository>) -> Self {
        Self { repo }
    }

    pub async fn write_observation(
        &self,
        observation: BalanceFactObservation,
    ) -> Result<(), OxideError> {
        let internal_total = observation.internal_available_usd + observation.internal_reserved_usd;
        let external_total = observation.external_available_usd + observation.external_locked_usd;
        self.repo
            .create_balance_snapshot(NewBalanceSnapshot {
                balance_snapshot_id: BalanceSnapshotId::new_v7(),
                holder_address: observation.holder_address,
                internal_available_usd: observation.internal_available_usd,
                internal_reserved_usd: observation.internal_reserved_usd,
                external_available_usd: observation.external_available_usd,
                external_locked_usd: observation.external_locked_usd,
                drift_usd: internal_total - external_total,
                source: BalanceSnapshotSource::ClobApi,
                block_number: observation.block_number,
                reconciliation_report_id: observation.reconciliation_report_id,
                observed_at: observation.observed_at,
            })
            .await?;
        Ok(())
    }
}
