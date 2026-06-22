//! Risk-Fill idempotency marker + atomic Fill commit.
//!
//! Owns the `risk_fill_applied` marker and the single transaction that binds it
//! to the risk-state snapshot, the optional potential-loss entry, and the audit
//! event. The marker's presence is the durable authority for whether a fill's
//! in-memory accounting was already applied, making relay replay safe.

use crate::postgres::{
    accounting::potential_loss,
    risk::{risk_audit, risk_state},
};
use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::risk::FillCommit,
    entities::risk_fill_applied::{ActiveModel, Column, Entity},
    types::TradeId,
};
use sea_orm::{
    DatabaseConnection, DatabaseTransaction, DbErr, EntityTrait, Set, TransactionTrait,
    sea_query::OnConflict,
};

pub struct PgRiskFillRepository {
    db: DatabaseConnection,
}

pub enum PgRiskFillClaim {
    Claimed(PgRiskFillCommitGuard),
    AlreadyApplied,
}

pub struct PgRiskFillCommitGuard {
    txn: DatabaseTransaction,
    trade_id: TradeId,
}

impl PgRiskFillRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    /// Claim this fill marker inside an open transaction.
    ///
    /// The marker is inserted before in-memory risk mutation, but remains
    /// invisible until the returned guard commits the full write-set.
    pub async fn begin_fill(
        &self,
        trade_id: &TradeId,
        applied_at: DateTime<Utc>,
    ) -> Result<PgRiskFillClaim, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let marker = ActiveModel {
            trade_id: Set(trade_id.clone()),
            applied_at: Set(applied_at),
        };
        let claim_result = Entity::insert(marker)
            .on_conflict(OnConflict::column(Column::TradeId).do_nothing().to_owned())
            .exec_with_returning_keys(&txn)
            .await;
        match claim_result {
            Ok(keys) if keys.is_empty() => {
                txn.rollback().await.map_err(StorageError::from)?;
                return Ok(PgRiskFillClaim::AlreadyApplied);
            }
            Ok(_) => {}
            Err(DbErr::RecordNotFound(_)) => {
                txn.rollback().await.map_err(StorageError::from)?;
                return Ok(PgRiskFillClaim::AlreadyApplied);
            }
            Err(error) => return Err(StorageError::from(error)),
        }

        Ok(PgRiskFillClaim::Claimed(PgRiskFillCommitGuard {
            txn,
            trade_id: trade_id.clone(),
        }))
    }
}

impl PgRiskFillCommitGuard {
    pub async fn commit(self, commit: FillCommit) -> Result<(), StorageError> {
        if commit.trade_id != self.trade_id {
            return Err(StorageError::StaleData(format!(
                "risk fill commit trade {} does not match claimed marker {}",
                commit.trade_id, self.trade_id
            )));
        }

        if let Some(entry) = commit.potential_loss {
            potential_loss::do_create(&self.txn, entry).await?;
        }
        risk_state::do_upsert(&self.txn, commit.state).await?;
        risk_audit::do_create(&self.txn, commit.audit).await?;

        self.txn.commit().await.map_err(StorageError::from)?;
        Ok(())
    }
}
