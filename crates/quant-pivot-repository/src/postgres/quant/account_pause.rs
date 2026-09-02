//! Durable account-pause envelope journal.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity::QUANT_ACCOUNT_PAUSE_OPERATION};
use quant_pivot_models::{
    domain::quant::{
        AccountPauseConfirmation, AccountPauseDispatch, AccountPauseOperationInfo,
        NewAccountPauseOperation,
    },
    entities::quant_account_pause_operation::{ActiveModel, Column, Entity, Model},
    enums::execution::{AccountPauseOperationKind, AccountPauseOperationState},
    types::{AccountPauseOperationId, AccountRecoveryIncidentId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter,
};

use crate::traits::AccountPauseOperationRepository;

pub struct PgAccountPauseOperationRepository {
    db: DatabaseConnection,
}

impl PgAccountPauseOperationRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    fn exact_retry(stored: &Model, incoming: &NewAccountPauseOperation) -> bool {
        stored.account_pause_operation_id == incoming.account_pause_operation_id
            && stored.recovery_incident_id == incoming.recovery_incident_id
            && stored.exchange_address == incoming.exchange_address
            && stored.operation_kind == incoming.operation_kind
            && stored.submission_kind == incoming.submission_kind
            && stored.requested_block == incoming.requested_block
            && stored.interval_blocks == incoming.interval_blocks
            && stored.effective_block == incoming.effective_block
            && stored.prepared_block_number == incoming.prepared_block_number
            && stored.prepared_block_hash == incoming.prepared_block_hash
            && stored.prepared_nonce == incoming.prepared_nonce
            && stored.gas_limit == incoming.gas_limit
            && stored.calldata_hash == incoming.calldata_hash
            && stored.deployment_digest == incoming.deployment_digest
            && stored.signed_envelope == incoming.signed_envelope
            && stored.signed_envelope_hash == incoming.signed_envelope_hash
            && stored.transaction_hash == incoming.transaction_hash
    }
}

#[async_trait::async_trait]
impl AccountPauseOperationRepository for PgAccountPauseOperationRepository {
    async fn insert_prepared(
        &self,
        submission: NewAccountPauseOperation,
    ) -> Result<AccountPauseOperationInfo, StorageError> {
        if let Some(stored) = Entity::find_by_id(submission.account_pause_operation_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        {
            if Self::exact_retry(&stored, &submission) {
                return Ok(stored.into());
            }
            return Err(StorageError::state_conflict(
                QUANT_ACCOUNT_PAUSE_OPERATION,
                Some(&submission.account_pause_operation_id),
                "pause-state operation replay changed durable bytes or chain scope",
            ));
        }
        Entity::insert(submission.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn recoverable(
        &self,
        incident_id: &AccountRecoveryIncidentId,
        operation_kind: AccountPauseOperationKind,
    ) -> Result<Vec<AccountPauseOperationInfo>, StorageError> {
        Entity::find()
            .filter(Column::RecoveryIncidentId.eq(*incident_id))
            .filter(Column::OperationKind.eq(operation_kind))
            .filter(Column::State.is_in([
                AccountPauseOperationState::Prepared,
                AccountPauseOperationState::Ambiguous,
            ]))
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn for_incident(
        &self,
        incident_id: &AccountRecoveryIncidentId,
        operation_kind: AccountPauseOperationKind,
    ) -> Result<Vec<AccountPauseOperationInfo>, StorageError> {
        Entity::find()
            .filter(Column::RecoveryIncidentId.eq(*incident_id))
            .filter(Column::OperationKind.eq(operation_kind))
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn record_dispatch(
        &self,
        submission_id: &AccountPauseOperationId,
        dispatch: AccountPauseDispatch,
        dispatched_at: DateTime<Utc>,
    ) -> Result<AccountPauseOperationInfo, StorageError> {
        let stored = Entity::find_by_id(*submission_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_ACCOUNT_PAUSE_OPERATION, submission_id))?;
        if matches!(
            stored.state,
            AccountPauseOperationState::Dispatched | AccountPauseOperationState::Confirmed
        ) {
            return Ok(stored.into());
        }
        let mut active: ActiveModel = stored.into();
        match dispatch {
            AccountPauseDispatch::EoaAccepted => {
                active.state = ActiveValue::Set(AccountPauseOperationState::Dispatched);
            }
            AccountPauseDispatch::RelayerAccepted(transaction_id) => {
                active.state = ActiveValue::Set(AccountPauseOperationState::Dispatched);
                active.relayer_transaction_id = ActiveValue::Set(Some(transaction_id));
            }
            AccountPauseDispatch::Ambiguous => {
                active.state = ActiveValue::Set(AccountPauseOperationState::Ambiguous);
            }
        }
        active.dispatched_at = ActiveValue::Set(Some(dispatched_at));
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn confirm(
        &self,
        submission_id: &AccountPauseOperationId,
        confirmation: AccountPauseConfirmation,
    ) -> Result<AccountPauseOperationInfo, StorageError> {
        let stored = Entity::find_by_id(*submission_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_ACCOUNT_PAUSE_OPERATION, submission_id))?;
        if stored.state == AccountPauseOperationState::Confirmed {
            return Ok(stored.into());
        }
        if stored.state != AccountPauseOperationState::Dispatched {
            return Err(StorageError::state_conflict(
                QUANT_ACCOUNT_PAUSE_OPERATION,
                Some(submission_id),
                "only a dispatched pause-state operation can be confirmed",
            ));
        }
        let mut active: ActiveModel = stored.into();
        active.state = ActiveValue::Set(AccountPauseOperationState::Confirmed);
        active.confirmation_block_number = ActiveValue::Set(Some(confirmation.block_number));
        active.confirmation_block_hash = ActiveValue::Set(Some(confirmation.block_hash));
        active.confirmation_transaction_hash =
            ActiveValue::Set(Some(confirmation.transaction_hash));
        active.confirmation_log_index = ActiveValue::Set(Some(confirmation.log_index));
        active.confirmed_at = ActiveValue::Set(Some(confirmation.confirmed_at));
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }
}
