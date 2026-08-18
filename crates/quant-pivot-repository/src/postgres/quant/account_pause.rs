//! Durable account-pause envelope journal.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity::QUANT_ACCOUNT_PAUSE_SUBMISSION};
use quant_pivot_models::{
    domain::quant::{
        AccountPauseConfirmation, AccountPauseDispatch, AccountPauseSubmissionInfo,
        NewAccountPauseSubmission,
    },
    entities::quant_account_pause_submission::{ActiveModel, Column, Entity, Model},
    enums::execution::AccountPauseSubmissionState,
    types::{AccountPauseSubmissionId, AccountRecoveryIncidentId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter,
};

use crate::traits::AccountPauseRepository;

pub struct PgAccountPauseRepository {
    db: DatabaseConnection,
}

impl PgAccountPauseRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    fn exact_retry(stored: &Model, incoming: &NewAccountPauseSubmission) -> bool {
        stored.account_pause_submission_id == incoming.account_pause_submission_id
            && stored.recovery_incident_id == incoming.recovery_incident_id
            && stored.exchange_address == incoming.exchange_address
            && stored.kind == incoming.kind
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
impl AccountPauseRepository for PgAccountPauseRepository {
    async fn insert_prepared(
        &self,
        submission: NewAccountPauseSubmission,
    ) -> Result<AccountPauseSubmissionInfo, StorageError> {
        if let Some(stored) = Entity::find_by_id(submission.account_pause_submission_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        {
            if Self::exact_retry(&stored, &submission) {
                return Ok(stored.into());
            }
            return Err(StorageError::state_conflict(
                QUANT_ACCOUNT_PAUSE_SUBMISSION,
                Some(&submission.account_pause_submission_id),
                "pause envelope replay changed durable bytes or chain scope",
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
    ) -> Result<Vec<AccountPauseSubmissionInfo>, StorageError> {
        Entity::find()
            .filter(Column::RecoveryIncidentId.eq(*incident_id))
            .filter(Column::State.is_in([
                AccountPauseSubmissionState::Prepared,
                AccountPauseSubmissionState::Ambiguous,
            ]))
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn for_incident(
        &self,
        incident_id: &AccountRecoveryIncidentId,
    ) -> Result<Vec<AccountPauseSubmissionInfo>, StorageError> {
        Entity::find()
            .filter(Column::RecoveryIncidentId.eq(*incident_id))
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn record_dispatch(
        &self,
        submission_id: &AccountPauseSubmissionId,
        dispatch: AccountPauseDispatch,
        dispatched_at: DateTime<Utc>,
    ) -> Result<AccountPauseSubmissionInfo, StorageError> {
        let stored = Entity::find_by_id(*submission_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_ACCOUNT_PAUSE_SUBMISSION, submission_id)
            })?;
        if matches!(
            stored.state,
            AccountPauseSubmissionState::Dispatched | AccountPauseSubmissionState::Confirmed
        ) {
            return Ok(stored.into());
        }
        let mut active: ActiveModel = stored.into();
        match dispatch {
            AccountPauseDispatch::EoaAccepted => {
                active.state = ActiveValue::Set(AccountPauseSubmissionState::Dispatched);
            }
            AccountPauseDispatch::RelayerAccepted(transaction_id) => {
                active.state = ActiveValue::Set(AccountPauseSubmissionState::Dispatched);
                active.relayer_transaction_id = ActiveValue::Set(Some(transaction_id));
            }
            AccountPauseDispatch::Ambiguous => {
                active.state = ActiveValue::Set(AccountPauseSubmissionState::Ambiguous);
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
        submission_id: &AccountPauseSubmissionId,
        confirmation: AccountPauseConfirmation,
    ) -> Result<AccountPauseSubmissionInfo, StorageError> {
        let stored = Entity::find_by_id(*submission_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_ACCOUNT_PAUSE_SUBMISSION, submission_id)
            })?;
        if stored.state == AccountPauseSubmissionState::Confirmed {
            return Ok(stored.into());
        }
        if stored.state != AccountPauseSubmissionState::Dispatched {
            return Err(StorageError::state_conflict(
                QUANT_ACCOUNT_PAUSE_SUBMISSION,
                Some(submission_id),
                "only a dispatched pause envelope can be confirmed",
            ));
        }
        let mut active: ActiveModel = stored.into();
        active.state = ActiveValue::Set(AccountPauseSubmissionState::Confirmed);
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
