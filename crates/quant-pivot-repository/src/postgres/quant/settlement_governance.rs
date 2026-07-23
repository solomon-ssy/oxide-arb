//! `PostgreSQL` governed settlement actions and external observation cursors.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{
    StorageError,
    entity::{
        QUANT_SETTLEMENT_CHAIN_SUBMISSION, QUANT_SETTLEMENT_EXTERNAL_CURSOR,
        QUANT_SETTLEMENT_GOVERNED_ACTION, QUANT_SETTLEMENT_REDEEM,
    },
};
use quant_pivot_models::{
    domain::{
        api::settlement_redeem::SettlementGovernedActionListQuery,
        pagination::{PageWindow, Paginated},
        quant::{
            settlement::{NewSettlementChainSubmission, SettlementChainSubmissionInfo},
            settlement_governance::{
                AdvanceSettlementExternalCursor, BeginGovernedActionDispatch,
                ConfirmSettlementGovernedAction, FailSettlementGovernedAction,
                NewSettlementExternalCursor, NewSettlementGovernedAction,
                PersistExternalSettlementScan, PersistPreparedGovernedActionSubmission,
                RecordGovernedActionEoaBroadcast, RecordGovernedActionRelayerAcceptance,
                RecordGovernedActionRelayerChainHash, RequireGovernedActionReconciliation,
                RevokeSettlementGovernedAction, ScheduleGovernedActionRetry,
                ScheduleGovernedActionWork, SettlementExternalCursorInfo,
                SettlementGovernedActionInfo, SettlementGovernedActionWorkClaim,
            },
        },
    },
    entities::{
        quant_settlement_chain_submission::{
            Column as ChainSubmissionColumn, Entity as ChainSubmissionEntity,
            Model as ChainSubmissionModel,
        },
        quant_settlement_external_cursor::{
            Column as ExternalCursorColumn, Entity as ExternalCursorEntity,
            Model as ExternalCursorModel,
        },
        quant_settlement_governed_action::{
            Column as GovernedActionColumn, Entity as GovernedActionEntity,
            Model as GovernedActionModel,
        },
        quant_settlement_redeem::{
            Entity as SettlementRedeemEntity, Model as SettlementRedeemModel,
        },
    },
    enums::settlement::{
        SettlementCaseState, SettlementGovernedActionKind, SettlementGovernedActionState,
        SettlementReadinessStatus, SettlementReconciliationState, SettlementRoute,
        SettlementSubmissionKind, SettlementSubmissionPurpose, SettlementSubmissionState,
    },
    types::{
        ContentHash, EvmAddress, ExecutionAccountId, SettlementChainSubmissionId,
        SettlementExternalCursorId, SettlementGovernedActionId, SettlementRedeemId, WorkerId,
        settlement_payload::SettlementChainReceiptEvidence,
    },
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, DatabaseTransaction,
    EntityTrait, IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
    sea_query::{Expr, LockBehavior, LockType, OnConflict},
};

use crate::{
    postgres::{error, query::paginate_mapped},
    traits::quant::settlement_governance::{
        SettlementExternalCursorRepository, SettlementGovernanceRepository,
    },
};

pub struct PgSettlementGovernanceRepository {
    db: DatabaseConnection,
}

impl PgSettlementGovernanceRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

async fn lock_governed_action(
    txn: &DatabaseTransaction,
    action_id: SettlementGovernedActionId,
) -> Result<GovernedActionModel, StorageError> {
    GovernedActionEntity::find_by_id(action_id)
        .lock_exclusive()
        .one(txn)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| error::not_found(QUANT_SETTLEMENT_GOVERNED_ACTION, action_id))
}

async fn action_submission(
    txn: &DatabaseTransaction,
    action_id: SettlementGovernedActionId,
) -> Result<Option<ChainSubmissionModel>, StorageError> {
    ChainSubmissionEntity::find()
        .filter(ChainSubmissionColumn::SettlementGovernedActionId.eq(action_id))
        .one(txn)
        .await
        .map_err(StorageError::from)
}

async fn lock_action_submission(
    txn: &DatabaseTransaction,
    action_id: SettlementGovernedActionId,
    submission_id: SettlementChainSubmissionId,
) -> Result<ChainSubmissionModel, StorageError> {
    let submission = ChainSubmissionEntity::find_by_id(submission_id)
        .lock_exclusive()
        .one(txn)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| error::not_found(QUANT_SETTLEMENT_CHAIN_SUBMISSION, submission_id))?;
    if submission.settlement_governed_action_id != Some(action_id) {
        return Err(error::state_conflict(
            QUANT_SETTLEMENT_CHAIN_SUBMISSION,
            Some(submission_id),
            "governed-action submission parent does not match",
        ));
    }
    Ok(submission)
}

fn require_live_action_claim(
    action: &GovernedActionModel,
    owner: &WorkerId,
    now: DateTime<Utc>,
) -> Result<(), StorageError> {
    if action.claim_owner.as_ref() != Some(owner)
        || action
            .lease_expires_at
            .is_none_or(|lease_expires_at| lease_expires_at <= now)
    {
        return Err(error::state_conflict(
            QUANT_SETTLEMENT_GOVERNED_ACTION,
            Some(action.settlement_governed_action_id),
            "governed action lease is absent, expired, or owned by another worker",
        ));
    }
    Ok(())
}

fn governed_action_purpose(
    kind: SettlementGovernedActionKind,
) -> Result<SettlementSubmissionPurpose, StorageError> {
    match kind {
        SettlementGovernedActionKind::OutcomeTokenApproval => {
            Ok(SettlementSubmissionPurpose::OutcomeTokenApproval)
        }
        SettlementGovernedActionKind::OutcomeTokenRevocation => {
            Ok(SettlementSubmissionPurpose::OutcomeTokenRevocation)
        }
        SettlementGovernedActionKind::CanaryGrant => Err(error::invariant_violation(
            Some(QUANT_SETTLEMENT_GOVERNED_ACTION),
            "canary grant is consumed by a redeem submission and has no standalone transport",
        )),
    }
}

#[async_trait::async_trait]
impl SettlementGovernanceRepository for PgSettlementGovernanceRepository {
    async fn create_action(
        &self,
        action: NewSettlementGovernedAction,
    ) -> Result<SettlementGovernedActionInfo, StorageError> {
        let expected = action.clone();
        GovernedActionEntity::insert(action.into_active_model())
            .on_conflict(
                OnConflict::column(GovernedActionColumn::IdempotencyKey)
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(&self.db)
            .await
            .map_err(StorageError::from)?;
        let stored = GovernedActionEntity::find()
            .filter(GovernedActionColumn::IdempotencyKey.eq(&expected.idempotency_key))
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                error::not_found(
                    QUANT_SETTLEMENT_GOVERNED_ACTION,
                    expected.settlement_governed_action_id,
                )
            })?;
        let exact = stored.execution_account_id == expected.execution_account_id
            && stored.settlement_redeem_id == expected.settlement_redeem_id
            && stored.kind == expected.kind
            && stored.route == expected.route
            && stored.target_adapter == expected.target_adapter
            && stored.deployment_digest == expected.deployment_digest
            && stored.deployment_evidence_version == expected.deployment_evidence_version
            && stored.desired_approval == expected.desired_approval
            && stored.authorization_digest == expected.authorization_digest
            && stored.payout_ceiling_usd == expected.payout_ceiling_usd
            && stored.scope_digest == expected.scope_digest
            && stored.authorization_reason == expected.authorization_reason
            && stored.authorized_by == expected.authorized_by
            && stored.expires_at == expected.expires_at;
        if !exact {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_GOVERNED_ACTION,
                Some(stored.settlement_governed_action_id),
                "governed action idempotency key was reused for another immutable scope",
            ));
        }
        Ok(stored.into())
    }

    async fn find_action(
        &self,
        action_id: &SettlementGovernedActionId,
    ) -> Result<Option<SettlementGovernedActionInfo>, StorageError> {
        GovernedActionEntity::find_by_id(*action_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|model| model.map(Into::into))
    }

    async fn page_actions(
        &self,
        query: SettlementGovernedActionListQuery,
    ) -> Result<Paginated<SettlementGovernedActionInfo>, StorageError> {
        let mut select =
            GovernedActionEntity::find().order_by_desc(GovernedActionColumn::AuthorizedAt);
        if let Some(kind) = query.kind {
            select = select.filter(GovernedActionColumn::Kind.eq(kind));
        }
        if let Some(state) = query.state {
            select = select.filter(GovernedActionColumn::State.eq(state));
        }
        paginate_mapped(select, &self.db, PageWindow::from_query(&query), Into::into).await
    }

    async fn revoke_action(
        &self,
        command: RevokeSettlementGovernedAction,
    ) -> Result<SettlementGovernedActionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let model = lock_governed_action(&txn, command.settlement_governed_action_id).await?;
        if model.state == SettlementGovernedActionState::Revoked
            && model.scope_digest == command.expected_scope_digest
            && model.revoked_by == Some(command.actor)
            && model.revocation_reason.as_deref() == Some(command.reason.as_str())
        {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(model.into());
        }
        if !matches!(
            model.state,
            SettlementGovernedActionState::Authorized
                | SettlementGovernedActionState::RetryScheduled
        ) || model.scope_digest != command.expected_scope_digest
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_GOVERNED_ACTION,
                Some(model.settlement_governed_action_id),
                "governed action revocation state/scope compare-and-swap failed",
            ));
        }
        if command.revoked_at < model.authorized_at {
            return Err(error::invariant_violation(
                Some(QUANT_SETTLEMENT_GOVERNED_ACTION),
                "governed action revocation cannot predate authorization",
            ));
        }
        if action_submission(&txn, model.settlement_governed_action_id)
            .await?
            .is_some()
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_GOVERNED_ACTION,
                Some(model.settlement_governed_action_id),
                "durable submission identity forbids governed action revocation",
            ));
        }
        let mut active = model.into_active_model();
        active.state = ActiveValue::Set(SettlementGovernedActionState::Revoked);
        active.revoked_by = ActiveValue::Set(Some(command.actor));
        active.revocation_reason = ActiveValue::Set(Some(command.reason));
        active.revoked_at = ActiveValue::Set(Some(command.revoked_at));
        active.next_attempt_at = ActiveValue::Set(None);
        active.claim_owner = ActiveValue::Set(None);
        active.lease_expires_at = ActiveValue::Set(None);
        let revoked = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(revoked.into())
    }

    async fn find_submission_by_action(
        &self,
        action_id: &SettlementGovernedActionId,
    ) -> Result<Option<SettlementChainSubmissionInfo>, StorageError> {
        ChainSubmissionEntity::find()
            .filter(ChainSubmissionColumn::SettlementGovernedActionId.eq(*action_id))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|model| model.map(Into::into))
    }

    async fn claim_next_action(
        &self,
        execution_account_id: &ExecutionAccountId,
        owner: &WorkerId,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<Option<SettlementGovernedActionWorkClaim>, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        GovernedActionEntity::update_many()
            .col_expr(
                GovernedActionColumn::State,
                Expr::value(SettlementGovernedActionState::Expired),
            )
            .col_expr(
                GovernedActionColumn::NextAttemptAt,
                Expr::value(None::<DateTime<Utc>>),
            )
            .col_expr(
                GovernedActionColumn::ClaimOwner,
                Expr::value(None::<WorkerId>),
            )
            .col_expr(
                GovernedActionColumn::LeaseExpiresAt,
                Expr::value(None::<DateTime<Utc>>),
            )
            .filter(GovernedActionColumn::State.is_in([
                SettlementGovernedActionState::Authorized,
                SettlementGovernedActionState::RetryScheduled,
            ]))
            .filter(GovernedActionColumn::ExpiresAt.lte(now))
            .filter(
                Condition::any()
                    .add(GovernedActionColumn::ClaimOwner.is_null())
                    .add(GovernedActionColumn::LeaseExpiresAt.lte(now)),
            )
            .filter(Expr::cust(
                "NOT EXISTS (SELECT 1 FROM quant_settlement_chain_submission AS submission \
                 WHERE submission.settlement_governed_action_id = \
                 quant_settlement_governed_action.settlement_governed_action_id)",
            ))
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;

        let transport_kinds = [
            SettlementGovernedActionKind::OutcomeTokenApproval,
            SettlementGovernedActionKind::OutcomeTokenRevocation,
        ];
        let claimable_lease = Condition::any()
            .add(GovernedActionColumn::ClaimOwner.is_null())
            .add(GovernedActionColumn::LeaseExpiresAt.lte(now));
        let recoverable = Expr::cust(
            "EXISTS (SELECT 1 FROM quant_settlement_chain_submission AS submission \
             WHERE submission.settlement_governed_action_id = \
             quant_settlement_governed_action.settlement_governed_action_id \
             AND submission.state NOT IN \
             ('confirmed'::qp_settlement_submission_state, 'failed'::qp_settlement_submission_state))",
        );
        let unsubmitted = Condition::all()
            .add(GovernedActionColumn::ExpiresAt.gt(now))
            .add(Expr::cust(
                "NOT EXISTS (SELECT 1 FROM quant_settlement_chain_submission AS submission \
                 WHERE submission.settlement_governed_action_id = \
                 quant_settlement_governed_action.settlement_governed_action_id)",
            ));
        let model = GovernedActionEntity::find()
            .filter(GovernedActionColumn::ExecutionAccountId.eq(*execution_account_id))
            .filter(GovernedActionColumn::Kind.is_in(transport_kinds))
            .filter(GovernedActionColumn::State.is_in([
                SettlementGovernedActionState::Authorized,
                SettlementGovernedActionState::RetryScheduled,
            ]))
            .filter(GovernedActionColumn::NextAttemptAt.lte(now))
            .filter(claimable_lease)
            .filter(Condition::any().add(recoverable).add(unsubmitted))
            .order_by_asc(GovernedActionColumn::AuthorizedAt)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .one(&txn)
            .await
            .map_err(StorageError::from)?;
        let Some(model) = model else {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(None);
        };
        let action_id = model.settlement_governed_action_id;
        let mut active = model.into_active_model();
        active.claim_owner = ActiveValue::Set(Some(*owner));
        active.lease_expires_at = ActiveValue::Set(Some(lease_expires_at));
        let claimed = active.update(&txn).await.map_err(StorageError::from)?;
        let submission = action_submission(&txn, action_id).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(Some(SettlementGovernedActionWorkClaim {
            action: claimed.into(),
            submission: submission.map(Into::into),
        }))
    }

    async fn renew_action_claim(
        &self,
        action_id: &SettlementGovernedActionId,
        owner: &WorkerId,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        let result = GovernedActionEntity::update_many()
            .col_expr(
                GovernedActionColumn::LeaseExpiresAt,
                Expr::value(lease_expires_at),
            )
            .filter(GovernedActionColumn::SettlementGovernedActionId.eq(*action_id))
            .filter(GovernedActionColumn::ClaimOwner.eq(*owner))
            .filter(GovernedActionColumn::LeaseExpiresAt.gt(now))
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(result.rows_affected == 1)
    }

    async fn release_action_claim(
        &self,
        action_id: &SettlementGovernedActionId,
        owner: &WorkerId,
    ) -> Result<bool, StorageError> {
        let result = GovernedActionEntity::update_many()
            .col_expr(
                GovernedActionColumn::ClaimOwner,
                Expr::value(None::<WorkerId>),
            )
            .col_expr(
                GovernedActionColumn::LeaseExpiresAt,
                Expr::value(None::<DateTime<Utc>>),
            )
            .filter(GovernedActionColumn::SettlementGovernedActionId.eq(*action_id))
            .filter(GovernedActionColumn::ClaimOwner.eq(*owner))
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(result.rows_affected == 1)
    }

    async fn persist_prepared_action_submission(
        &self,
        command: PersistPreparedGovernedActionSubmission,
    ) -> Result<SettlementChainSubmissionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let action = lock_governed_action(&txn, command.settlement_governed_action_id).await?;
        require_live_action_claim(&action, &command.owner, command.persisted_at)?;
        let purpose = governed_action_purpose(action.kind)?;
        let submission = &command.submission;
        let exact_scope = matches!(
            action.state,
            SettlementGovernedActionState::Authorized
                | SettlementGovernedActionState::RetryScheduled
        ) && action.expires_at > command.persisted_at
            && action.scope_digest == command.expected_scope_digest
            && submission.settlement_redeem_id.is_none()
            && submission.settlement_governed_action_id
                == Some(action.settlement_governed_action_id)
            && submission.canary_action_id.is_none()
            && submission.purpose == purpose
            && submission.state == SettlementSubmissionState::Prepared
            && Some(submission.route) == action.route
            && Some(submission.target_adapter.clone()) == action.target_adapter
            && Some(submission.deployment_digest) == action.deployment_digest
            && Some(submission.deployment_evidence_version.clone())
                == action.deployment_evidence_version
            && action
                .verified_block_number
                .is_some_and(|block| submission.verified_block_number >= block)
            && submission.attempt_ordinal == 1
            && submission.signed_envelope.is_some()
            && submission.signed_envelope_hash.is_some()
            && submission.prepared_nonce.is_some();
        if !exact_scope {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_CHAIN_SUBMISSION,
                Some(submission.settlement_chain_submission_id),
                "prepared governed-action submission does not match the authorized scope",
            ));
        }
        if action_submission(&txn, action.settlement_governed_action_id)
            .await?
            .is_some()
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_GOVERNED_ACTION,
                Some(action.settlement_governed_action_id),
                "governed action already has a durable submission identity",
            ));
        }
        let inserted = ChainSubmissionEntity::insert(command.submission.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(inserted.into())
    }

    async fn begin_action_dispatch(
        &self,
        command: BeginGovernedActionDispatch,
    ) -> Result<SettlementChainSubmissionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let action = lock_governed_action(&txn, command.settlement_governed_action_id).await?;
        require_live_action_claim(&action, &command.owner, command.dispatching_at)?;
        let submission = lock_action_submission(
            &txn,
            action.settlement_governed_action_id,
            command.settlement_chain_submission_id,
        )
        .await?;
        let exact_scope = action.scope_digest == command.expected_scope_digest
            && submission.state == SettlementSubmissionState::Prepared
            && submission.target_adapter == command.expected_target_adapter
            && submission.deployment_digest == command.expected_deployment_digest
            && submission.calldata_hash == command.expected_calldata_hash
            && submission.signed_envelope_hash == Some(command.expected_signed_envelope_hash);
        if !exact_scope {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_CHAIN_SUBMISSION,
                Some(submission.settlement_chain_submission_id),
                "governed action prepare-to-dispatch scope CAS failed",
            ));
        }
        let mut active = submission.into_active_model();
        active.state = ActiveValue::Set(SettlementSubmissionState::Dispatching);
        active.dispatched_at = ActiveValue::Set(Some(command.dispatching_at));
        let dispatching = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(dispatching.into())
    }

    async fn record_action_eoa_broadcast(
        &self,
        command: RecordGovernedActionEoaBroadcast,
    ) -> Result<SettlementChainSubmissionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let action = lock_governed_action(&txn, command.settlement_governed_action_id).await?;
        require_live_action_claim(&action, &command.owner, command.observed_at)?;
        let submission = lock_action_submission(
            &txn,
            action.settlement_governed_action_id,
            command.settlement_chain_submission_id,
        )
        .await?;
        if submission.state != SettlementSubmissionState::Dispatching
            || submission.kind != SettlementSubmissionKind::DirectEoa
            || submission.signed_envelope_hash != Some(command.expected_signed_envelope_hash)
            || submission.transaction_hash.is_none()
            || submission.relayer_transaction_id.is_some()
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_CHAIN_SUBMISSION,
                Some(submission.settlement_chain_submission_id),
                "governed EOA broadcast identity/state CAS failed",
            ));
        }
        let mut active = submission.into_active_model();
        active.state = ActiveValue::Set(SettlementSubmissionState::AwaitingFinality);
        active.chain_hash_observed_at = ActiveValue::Set(Some(command.observed_at));
        let awaiting = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(awaiting.into())
    }

    async fn record_action_relayer_acceptance(
        &self,
        command: RecordGovernedActionRelayerAcceptance,
    ) -> Result<SettlementChainSubmissionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let action = lock_governed_action(&txn, command.settlement_governed_action_id).await?;
        require_live_action_claim(&action, &command.owner, command.observed_at)?;
        let submission = lock_action_submission(
            &txn,
            action.settlement_governed_action_id,
            command.settlement_chain_submission_id,
        )
        .await?;
        if submission.state != SettlementSubmissionState::Dispatching
            || submission.kind != SettlementSubmissionKind::Relayer
            || submission.signed_envelope_hash != Some(command.expected_signed_envelope_hash)
            || submission.transaction_hash.is_some()
            || submission.relayer_transaction_id.is_some()
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_CHAIN_SUBMISSION,
                Some(submission.settlement_chain_submission_id),
                "governed relayer acceptance identity/state CAS failed",
            ));
        }
        let mut active = submission.into_active_model();
        active.state = ActiveValue::Set(SettlementSubmissionState::AwaitingChainHash);
        active.relayer_transaction_id = ActiveValue::Set(Some(command.relayer_transaction_id));
        let awaiting = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(awaiting.into())
    }

    async fn record_action_relayer_chain_hash(
        &self,
        command: RecordGovernedActionRelayerChainHash,
    ) -> Result<SettlementChainSubmissionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let action = lock_governed_action(&txn, command.settlement_governed_action_id).await?;
        require_live_action_claim(&action, &command.owner, command.observed_at)?;
        let submission = lock_action_submission(
            &txn,
            action.settlement_governed_action_id,
            command.settlement_chain_submission_id,
        )
        .await?;
        if submission.state != SettlementSubmissionState::AwaitingChainHash
            || submission.kind != SettlementSubmissionKind::Relayer
            || submission.relayer_transaction_id.as_ref()
                != Some(&command.expected_relayer_transaction_id)
            || submission.transaction_hash.is_some()
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_CHAIN_SUBMISSION,
                Some(submission.settlement_chain_submission_id),
                "governed relayer chain-hash identity/state CAS failed",
            ));
        }
        let mut active = submission.into_active_model();
        active.state = ActiveValue::Set(SettlementSubmissionState::AwaitingFinality);
        active.transaction_hash = ActiveValue::Set(Some(command.transaction_hash));
        active.chain_hash_observed_at = ActiveValue::Set(Some(command.observed_at));
        let awaiting = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(awaiting.into())
    }

    async fn schedule_action_retry(
        &self,
        command: ScheduleGovernedActionRetry,
    ) -> Result<SettlementGovernedActionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let action = lock_governed_action(&txn, command.settlement_governed_action_id).await?;
        require_live_action_claim(&action, &command.owner, command.scheduled_at)?;
        if action.scope_digest != command.expected_scope_digest
            || !matches!(
                action.state,
                SettlementGovernedActionState::Authorized
                    | SettlementGovernedActionState::RetryScheduled
            )
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_GOVERNED_ACTION,
                Some(action.settlement_governed_action_id),
                "governed action retry scope/state CAS failed",
            ));
        }
        let retry_count = action.retry_count.checked_add(1).ok_or_else(|| {
            error::invariant_violation(
                Some(QUANT_SETTLEMENT_GOVERNED_ACTION),
                "governed action retry count overflow",
            )
        })?;
        let mut active = action.into_active_model();
        active.state = ActiveValue::Set(SettlementGovernedActionState::RetryScheduled);
        active.failure_code = ActiveValue::Set(Some(command.failure_code));
        active.retry_count = ActiveValue::Set(retry_count);
        active.next_attempt_at = ActiveValue::Set(Some(command.next_attempt_at));
        active.last_error = ActiveValue::Set(Some(command.last_error));
        active.claim_owner = ActiveValue::Set(None);
        active.lease_expires_at = ActiveValue::Set(None);
        let scheduled = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(scheduled.into())
    }

    async fn schedule_action_work(
        &self,
        command: ScheduleGovernedActionWork,
    ) -> Result<SettlementGovernedActionInfo, StorageError> {
        if command.next_attempt_at <= command.scheduled_at {
            return Err(error::invariant_violation(
                Some(QUANT_SETTLEMENT_GOVERNED_ACTION),
                "governed action next work time must be in the future",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let action = lock_governed_action(&txn, command.settlement_governed_action_id).await?;
        require_live_action_claim(&action, &command.owner, command.scheduled_at)?;
        if action.scope_digest != command.expected_scope_digest
            || !matches!(
                action.state,
                SettlementGovernedActionState::Authorized
                    | SettlementGovernedActionState::RetryScheduled
            )
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_GOVERNED_ACTION,
                Some(action.settlement_governed_action_id),
                "governed action polling schedule scope/state CAS failed",
            ));
        }
        let mut active = action.into_active_model();
        active.next_attempt_at = ActiveValue::Set(Some(command.next_attempt_at));
        active.claim_owner = ActiveValue::Set(None);
        active.lease_expires_at = ActiveValue::Set(None);
        let scheduled = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(scheduled.into())
    }

    async fn fail_action(
        &self,
        command: FailSettlementGovernedAction,
    ) -> Result<SettlementGovernedActionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let action = lock_governed_action(&txn, command.settlement_governed_action_id).await?;
        require_live_action_claim(&action, &command.owner, command.failed_at)?;
        if action.scope_digest != command.expected_scope_digest
            || !matches!(
                action.state,
                SettlementGovernedActionState::Authorized
                    | SettlementGovernedActionState::RetryScheduled
            )
            || action_submission(&txn, action.settlement_governed_action_id)
                .await?
                .is_some()
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_GOVERNED_ACTION,
                Some(action.settlement_governed_action_id),
                "governed action terminal failure scope/state CAS failed",
            ));
        }
        let mut active = action.into_active_model();
        active.state = ActiveValue::Set(SettlementGovernedActionState::Failed);
        active.failure_code = ActiveValue::Set(Some(command.failure_code));
        active.last_error = ActiveValue::Set(Some(command.last_error));
        active.next_attempt_at = ActiveValue::Set(None);
        active.claim_owner = ActiveValue::Set(None);
        active.lease_expires_at = ActiveValue::Set(None);
        let failed = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(failed.into())
    }

    async fn confirm_action(
        &self,
        command: ConfirmSettlementGovernedAction,
    ) -> Result<SettlementGovernedActionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let action = lock_governed_action(&txn, command.settlement_governed_action_id).await?;
        require_live_action_claim(&action, &command.owner, command.confirmed_at)?;
        let submission = lock_action_submission(
            &txn,
            action.settlement_governed_action_id,
            command.settlement_chain_submission_id,
        )
        .await?;
        let SettlementChainReceiptEvidence::OperatorApproval(evidence) = &command.receipt_evidence
        else {
            return Err(error::invariant_violation(
                Some(QUANT_SETTLEMENT_CHAIN_SUBMISSION),
                "governed action confirmation requires operator-approval receipt evidence",
            ));
        };
        if action.scope_digest != command.expected_scope_digest
            || submission.state != SettlementSubmissionState::AwaitingFinality
            || action.desired_approval != Some(evidence.desired_approval)
            || !evidence.receipt_success
            || !evidence.operator_approved.eq(&evidence.desired_approval)
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_GOVERNED_ACTION,
                Some(action.settlement_governed_action_id),
                "governed action confirmation evidence/scope CAS failed",
            ));
        }
        let mut submission_active = submission.into_active_model();
        submission_active.state = ActiveValue::Set(SettlementSubmissionState::Confirmed);
        submission_active.receipt_evidence_json = ActiveValue::Set(Some(command.receipt_evidence));
        submission_active.confirmed_at = ActiveValue::Set(Some(command.confirmed_at));
        submission_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;
        let mut action_active = action.into_active_model();
        action_active.state = ActiveValue::Set(SettlementGovernedActionState::Consumed);
        action_active.consumed_at = ActiveValue::Set(Some(command.confirmed_at));
        action_active.failure_code = ActiveValue::Set(None);
        action_active.next_attempt_at = ActiveValue::Set(None);
        action_active.last_error = ActiveValue::Set(None);
        action_active.claim_owner = ActiveValue::Set(None);
        action_active.lease_expires_at = ActiveValue::Set(None);
        let confirmed = action_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(confirmed.into())
    }

    async fn require_action_reconciliation(
        &self,
        command: RequireGovernedActionReconciliation,
    ) -> Result<SettlementGovernedActionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let action = lock_governed_action(&txn, command.settlement_governed_action_id).await?;
        require_live_action_claim(&action, &command.owner, command.observed_at)?;
        let submission = lock_action_submission(
            &txn,
            action.settlement_governed_action_id,
            command.settlement_chain_submission_id,
        )
        .await?;
        if action.scope_digest != command.expected_scope_digest {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_GOVERNED_ACTION,
                Some(action.settlement_governed_action_id),
                "governed action reconciliation scope CAS failed",
            ));
        }
        let mut submission_active = submission.into_active_model();
        submission_active.state = ActiveValue::Set(SettlementSubmissionState::Failed);
        submission_active.failure_code = ActiveValue::Set(Some(command.failure_code));
        submission_active.last_error = ActiveValue::Set(Some(command.last_error.clone()));
        submission_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;
        let mut action_active = action.into_active_model();
        action_active.state =
            ActiveValue::Set(SettlementGovernedActionState::ReconciliationRequired);
        action_active.failure_code = ActiveValue::Set(Some(command.failure_code));
        action_active.last_error = ActiveValue::Set(Some(command.last_error));
        action_active.next_attempt_at = ActiveValue::Set(None);
        action_active.claim_owner = ActiveValue::Set(None);
        action_active.lease_expires_at = ActiveValue::Set(None);
        let failed = action_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(failed.into())
    }

    async fn find_authorized_canary(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
        authorization_digest: ContentHash,
        route: SettlementRoute,
        target_adapter: &EvmAddress,
        deployment_digest: ContentHash,
        now: DateTime<Utc>,
    ) -> Result<Option<SettlementGovernedActionInfo>, StorageError> {
        GovernedActionEntity::find()
            .filter(GovernedActionColumn::Kind.eq(SettlementGovernedActionKind::CanaryGrant))
            .filter(GovernedActionColumn::State.eq(SettlementGovernedActionState::Authorized))
            .filter(GovernedActionColumn::SettlementRedeemId.eq(*settlement_redeem_id))
            .filter(GovernedActionColumn::AuthorizationDigest.eq(authorization_digest))
            .filter(GovernedActionColumn::Route.eq(route))
            .filter(GovernedActionColumn::TargetAdapter.eq(target_adapter))
            .filter(GovernedActionColumn::DeploymentDigest.eq(deployment_digest))
            .filter(GovernedActionColumn::ExpiresAt.gt(now))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|model| model.map(Into::into))
    }

    async fn has_confirmed_canary(
        &self,
        execution_account_id: &ExecutionAccountId,
        route: SettlementRoute,
        deployment_digest: ContentHash,
    ) -> Result<bool, StorageError> {
        let canary_ids = GovernedActionEntity::find()
            .select_only()
            .column(GovernedActionColumn::SettlementGovernedActionId)
            .filter(GovernedActionColumn::ExecutionAccountId.eq(*execution_account_id))
            .filter(GovernedActionColumn::Kind.eq(SettlementGovernedActionKind::CanaryGrant))
            .filter(GovernedActionColumn::State.eq(SettlementGovernedActionState::Consumed))
            .filter(GovernedActionColumn::Route.eq(route))
            .filter(GovernedActionColumn::DeploymentDigest.eq(deployment_digest))
            .into_tuple::<SettlementGovernedActionId>()
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        if canary_ids.is_empty() {
            return Ok(false);
        }
        ChainSubmissionEntity::find()
            .filter(ChainSubmissionColumn::CanaryActionId.is_in(canary_ids))
            .filter(ChainSubmissionColumn::Purpose.eq(SettlementSubmissionPurpose::Redeem))
            .filter(ChainSubmissionColumn::State.eq(SettlementSubmissionState::Confirmed))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|submission| submission.is_some())
    }
}

#[async_trait::async_trait]
impl SettlementExternalCursorRepository for PgSettlementGovernanceRepository {
    async fn ensure_cursor(
        &self,
        cursor: NewSettlementExternalCursor,
    ) -> Result<SettlementExternalCursorInfo, StorageError> {
        let expected = cursor.clone();
        ExternalCursorEntity::insert(cursor.into_active_model())
            .on_conflict(
                OnConflict::columns([
                    ExternalCursorColumn::ExecutionAccountId,
                    ExternalCursorColumn::TargetAdapter,
                    ExternalCursorColumn::DeploymentDigest,
                ])
                .do_nothing()
                .to_owned(),
            )
            .exec_without_returning(&self.db)
            .await
            .map_err(StorageError::from)?;
        let stored = ExternalCursorEntity::find()
            .filter(ExternalCursorColumn::ExecutionAccountId.eq(expected.execution_account_id))
            .filter(ExternalCursorColumn::TargetAdapter.eq(&expected.target_adapter))
            .filter(ExternalCursorColumn::DeploymentDigest.eq(expected.deployment_digest))
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                error::not_found(
                    QUANT_SETTLEMENT_EXTERNAL_CURSOR,
                    expected.settlement_external_cursor_id,
                )
            })?;
        let exact = stored.chain_id == expected.chain_id
            && stored.route == expected.route
            && stored.target_code_hash == expected.target_code_hash
            && stored.deployment_evidence_version == expected.deployment_evidence_version;
        if !exact {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_EXTERNAL_CURSOR,
                Some(stored.settlement_external_cursor_id),
                "external cursor deployment identity is inconsistent",
            ));
        }
        Ok(stored.into())
    }

    async fn find_cursor(
        &self,
        cursor_id: &SettlementExternalCursorId,
    ) -> Result<Option<SettlementExternalCursorInfo>, StorageError> {
        ExternalCursorEntity::find_by_id(*cursor_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|model| model.map(Into::into))
    }

    async fn advance_cursor(
        &self,
        command: AdvanceSettlementExternalCursor,
    ) -> Result<SettlementExternalCursorInfo, StorageError> {
        self.persist_scan(PersistExternalSettlementScan {
            cursor: command,
            submissions: Vec::new(),
            observed_at: Utc::now(),
        })
        .await
    }

    async fn persist_scan(
        &self,
        command: PersistExternalSettlementScan,
    ) -> Result<SettlementExternalCursorInfo, StorageError> {
        validate_cursor_advance(&command.cursor)?;
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let cursor = ExternalCursorEntity::find_by_id(command.cursor.settlement_external_cursor_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                error::not_found(
                    QUANT_SETTLEMENT_EXTERNAL_CURSOR,
                    command.cursor.settlement_external_cursor_id,
                )
            })?;
        if cursor.next_block_number != command.cursor.expected_next_block_number {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_EXTERNAL_CURSOR,
                Some(cursor.settlement_external_cursor_id),
                "external cursor compare-and-swap lost",
            ));
        }
        for submission in command.submissions {
            validate_external_submission(&cursor, &submission)?;
            let redeem_id = submission.settlement_redeem_id.ok_or_else(|| {
                error::invariant_violation(
                    Some(QUANT_SETTLEMENT_CHAIN_SUBMISSION),
                    "external observation requires a settlement case parent",
                )
            })?;
            let redeem = SettlementRedeemEntity::find_by_id(redeem_id)
                .lock_exclusive()
                .one(&txn)
                .await
                .map_err(StorageError::from)?
                .ok_or_else(|| error::not_found(QUANT_SETTLEMENT_REDEEM, redeem_id))?;
            validate_external_case(&cursor, &redeem, &submission)?;
            let verified_block_number = submission.verified_block_number;
            let verified_block_hash = submission.verified_block_hash.clone();
            let submission_state = submission.state;
            let submission_failure_code = submission.failure_code;
            let submission_attempt_ordinal = submission.attempt_ordinal;
            ChainSubmissionEntity::insert(submission.into_active_model())
                .exec_without_returning(&txn)
                .await
                .map_err(StorageError::from)?;
            let mut active = redeem.into_active_model();
            if submission_state == SettlementSubmissionState::AwaitingFinality {
                active.state = ActiveValue::Set(SettlementCaseState::Submitted);
                active.verified_block_number = ActiveValue::Set(Some(verified_block_number));
                active.verified_block_hash = ActiveValue::Set(Some(verified_block_hash));
                active.next_attempt_at = ActiveValue::Set(Some(command.observed_at));
            } else {
                active.state = ActiveValue::Set(SettlementCaseState::ReconciliationRequired);
                active.reconciliation_state =
                    ActiveValue::Set(SettlementReconciliationState::OperatorReviewRequired);
                active.failure_code = ActiveValue::Set(submission_failure_code);
                active.next_attempt_at = ActiveValue::Set(None);
                active.failed_at = ActiveValue::Set(Some(command.observed_at));
            }
            active.attempt_count = ActiveValue::Set(submission_attempt_ordinal);
            active.submitted_at = ActiveValue::Set(Some(command.observed_at));
            active.claim_owner = ActiveValue::Set(None);
            active.lease_expires_at = ActiveValue::Set(None);
            active.updated_at = ActiveValue::Set(command.observed_at);
            active.update(&txn).await.map_err(StorageError::from)?;
        }
        let mut active = cursor.into_active_model();
        active.next_block_number = ActiveValue::Set(command.cursor.next_block_number);
        active.last_observed_block_number =
            ActiveValue::Set(Some(command.cursor.last_observed_block_number));
        active.last_observed_block_hash =
            ActiveValue::Set(Some(command.cursor.last_observed_block_hash));
        active.updated_at = ActiveValue::Set(command.observed_at);
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }
}

fn validate_cursor_advance(command: &AdvanceSettlementExternalCursor) -> Result<(), StorageError> {
    if command.next_block_number <= command.expected_next_block_number
        || command.last_observed_block_number < command.expected_next_block_number
        || command.next_block_number != command.last_observed_block_number + 1
    {
        return Err(error::invariant_violation(
            Some(QUANT_SETTLEMENT_EXTERNAL_CURSOR),
            "external cursor must advance one contiguous canonical block range",
        ));
    }
    Ok(())
}

fn validate_external_submission(
    cursor: &ExternalCursorModel,
    submission: &NewSettlementChainSubmission,
) -> Result<(), StorageError> {
    if submission.kind != SettlementSubmissionKind::ExternallyObserved
        || submission.purpose != SettlementSubmissionPurpose::Redeem
        || !matches!(
            submission.state,
            SettlementSubmissionState::AwaitingFinality | SettlementSubmissionState::Failed
        )
        || submission.settlement_governed_action_id.is_some()
        || submission.canary_action_id.is_some()
        || submission.transaction_hash.is_none()
        || submission.signed_envelope.is_some()
        || submission.signed_envelope_hash.is_some()
        || submission.prepared_nonce.is_some()
        || submission.gas_limit.is_some()
        || submission.route != cursor.route
        || submission.target_adapter != cursor.target_adapter
        || submission.target_code_hash != cursor.target_code_hash
        || submission.deployment_digest != cursor.deployment_digest
        || submission.deployment_evidence_version != cursor.deployment_evidence_version
        || (submission.state == SettlementSubmissionState::Failed
            && (submission.failure_code.is_none()
                || submission.failure_history_json.entries.is_empty()
                || submission.last_error.is_none()))
    {
        return Err(error::invariant_violation(
            Some(QUANT_SETTLEMENT_CHAIN_SUBMISSION),
            "external submission does not match the exact cursor deployment scope",
        ));
    }
    Ok(())
}

fn validate_external_case(
    cursor: &ExternalCursorModel,
    redeem: &SettlementRedeemModel,
    submission: &NewSettlementChainSubmission,
) -> Result<(), StorageError> {
    let exact_ready_scope = redeem.readiness_status == SettlementReadinessStatus::Ready
        && redeem.target_adapter.as_ref() == Some(&submission.target_adapter)
        && redeem.target_code_hash.as_ref() == Some(&submission.target_code_hash)
        && redeem.deployment_digest == Some(submission.deployment_digest)
        && redeem.deployment_evidence_version.as_ref()
            == Some(&submission.deployment_evidence_version)
        && redeem.balance_before_json.is_some()
        && redeem.expected_payout_usd.is_some()
        && redeem
            .verified_block_number
            .is_some_and(|block| submission.verified_block_number >= block);
    if redeem.execution_account_id != cursor.execution_account_id
        || redeem.route != cursor.route
        || (submission.state == SettlementSubmissionState::AwaitingFinality && !exact_ready_scope)
        || (submission.state == SettlementSubmissionState::Failed && exact_ready_scope)
    {
        return Err(error::state_conflict(
            QUANT_SETTLEMENT_REDEEM,
            Some(redeem.settlement_redeem_id),
            "external redemption cannot be attached without exact frozen pre-redemption evidence",
        ));
    }
    Ok(())
}
