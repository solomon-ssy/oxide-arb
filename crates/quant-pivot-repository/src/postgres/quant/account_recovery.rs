//! Atomic account execution association, recovery manifests, and lot allocation.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{
    StorageError,
    entity::{
        QUANT_ACCOUNT_CHAIN_EXECUTION, QUANT_ACCOUNT_CLEAN_FUNDER_BLOCKER,
        QUANT_ACCOUNT_RECOVERY_INCIDENT, QUANT_ACCOUNT_RECOVERY_MANIFEST,
        QUANT_STRATEGY_POSITION_LOT,
    },
};
use quant_pivot_models::{
    domain::quant::{
        AccountChainExecutionInfo, AccountCleanFunderBlockerInfo,
        AccountExecutionAssociationOutcome, AccountRecoveryCreatedLots,
        AccountRecoveryIncidentInfo, AccountRecoveryLotAllocation, AccountRecoveryManifestDraft,
        AccountRecoveryManifestInfo, FinalizeAccountRecoveryIncident, NewAccountCleanFunderBlocker,
        NewAccountExecutionAssociation, NewAccountRecoveryIncident, NewAccountRecoveryManifest,
        SealAccountRecoveryIncident,
    },
    entities::{
        quant_account_chain_execution::{
            Column as ChainExecutionColumn, Entity as ChainExecutionEntity, Model as ChainExecution,
        },
        quant_account_clean_funder_blocker::Entity as CleanFunderBlockerEntity,
        quant_account_execution_association::{
            Column as AssociationColumn, Entity as AssociationEntity, Model as Association,
        },
        quant_account_pause_operation::{
            Column as PauseOperationColumn, Entity as PauseOperationEntity,
        },
        quant_account_recovery_incident::{Column as IncidentColumn, Entity as IncidentEntity},
        quant_account_recovery_manifest::{
            Column as ManifestColumn, Entity as ManifestEntity, Model as Manifest,
        },
        quant_execution_order::{Column as OrderColumn, Entity as OrderEntity},
        quant_order_intent::Entity as IntentEntity,
        quant_strategy_position_lot::{
            ActiveModel as LotActiveModel, Entity as LotEntity, Model as LotModel,
        },
    },
    enums::execution::{
        AccountChainExecutionRole, AccountExecutionAssociationKind, AccountPauseOperationKind,
        AccountPauseOperationState, AccountRecoveryIncidentKind, AccountRecoveryIncidentStatus,
        PositionLedgerState, StrategyPositionOriginKind,
    },
    hashing::CanonicalDigest,
    types::{
        AccountChainExecutionId, AccountRecoveryIncidentId, AccountRecoveryManifestId,
        ExecutionAccountId, ExecutionOrderId, Price,
    },
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, ConnectionTrait, DatabaseBackend,
    DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder, QuerySelect,
    Statement, TransactionTrait,
};

use crate::{postgres::primitives, traits::AccountRecoveryRepository};

const ASSOCIATION_HASH_DOMAIN: &str = "quant-pivot/account-execution-association";
const ASSOCIATION_HASH_VERSION: u32 = 1;
const CLEAN_FUNDER_HASH_DOMAIN: &str = "quant-pivot/account-clean-funder-blocker";
const CLEAN_FUNDER_HASH_VERSION: u32 = 1;

pub struct PgAccountRecoveryRepository {
    db: DatabaseConnection,
}

impl PgAccountRecoveryRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    async fn lock_account(
        db: &impl ConnectionTrait,
        execution_account_id: ExecutionAccountId,
    ) -> Result<(), StorageError> {
        db.execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            [format!("account-recovery:{execution_account_id}").into()],
        ))
        .await
        .map_err(StorageError::from)?;
        Ok(())
    }

    async fn system_order(
        db: &impl ConnectionTrait,
        execution: &ChainExecution,
    ) -> Result<Option<ExecutionOrderId>, StorageError> {
        let Some(order) = OrderEntity::find()
            .filter(OrderColumn::VenueOrderId.eq(execution.order_id.clone()))
            .one(db)
            .await
            .map_err(StorageError::from)?
        else {
            return Ok(None);
        };
        let intent = IntentEntity::find_by_id(order.order_intent_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found("quant_order_intent", order.order_intent_id))?;
        Ok(
            (intent.execution_account_id == execution.execution_account_id)
                .then_some(order.execution_order_id),
        )
    }

    async fn open_incident(
        db: &impl ConnectionTrait,
        execution: &ChainExecution,
        opened_at: DateTime<Utc>,
    ) -> Result<(AccountRecoveryIncidentInfo, bool), StorageError> {
        if let Some(existing) = IncidentEntity::find()
            .filter(IncidentColumn::ExecutionAccountId.eq(execution.execution_account_id))
            .filter(IncidentColumn::Status.is_in([
                AccountRecoveryIncidentStatus::Open,
                AccountRecoveryIncidentStatus::Reconciling,
            ]))
            .one(db)
            .await
            .map_err(StorageError::from)?
        {
            return Ok((existing.into(), false));
        }
        let incident = NewAccountRecoveryIncident {
            account_recovery_incident_id: AccountRecoveryIncidentId::from_v7(),
            execution_account_id: execution.execution_account_id,
            kind: AccountRecoveryIncidentKind::UnknownExternalExecution,
            status: AccountRecoveryIncidentStatus::Open,
            trigger_chain_execution_id: Some(execution.account_chain_execution_id),
            reason: format!(
                "finalized account execution {} has no system order",
                execution.account_chain_execution_id
            ),
            opened_at,
            seal_hash: None,
            sealed_by: None,
            sealed_at: None,
            revision: 0,
        };
        let stored = IncidentEntity::insert(incident.into_active_model())
            .exec_with_returning(db)
            .await
            .map_err(StorageError::from)?;
        Ok((stored.into(), true))
    }

    async fn incident_by_association(
        db: &impl ConnectionTrait,
        association: &Association,
    ) -> Result<Option<AccountRecoveryIncidentInfo>, StorageError> {
        let Some(id) = association.recovery_incident_id else {
            return Ok(None);
        };
        IncidentEntity::find_by_id(id)
            .one(db)
            .await
            .map_err(StorageError::from)
            .map(|incident| incident.map(Into::into))
    }

    fn exact_manifest(stored: &Manifest, draft: &AccountRecoveryManifestDraft) -> bool {
        let mut stored_input = stored.input_json.clone();
        stored_input.observed_at = draft.input.observed_at;
        stored.recovery_incident_id == draft.recovery_incident_id
            && stored_input == draft.input
            && stored.assessment_json == draft.assessment
            && stored.created_lots_json.0 == draft.created_lots
            && stored.evidence_hash == draft.assessment.evidence_hash
    }

    async fn apply_allocations(
        db: &impl ConnectionTrait,
        draft: &AccountRecoveryManifestDraft,
    ) -> Result<(), StorageError> {
        if !draft.assessment.converged() {
            if draft.created_lots.is_empty() {
                return Ok(());
            }
            return Err(StorageError::invariant_violation(
                Some(QUANT_ACCOUNT_RECOVERY_MANIFEST),
                "blocked recovery manifest cannot create position lots",
            ));
        }
        for allocation in &draft.assessment.allocations {
            let row = LotEntity::find_by_id(allocation.strategy_position_lot_id)
                .lock_exclusive()
                .one(db)
                .await
                .map_err(StorageError::from)?
                .ok_or_else(|| {
                    StorageError::not_found(
                        QUANT_STRATEGY_POSITION_LOT,
                        allocation.strategy_position_lot_id,
                    )
                })?;
            Self::validate_allocation(&row, allocation, draft)?;
            let realized_pnl_usd = row.realized_pnl_usd + allocation.realized_pnl_delta_usd;
            let mut active: LotActiveModel = row.into();
            active.shares = ActiveValue::Set(allocation.after_shares);
            active.cost_usd = ActiveValue::Set(allocation.after_cost_usd);
            active.realized_pnl_usd = ActiveValue::Set(realized_pnl_usd);
            active.updated_at = ActiveValue::Set(draft.input.observed_at);
            if allocation.after_shares.is_zero() {
                active.state = ActiveValue::Set(PositionLedgerState::Closed);
                active.closed_at = ActiveValue::Set(allocation.closed_at);
            } else {
                active.state = ActiveValue::Set(PositionLedgerState::Open);
                active.avg_price = ActiveValue::Set(Price::new(
                    allocation.after_cost_usd.inner() / allocation.after_shares.inner(),
                ));
                active.closed_at = ActiveValue::Set(None);
            }
            active.update(db).await.map_err(StorageError::from)?;
        }
        Self::insert_created_lots(db, draft).await
    }

    fn validate_allocation(
        row: &LotModel,
        allocation: &AccountRecoveryLotAllocation,
        draft: &AccountRecoveryManifestDraft,
    ) -> Result<(), StorageError> {
        if row.execution_account_id != draft.input.execution_account_id
            || row.token_id != allocation.token_id
            || row.shares != allocation.before_shares
            || row.cost_usd != allocation.before_cost_usd
            || allocation.after_shares.is_negative()
            || allocation.after_cost_usd.is_negative()
        {
            return Err(StorageError::state_conflict(
                QUANT_STRATEGY_POSITION_LOT,
                Some(&allocation.strategy_position_lot_id),
                "position lot changed after recovery assessment",
            ));
        }
        Ok(())
    }

    async fn insert_created_lots(
        db: &impl ConnectionTrait,
        draft: &AccountRecoveryManifestDraft,
    ) -> Result<(), StorageError> {
        let expected = draft
            .assessment
            .created_lots
            .iter()
            .map(|lot| (lot.strategy_position_lot_id, lot))
            .collect::<HashMap<_, _>>();
        if expected.len() != draft.created_lots.len() {
            return Err(StorageError::invariant_violation(
                Some(QUANT_ACCOUNT_RECOVERY_MANIFEST),
                "recovery lot materialization count differs from assessment",
            ));
        }
        for lot in &draft.created_lots {
            let Some(assessment) = expected.get(&lot.strategy_position_lot_id) else {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_ACCOUNT_RECOVERY_MANIFEST),
                    "recovery lot is absent from assessment",
                ));
            };
            if !assessment.acquired_shares.is_positive() {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_ACCOUNT_RECOVERY_MANIFEST),
                    "recovery BUY lot must have positive acquired shares",
                ));
            }
            let expected_state = if assessment.remaining_shares.is_zero() {
                PositionLedgerState::Closed
            } else {
                PositionLedgerState::Open
            };
            let expected_avg_price = Price::new(
                assessment.acquired_cost_usd.inner() / assessment.acquired_shares.inner(),
            );
            if lot.origin_kind != StrategyPositionOriginKind::AccountRecoveryIncident
                || lot.recovery_incident_id != Some(draft.recovery_incident_id)
                || lot.execution_account_id != draft.input.execution_account_id
                || lot.token_id != assessment.token_id
                || lot.shares != assessment.remaining_shares
                || lot.cost_usd != assessment.remaining_cost_usd
                || lot.realized_pnl_usd != assessment.realized_pnl_delta_usd
                || lot.avg_price != expected_avg_price
                || lot.state != expected_state
                || lot.closed_at != assessment.closed_at
            {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_ACCOUNT_RECOVERY_MANIFEST),
                    "recovery lot materialization differs from assessment",
                ));
            }
            LotEntity::insert(lot.clone().into_active_model())
                .exec(db)
                .await
                .map_err(StorageError::from)?;
        }
        Ok(())
    }

    async fn ensure_clean_funder(
        db: &impl ConnectionTrait,
        execution: &ChainExecution,
        incident_id: AccountRecoveryIncidentId,
    ) -> Result<(), StorageError> {
        if execution.role == AccountChainExecutionRole::Taker {
            return Ok(());
        }
        if CleanFunderBlockerEntity::find_by_id(incident_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .is_some()
        {
            return Ok(());
        }
        let evidence_hash = CanonicalDigest::content_hash_typed(
            CLEAN_FUNDER_HASH_DOMAIN,
            CLEAN_FUNDER_HASH_VERSION,
            &(
                incident_id,
                execution.account_chain_execution_id,
                execution.role,
                execution.source_event_hash,
            ),
        )
        .map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_ACCOUNT_CLEAN_FUNDER_BLOCKER),
                format!("clean-funder blocker hash failed: {error}"),
            )
        })?;
        CleanFunderBlockerEntity::insert(
            NewAccountCleanFunderBlocker {
                recovery_incident_id: incident_id,
                account_chain_execution_id: execution.account_chain_execution_id,
                role: execution.role,
                evidence_hash,
            }
            .into_active_model(),
        )
        .exec(db)
        .await
        .map_err(StorageError::from)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl AccountRecoveryRepository for PgAccountRecoveryRepository {
    async fn associate_execution(
        &self,
        execution_id: &AccountChainExecutionId,
        associated_at: DateTime<Utc>,
    ) -> Result<AccountExecutionAssociationOutcome, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let execution = ChainExecutionEntity::find_by_id(*execution_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_ACCOUNT_CHAIN_EXECUTION, execution_id))?;
        let database_now = primitives::statement_timestamp(&txn).await?;
        Self::lock_account(&txn, execution.execution_account_id).await?;
        if let Some(existing) = AssociationEntity::find_by_id(*execution_id)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        {
            let incident = Self::incident_by_association(&txn, &existing).await?;
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(AccountExecutionAssociationOutcome {
                association: existing.into(),
                incident,
                incident_created: false,
            });
        }
        let system_order = Self::system_order(&txn, &execution).await?;
        let (kind, execution_order_id, incident, incident_created) =
            if let Some(execution_order_id) = system_order {
                (
                    AccountExecutionAssociationKind::SystemOrder,
                    Some(execution_order_id),
                    None,
                    false,
                )
            } else {
                let (incident, created) =
                    Self::open_incident(&txn, &execution, database_now).await?;
                (
                    AccountExecutionAssociationKind::RecoveryIncident,
                    None,
                    Some(incident),
                    created,
                )
            };
        let recovery_incident_id = incident
            .as_ref()
            .map(|incident| incident.account_recovery_incident_id);
        if let Some(incident_id) = recovery_incident_id {
            Self::ensure_clean_funder(&txn, &execution, incident_id).await?;
        }
        let evidence_hash = CanonicalDigest::content_hash_typed(
            ASSOCIATION_HASH_DOMAIN,
            ASSOCIATION_HASH_VERSION,
            &(
                execution.account_chain_execution_id,
                kind,
                execution_order_id,
                recovery_incident_id,
                execution.source_event_hash,
            ),
        )
        .map_err(|error| {
            StorageError::invariant_violation(
                Some("quant_account_execution_association"),
                format!("association evidence hash failed: {error}"),
            )
        })?;
        let association = AssociationEntity::insert(
            NewAccountExecutionAssociation {
                account_chain_execution_id: execution.account_chain_execution_id,
                kind,
                execution_order_id,
                recovery_incident_id,
                evidence_hash,
                associated_at,
            }
            .into_active_model(),
        )
        .exec_with_returning(&txn)
        .await
        .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(AccountExecutionAssociationOutcome {
            association: association.into(),
            incident,
            incident_created,
        })
    }

    async fn active_incident(
        &self,
        execution_account_id: &ExecutionAccountId,
    ) -> Result<Option<AccountRecoveryIncidentInfo>, StorageError> {
        IncidentEntity::find()
            .filter(IncidentColumn::ExecutionAccountId.eq(*execution_account_id))
            .filter(IncidentColumn::Status.is_in([
                AccountRecoveryIncidentStatus::Open,
                AccountRecoveryIncidentStatus::Reconciling,
            ]))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|incident| incident.map(Into::into))
    }

    async fn find_incident(
        &self,
        incident_id: &AccountRecoveryIncidentId,
    ) -> Result<Option<AccountRecoveryIncidentInfo>, StorageError> {
        IncidentEntity::find_by_id(*incident_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|incident| incident.map(Into::into))
    }

    async fn incident_executions(
        &self,
        incident_id: &AccountRecoveryIncidentId,
    ) -> Result<Vec<AccountChainExecutionInfo>, StorageError> {
        let execution_ids = AssociationEntity::find()
            .select_only()
            .column(AssociationColumn::AccountChainExecutionId)
            .filter(AssociationColumn::RecoveryIncidentId.eq(*incident_id))
            .into_tuple::<AccountChainExecutionId>()
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        let mut executions = ChainExecutionEntity::find()
            .filter(ChainExecutionColumn::AccountChainExecutionId.is_in(execution_ids))
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(Into::into)
            .collect::<Vec<AccountChainExecutionInfo>>();
        executions.sort_by(|left, right| {
            left.available_at.cmp(&right.available_at).then_with(|| {
                left.account_chain_execution_id
                    .to_string()
                    .cmp(&right.account_chain_execution_id.to_string())
            })
        });
        Ok(executions)
    }

    async fn append_manifest(
        &self,
        draft: AccountRecoveryManifestDraft,
    ) -> Result<AccountRecoveryManifestInfo, StorageError> {
        if draft.input.finalized_block_number < 0
            || draft.input.recovery_incident_id != draft.recovery_incident_id
        {
            return Err(StorageError::invariant_violation(
                Some(QUANT_ACCOUNT_RECOVERY_MANIFEST),
                "account recovery finalized block cannot be negative",
            ));
        }
        let manifest_id =
            AccountRecoveryManifestId::from_content_hash(&draft.assessment.evidence_hash);
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let incident = IncidentEntity::find_by_id(draft.recovery_incident_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_ACCOUNT_RECOVERY_INCIDENT, draft.recovery_incident_id)
            })?;
        if incident.status == AccountRecoveryIncidentStatus::Sealed {
            return Err(StorageError::state_conflict(
                QUANT_ACCOUNT_RECOVERY_INCIDENT,
                Some(&draft.recovery_incident_id),
                "sealed incident cannot accept another recovery manifest",
            ));
        }
        if let Some(stored) = ManifestEntity::find_by_id(manifest_id)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        {
            if Self::exact_manifest(&stored, &draft) {
                txn.commit().await.map_err(StorageError::from)?;
                return Ok(stored.into());
            }
            return Err(StorageError::state_conflict(
                QUANT_ACCOUNT_RECOVERY_MANIFEST,
                Some(&manifest_id),
                "manifest hash replay changed its immutable payload",
            ));
        }
        let previous_attempt = ManifestEntity::find()
            .filter(ManifestColumn::RecoveryIncidentId.eq(draft.recovery_incident_id))
            .order_by_desc(ManifestColumn::AttemptNo)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .map(|manifest| manifest.attempt_no);
        let attempt_no = previous_attempt.map_or(Ok(1), |attempt| {
            attempt.checked_add(1).ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(QUANT_ACCOUNT_RECOVERY_MANIFEST),
                    "account recovery manifest attempt overflow",
                )
            })
        })?;
        let converged = draft.assessment.converged();
        let evidence_hash = draft.assessment.evidence_hash;
        Self::apply_allocations(&txn, &draft).await?;
        let stored = ManifestEntity::insert(
            NewAccountRecoveryManifest {
                account_recovery_manifest_id: manifest_id,
                recovery_incident_id: draft.recovery_incident_id,
                attempt_no,
                observed_at: draft.input.observed_at,
                finalized_block_number: draft.input.finalized_block_number,
                finalized_block_hash: draft.input.finalized_block_hash.clone(),
                converged,
                input_json: draft.input,
                assessment_json: draft.assessment,
                created_lots_json: AccountRecoveryCreatedLots(draft.created_lots),
                evidence_hash,
            }
            .into_active_model(),
        )
        .exec_with_returning(&txn)
        .await
        .map_err(StorageError::from)?;
        if incident.status == AccountRecoveryIncidentStatus::Open {
            let database_now = primitives::statement_timestamp(&txn).await?;
            let revision = incident.revision.checked_add(1).ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(QUANT_ACCOUNT_RECOVERY_INCIDENT),
                    "account recovery incident revision overflow",
                )
            })?;
            let mut active = incident.into_active_model();
            active.status = ActiveValue::Set(AccountRecoveryIncidentStatus::Reconciling);
            active.revision = ActiveValue::Set(revision);
            active.updated_at = ActiveValue::Set(database_now);
            active.update(&txn).await.map_err(StorageError::from)?;
        }
        txn.commit().await.map_err(StorageError::from)?;
        Ok(stored.into())
    }

    async fn latest_manifest(
        &self,
        incident_id: &AccountRecoveryIncidentId,
    ) -> Result<Option<AccountRecoveryManifestInfo>, StorageError> {
        ManifestEntity::find()
            .filter(ManifestColumn::RecoveryIncidentId.eq(*incident_id))
            .order_by_desc(ManifestColumn::AttemptNo)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|manifest| manifest.map(Into::into))
    }

    async fn clean_funder_blocker(
        &self,
        incident_id: &AccountRecoveryIncidentId,
    ) -> Result<Option<AccountCleanFunderBlockerInfo>, StorageError> {
        CleanFunderBlockerEntity::find_by_id(*incident_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|blocker| blocker.map(Into::into))
    }

    async fn seal_incident(
        &self,
        command: SealAccountRecoveryIncident,
    ) -> Result<AccountRecoveryIncidentInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let incident = IncidentEntity::find_by_id(command.recovery_incident_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    QUANT_ACCOUNT_RECOVERY_INCIDENT,
                    command.recovery_incident_id,
                )
            })?;
        let manifest = ManifestEntity::find_by_id(command.account_recovery_manifest_id)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    QUANT_ACCOUNT_RECOVERY_MANIFEST,
                    command.account_recovery_manifest_id,
                )
            })?;
        if CleanFunderBlockerEntity::find_by_id(command.recovery_incident_id)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .is_some()
        {
            return Err(StorageError::state_conflict(
                QUANT_ACCOUNT_CLEAN_FUNDER_BLOCKER,
                Some(&command.recovery_incident_id),
                "external resting-order evidence requires a clean funder",
            ));
        }
        let latest = ManifestEntity::find()
            .filter(ManifestColumn::RecoveryIncidentId.eq(command.recovery_incident_id))
            .order_by_desc(ManifestColumn::AttemptNo)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::state_conflict(
                    QUANT_ACCOUNT_RECOVERY_INCIDENT,
                    Some(&command.recovery_incident_id),
                    "incident has no recovery manifest",
                )
            })?;
        if manifest.recovery_incident_id != command.recovery_incident_id
            || latest.account_recovery_manifest_id != command.account_recovery_manifest_id
            || !manifest.converged
        {
            return Err(StorageError::state_conflict(
                QUANT_ACCOUNT_RECOVERY_INCIDENT,
                Some(&command.recovery_incident_id),
                "only the latest converged recovery manifest can be sealed",
            ));
        }
        if incident.seal_hash.is_some() {
            if incident.seal_hash == Some(manifest.evidence_hash)
                && incident.sealed_by == Some(command.actor)
            {
                txn.commit().await.map_err(StorageError::from)?;
                return Ok(incident.into());
            }
            return Err(StorageError::state_conflict(
                QUANT_ACCOUNT_RECOVERY_INCIDENT,
                Some(&command.recovery_incident_id),
                "incident already carries a different immutable seal",
            ));
        }
        if incident.status != AccountRecoveryIncidentStatus::Reconciling
            || incident.revision != command.expected_revision
        {
            return Err(StorageError::state_conflict(
                QUANT_ACCOUNT_RECOVERY_INCIDENT,
                Some(&command.recovery_incident_id),
                "incident revision or lifecycle changed before seal",
            ));
        }
        let revision = incident.revision.checked_add(1).ok_or_else(|| {
            StorageError::invariant_violation(
                Some(QUANT_ACCOUNT_RECOVERY_INCIDENT),
                "account recovery incident revision overflow",
            )
        })?;
        let mut active = incident.into_active_model();
        active.seal_hash = ActiveValue::Set(Some(manifest.evidence_hash));
        active.sealed_by = ActiveValue::Set(Some(command.actor));
        active.sealed_at = ActiveValue::Set(Some(command.sealed_at));
        active.revision = ActiveValue::Set(revision);
        active.updated_at = ActiveValue::Set(command.sealed_at);
        let stored = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(stored.into())
    }

    async fn finalize_incident(
        &self,
        command: FinalizeAccountRecoveryIncident,
    ) -> Result<AccountRecoveryIncidentInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let incident = IncidentEntity::find_by_id(command.recovery_incident_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    QUANT_ACCOUNT_RECOVERY_INCIDENT,
                    command.recovery_incident_id,
                )
            })?;
        if incident.status == AccountRecoveryIncidentStatus::Sealed {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(incident.into());
        }
        if incident.status != AccountRecoveryIncidentStatus::Reconciling
            || incident.revision != command.expected_revision
            || incident.seal_hash.is_none()
        {
            return Err(StorageError::state_conflict(
                QUANT_ACCOUNT_RECOVERY_INCIDENT,
                Some(&command.recovery_incident_id),
                "incident is not ready for terminal seal",
            ));
        }
        let operations = PauseOperationEntity::find()
            .filter(PauseOperationColumn::RecoveryIncidentId.eq(command.recovery_incident_id))
            .all(&txn)
            .await
            .map_err(StorageError::from)?;
        let pause_count = operations
            .iter()
            .filter(|operation| operation.operation_kind == AccountPauseOperationKind::Pause)
            .count();
        let unpause_count = operations
            .iter()
            .filter(|operation| operation.operation_kind == AccountPauseOperationKind::Unpause)
            .count();
        if pause_count == 0
            || pause_count != unpause_count
            || operations
                .iter()
                .any(|operation| operation.state != AccountPauseOperationState::Confirmed)
        {
            return Err(StorageError::state_conflict(
                QUANT_ACCOUNT_RECOVERY_INCIDENT,
                Some(&command.recovery_incident_id),
                "pause and unpause operations are not fully finalized",
            ));
        }
        let revision = incident.revision.checked_add(1).ok_or_else(|| {
            StorageError::invariant_violation(
                Some(QUANT_ACCOUNT_RECOVERY_INCIDENT),
                "account recovery incident revision overflow",
            )
        })?;
        let mut active = incident.into_active_model();
        active.status = ActiveValue::Set(AccountRecoveryIncidentStatus::Sealed);
        active.revision = ActiveValue::Set(revision);
        active.updated_at = ActiveValue::Set(command.finalized_at);
        let stored = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(stored.into())
    }
}
