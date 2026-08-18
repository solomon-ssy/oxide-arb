//! Atomic account execution association and incident opening.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity::QUANT_ACCOUNT_CHAIN_EXECUTION};
use quant_pivot_models::{
    domain::quant::{
        AccountExecutionAssociationOutcome, AccountRecoveryIncidentInfo,
        NewAccountExecutionAssociation, NewAccountRecoveryIncident,
    },
    entities::{
        quant_account_chain_execution::{Entity as ChainExecutionEntity, Model as ChainExecution},
        quant_account_execution_association::{Entity as AssociationEntity, Model as Association},
        quant_account_recovery_incident::{Column as IncidentColumn, Entity as IncidentEntity},
        quant_execution_order::{Column as OrderColumn, Entity as OrderEntity},
        quant_order_intent::Entity as IntentEntity,
    },
    enums::execution::{
        AccountExecutionAssociationKind, AccountRecoveryIncidentKind, AccountRecoveryIncidentStatus,
    },
    hashing::CanonicalDigest,
    types::{
        AccountChainExecutionId, AccountRecoveryIncidentId, ExecutionAccountId, ExecutionOrderId,
    },
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait,
    IntoActiveModel, QueryFilter, QuerySelect, Statement, TransactionTrait,
};

use crate::{postgres::primitives, traits::AccountRecoveryRepository};

const ASSOCIATION_HASH_DOMAIN: &str = "quant-pivot/account-execution-association";
const ASSOCIATION_HASH_VERSION: u32 = 1;

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
}
