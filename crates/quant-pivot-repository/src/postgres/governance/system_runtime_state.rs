//! Transactional `PostgreSQL` owner of runtime mode and cold-start bootstrap state.

use crate::{
    postgres::{
        governance::runtime_config::{append_activation_if_current, validate_operator_approval},
        primitives,
    },
    traits::SystemRuntimeStateRepository,
};
use async_trait::async_trait;
use chrono::Utc;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        ActivateBootstrapState, BootstrapActivationInfo, NewRuntimeConfigActivation,
        NewSystemBootstrapTransition, SystemRuntimeStateInfo,
    },
    entities::{
        runtime_config_version::Model as RuntimeConfig,
        system_bootstrap_transition::Entity as BootstrapTransitionEntity,
        system_kill_switch::Entity as KillSwitchEntity,
        system_runtime_state::{ActiveModel, Column, Entity, Model},
    },
    enums::{
        execution::KillSwitchState, quant::QuantRuntimeMode,
        runtime_config::RuntimeConfigActivationKind, system::BootstrapPhase,
    },
    types::{BootstrapTransitionId, RuntimeConfigActivationId, RuntimeConfigApprovalId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ColumnTrait, DatabaseConnection, EntityTrait,
    IntoActiveModel, QueryFilter, QuerySelect, TransactionTrait, sea_query::LockType,
};

const SINGLETON_ID: i32 = 1;

pub struct PgSystemRuntimeStateRepository {
    db: DatabaseConnection,
}

impl PgSystemRuntimeStateRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

async fn locked_state(txn: &sea_orm::DatabaseTransaction) -> Result<Model, StorageError> {
    Entity::find_by_id(SINGLETON_ID)
        .lock(LockType::Update)
        .one(txn)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::not_found("system_runtime_state", SINGLETON_ID))
}

struct PhaseTransition<'a> {
    to_phase: BootstrapPhase,
    actor: &'a str,
    acting_role: Option<&'a str>,
    reason: &'a str,
    runtime_config: Option<&'a RuntimeConfig>,
    runtime_config_approval_id: Option<RuntimeConfigApprovalId>,
    report_only_forced_ack: bool,
}

async fn persist_phase(
    txn: &sea_orm::DatabaseTransaction,
    state: Model,
    transition: PhaseTransition<'_>,
) -> Result<SystemRuntimeStateInfo, StorageError> {
    let PhaseTransition {
        to_phase,
        actor,
        acting_role,
        reason,
        runtime_config,
        runtime_config_approval_id,
        report_only_forced_ack,
    } = transition;
    let from_phase = state.bootstrap_phase;
    let next_revision = state.state_revision.checked_add(1).ok_or_else(|| {
        StorageError::invariant_violation(
            Some("system_runtime_state"),
            "bootstrap state revision overflow",
        )
    })?;
    let now = primitives::statement_timestamp(txn).await?;
    BootstrapTransitionEntity::insert(
        NewSystemBootstrapTransition {
            bootstrap_transition_id: BootstrapTransitionId::from_v7(),
            bootstrap_contract_version: state.bootstrap_contract_version,
            state_revision: next_revision,
            from_phase,
            to_phase,
            runtime_config_version_id: runtime_config
                .map(|version| version.runtime_config_version_id.clone()),
            runtime_config_approval_id,
            actor: actor.to_owned(),
            acting_role: acting_role.map(str::to_owned),
            reason: reason.to_owned(),
            report_only_forced_ack,
            occurred_at: now,
        }
        .into_active_model(),
    )
    .exec(txn)
    .await
    .map_err(StorageError::from)?;

    let mut active: ActiveModel = state.into_active_model();
    active.bootstrap_phase = Set(to_phase);
    active.state_revision = Set(next_revision);
    active.changed_by = Set(actor.to_owned());
    active.reason = Set(reason.to_owned());
    active.changed_at = Set(now);
    active
        .update(txn)
        .await
        .map(Into::into)
        .map_err(StorageError::from)
}

async fn idempotent_phase_transition(
    db: &DatabaseConnection,
    from_phase: BootstrapPhase,
    to_phase: BootstrapPhase,
    reason: &'static str,
) -> Result<SystemRuntimeStateInfo, StorageError> {
    let txn = db.begin().await.map_err(StorageError::from)?;
    let state = locked_state(&txn).await?;
    let already_advanced = matches!(
        (from_phase, state.bootstrap_phase),
        (
            BootstrapPhase::Initializing,
            BootstrapPhase::CollectingBaseline
                | BootstrapPhase::AwaitingActivation
                | BootstrapPhase::Active
        ) | (
            BootstrapPhase::CollectingBaseline,
            BootstrapPhase::AwaitingActivation | BootstrapPhase::Active
        )
    );
    if state.bootstrap_phase == to_phase || already_advanced {
        txn.commit().await.map_err(StorageError::from)?;
        return Ok(state.into());
    }
    if state.bootstrap_phase != from_phase {
        return Err(StorageError::illegal_transition(
            "system_runtime_state",
            Some(SINGLETON_ID),
            state.bootstrap_phase,
            to_phase,
        ));
    }
    let state = persist_phase(
        &txn,
        state,
        PhaseTransition {
            to_phase,
            actor: "system",
            acting_role: None,
            reason,
            runtime_config: None,
            runtime_config_approval_id: None,
            report_only_forced_ack: false,
        },
    )
    .await?;
    txn.commit().await.map_err(StorageError::from)?;
    Ok(state)
}

#[async_trait]
impl SystemRuntimeStateRepository for PgSystemRuntimeStateRepository {
    async fn load(&self) -> Result<Option<SystemRuntimeStateInfo>, StorageError> {
        Entity::find_by_id(SINGLETON_ID)
            .one(&self.db)
            .await
            .map(|model| model.map(Into::into))
            .map_err(StorageError::from)
    }

    async fn begin_baseline_collection(&self) -> Result<SystemRuntimeStateInfo, StorageError> {
        idempotent_phase_transition(
            &self.db,
            BootstrapPhase::Initializing,
            BootstrapPhase::CollectingBaseline,
            "schema and control-plane bootstrap verified",
        )
        .await
    }

    async fn mark_catalog_baseline_ready(&self) -> Result<SystemRuntimeStateInfo, StorageError> {
        idempotent_phase_transition(
            &self.db,
            BootstrapPhase::CollectingBaseline,
            BootstrapPhase::AwaitingActivation,
            "complete Gamma catalog baseline committed",
        )
        .await
    }

    async fn activate_bootstrap(
        &self,
        command: ActivateBootstrapState,
    ) -> Result<BootstrapActivationInfo, StorageError> {
        if !command.report_only_forced_ack {
            return Err(StorageError::invariant_violation(
                Some("system_runtime_state"),
                "ReportOnlyForced acknowledgement is required",
            ));
        }

        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let state = locked_state(&txn).await?;
        if state.bootstrap_phase != BootstrapPhase::AwaitingActivation {
            return Err(StorageError::illegal_transition(
                "system_runtime_state",
                Some(SINGLETON_ID),
                state.bootstrap_phase,
                BootstrapPhase::Active,
            ));
        }
        if state.bootstrap_contract_version != command.bootstrap_contract_version {
            return Err(StorageError::state_conflict(
                "system_runtime_state",
                Some(SINGLETON_ID),
                format!(
                    "bootstrap contract version changed; expected {}, current {}",
                    command.bootstrap_contract_version, state.bootstrap_contract_version
                ),
            ));
        }
        if state.state_revision != command.expected_state_revision {
            return Err(StorageError::state_conflict(
                "system_runtime_state",
                Some(SINGLETON_ID),
                format!(
                    "bootstrap state revision changed; expected {}, current {}",
                    command.expected_state_revision, state.state_revision
                ),
            ));
        }

        let kill_switch = KillSwitchEntity::find_by_id(SINGLETON_ID)
            .lock(LockType::Update)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found("system_kill_switch", SINGLETON_ID))?;
        if kill_switch.state != KillSwitchState::ReportOnlyForced
            || !kill_switch.requires_operator_ack
        {
            return Err(StorageError::state_conflict(
                "system_kill_switch",
                Some(SINGLETON_ID),
                "bootstrap activation requires ReportOnlyForced with operator acknowledgement",
            ));
        }

        let activation = NewRuntimeConfigActivation {
            runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
            runtime_config_version_id: command.runtime_config_version_id,
            runtime_config_approval_id: Some(command.runtime_config_approval_id),
            activated_by: command.actor.clone(),
            reason: command.reason.clone(),
            activation_kind: RuntimeConfigActivationKind::Initial,
            previous_runtime_config_version_id: None,
            rollback_target_version_id: None,
            audit_event_id: None,
        };
        let validated = validate_operator_approval(
            &txn,
            &activation,
            command.require_approver_activator_separation,
        )
        .await?;
        append_activation_if_current(&txn, None, Some(activation)).await?;

        let state = persist_phase(
            &txn,
            state,
            PhaseTransition {
                to_phase: BootstrapPhase::Active,
                actor: &command.actor,
                acting_role: Some(&command.acting_role),
                reason: &command.reason,
                runtime_config: Some(&validated.version),
                runtime_config_approval_id: Some(validated.approval.runtime_config_approval_id),
                report_only_forced_ack: true,
            },
        )
        .await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(BootstrapActivationInfo {
            state,
            runtime_config: validated.version.into(),
        })
    }

    async fn set_quant_runtime_mode(
        &self,
        mode: QuantRuntimeMode,
        changed_by: &str,
        reason: &str,
    ) -> Result<(), StorageError> {
        let result = Entity::update_many()
            .filter(Column::Id.eq(SINGLETON_ID))
            .filter(Column::BootstrapPhase.eq(BootstrapPhase::Active))
            .col_expr(
                Column::QuantRuntimeMode,
                sea_orm::sea_query::Expr::value(mode),
            )
            .col_expr(
                Column::ChangedBy,
                sea_orm::sea_query::Expr::value(changed_by),
            )
            .col_expr(Column::Reason, sea_orm::sea_query::Expr::value(reason))
            .col_expr(
                Column::ChangedAt,
                sea_orm::sea_query::Expr::value(Utc::now()),
            )
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        if result.rows_affected != 1 {
            return Err(StorageError::state_conflict(
                "system_runtime_state",
                Some(SINGLETON_ID),
                "quant runtime mode can change only after bootstrap activation",
            ));
        }
        Ok(())
    }
}
