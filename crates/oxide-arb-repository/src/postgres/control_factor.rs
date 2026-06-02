use crate::traits::ControlFactorRepository;
use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::control_factor::{
        ControlFactorAuditEventInfo, ControlFactorMaterializationRunInfo,
        ControlFactorPublicationInfo, ControlFactorPublicationRowInfo,
        ControlFactorStageReportInfo, ControlFactorValue, ControlFactorValueInfo,
        NewControlFactorAuditEvent, NewControlFactorMaterializationRun,
        NewControlFactorPublication, NewControlFactorPublicationFactor,
        NewControlFactorPublicationRow, NewControlFactorStageReport, NewControlFactorValue,
    },
    entities::{
        control_factor_audit_event::Entity as AuditEntity,
        control_factor_materialization_run::Entity as RunEntity,
        control_factor_publication::{
            Column as PublicationColumn, Entity as PublicationEntity, Model as PublicationModel,
        },
        control_factor_publication_factor::{
            Column as PublicationFactorColumn, Entity as PublicationFactorEntity,
        },
        control_factor_stage_report::Entity as StageReportEntity,
        control_factor_value::{Column as FactorColumn, Entity as FactorEntity},
    },
    enums::control_factor::{
        ControlAuditEventType, FactorStatus, PublicationMode, PublicationStatus,
    },
    types::{ControlFactorId, FactorPublicationId},
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter,
    TransactionTrait, sea_query::Expr,
};

pub struct PgControlFactorRepository {
    db: DatabaseConnection,
}

impl PgControlFactorRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

async fn create_materialization_run_q(
    db: &impl ConnectionTrait,
    run: NewControlFactorMaterializationRun,
) -> Result<ControlFactorMaterializationRunInfo, StorageError> {
    RunEntity::insert(run.into_active_model())
        .exec_with_returning(db)
        .await
        .map(Into::into)
        .map_err(StorageError::from)
}

async fn create_stage_report_q(
    db: &impl ConnectionTrait,
    report: NewControlFactorStageReport,
) -> Result<ControlFactorStageReportInfo, StorageError> {
    StageReportEntity::insert(report.into_active_model())
        .exec_with_returning(db)
        .await
        .map(Into::into)
        .map_err(StorageError::from)
}

async fn create_factor_q(
    db: &impl ConnectionTrait,
    factor: NewControlFactorValue,
) -> Result<ControlFactorValueInfo, StorageError> {
    FactorEntity::insert(factor.into_active_model())
        .exec_with_returning(db)
        .await
        .map(Into::into)
        .map_err(StorageError::from)
}

async fn transition_factor_q(
    db: &impl ConnectionTrait,
    factor_id: &ControlFactorId,
    status: FactorStatus,
) -> Result<Option<ControlFactorValueInfo>, StorageError> {
    FactorEntity::update_many()
        .col_expr(FactorColumn::Status, Expr::value(status))
        .filter(FactorColumn::FactorId.eq(factor_id.clone()))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    load_factor_q(db, factor_id).await
}

async fn load_factor_q(
    db: &impl ConnectionTrait,
    factor_id: &ControlFactorId,
) -> Result<Option<ControlFactorValueInfo>, StorageError> {
    FactorEntity::find_by_id(factor_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|opt| opt.map(Into::into))
}

async fn create_publication_q(
    db: &impl ConnectionTrait,
    publication: NewControlFactorPublication,
) -> Result<ControlFactorPublicationInfo, StorageError> {
    if publication.factor_ids.is_empty() {
        return Err(StorageError::Codec(
            "control-factor publication must contain at least one factor".into(),
        ));
    }
    let row = NewControlFactorPublicationRow::from(&publication);
    let model = PublicationEntity::insert(row.into_active_model())
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)?;
    let memberships = publication
        .factor_ids
        .iter()
        .map(|factor_id| {
            NewControlFactorPublicationFactor {
                publication_id: publication.publication_id.clone(),
                factor_id: factor_id.clone(),
            }
            .into_active_model()
        })
        .collect::<Vec<_>>();
    PublicationFactorEntity::insert_many(memberships)
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    enrich_publication_q(db, model).await
}

async fn activate_publication_q(
    db: &impl ConnectionTrait,
    publication_id: &FactorPublicationId,
    actor: &str,
    reason: &str,
) -> Result<ControlFactorPublicationInfo, StorageError> {
    let publication = load_publication_model_q(db, publication_id).await?;
    let factor_ids = load_publication_factor_ids_q(db, publication_id).await?;
    let target_status = factor_status_for_mode(publication.mode);
    for factor_id in &factor_ids {
        validate_factor_for_publication_q(db, factor_id, target_status).await?;
    }
    PublicationEntity::update_many()
        .col_expr(
            PublicationColumn::Status,
            Expr::value(PublicationStatus::Superseded),
        )
        .filter(PublicationColumn::Mode.eq(publication.mode))
        .filter(PublicationColumn::Status.eq(PublicationStatus::Active))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    PublicationEntity::update_many()
        .col_expr(
            PublicationColumn::Status,
            Expr::value(PublicationStatus::Active),
        )
        .filter(PublicationColumn::PublicationId.eq(publication_id.clone()))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    for factor_id in &factor_ids {
        transition_factor_q(db, factor_id, target_status).await?;
    }
    append_audit_event_q(
        db,
        NewControlFactorAuditEvent {
            event_type: ControlAuditEventType::PublicationActivated,
            factor_id: None,
            publication_id: Some(publication_id.clone()),
            actor: actor.to_owned(),
            reason: reason.to_owned(),
            payload: serde_json::json!({ "mode": publication.mode.as_str() }),
        },
    )
    .await?;
    load_publication_info_q(db, publication_id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "control_factor_publication",
            id: publication_id.to_string(),
        })
}

async fn rollback_publication_q(
    db: &impl ConnectionTrait,
    active_publication_id: &FactorPublicationId,
    target_publication_id: &FactorPublicationId,
    actor: &str,
    reason: &str,
) -> Result<ControlFactorPublicationInfo, StorageError> {
    let target = load_publication_model_q(db, target_publication_id).await?;
    let factor_ids = load_publication_factor_ids_q(db, target_publication_id).await?;
    let target_factor_status = factor_status_for_mode(target.mode);
    for factor_id in &factor_ids {
        validate_factor_for_publication_q(db, factor_id, target_factor_status).await?;
    }
    PublicationEntity::update_many()
        .col_expr(
            PublicationColumn::Status,
            Expr::value(PublicationStatus::RolledBack),
        )
        .filter(PublicationColumn::PublicationId.eq(active_publication_id.clone()))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    PublicationEntity::update_many()
        .col_expr(
            PublicationColumn::Status,
            Expr::value(PublicationStatus::Active),
        )
        .filter(PublicationColumn::PublicationId.eq(target_publication_id.clone()))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    for factor_id in &factor_ids {
        transition_factor_q(db, factor_id, target_factor_status).await?;
    }
    append_audit_event_q(
        db,
        NewControlFactorAuditEvent {
            event_type: ControlAuditEventType::PublicationRolledBack,
            factor_id: None,
            publication_id: Some(active_publication_id.clone()),
            actor: actor.to_owned(),
            reason: reason.to_owned(),
            payload: serde_json::json!({ "target_publication_id": target_publication_id }),
        },
    )
    .await?;
    load_publication_info_q(db, target_publication_id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "control_factor_publication",
            id: target_publication_id.to_string(),
        })
}

async fn expire_factors_q(
    db: &impl ConnectionTrait,
    now: DateTime<Utc>,
) -> Result<u64, StorageError> {
    FactorEntity::update_many()
        .col_expr(FactorColumn::Status, Expr::value(FactorStatus::Expired))
        .filter(FactorColumn::ExpiresAt.lte(now))
        .filter(FactorColumn::Status.is_in([
            FactorStatus::Candidate,
            FactorStatus::Shadow,
            FactorStatus::Published,
            FactorStatus::ReportOnly,
        ]))
        .exec(db)
        .await
        .map(|result| result.rows_affected)
        .map_err(StorageError::from)
}

async fn append_audit_event_q(
    db: &impl ConnectionTrait,
    event: NewControlFactorAuditEvent,
) -> Result<ControlFactorAuditEventInfo, StorageError> {
    AuditEntity::insert(event.into_active_model())
        .exec_with_returning(db)
        .await
        .map(Into::into)
        .map_err(StorageError::from)
}

async fn load_active_publication_q(
    db: &impl ConnectionTrait,
    mode: PublicationMode,
) -> Result<Option<ControlFactorPublicationInfo>, StorageError> {
    let model = PublicationEntity::find()
        .filter(PublicationColumn::Mode.eq(mode))
        .filter(PublicationColumn::Status.eq(PublicationStatus::Active))
        .one(db)
        .await
        .map_err(StorageError::from)?;
    match model {
        Some(model) => enrich_publication_q(db, model).await.map(Some),
        None => Ok(None),
    }
}

async fn load_publication_info_q(
    db: &impl ConnectionTrait,
    publication_id: &FactorPublicationId,
) -> Result<Option<ControlFactorPublicationInfo>, StorageError> {
    let model = PublicationEntity::find_by_id(publication_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?;
    match model {
        Some(model) => enrich_publication_q(db, model).await.map(Some),
        None => Ok(None),
    }
}

async fn load_publication_model_q(
    db: &impl ConnectionTrait,
    publication_id: &FactorPublicationId,
) -> Result<PublicationModel, StorageError> {
    PublicationEntity::find_by_id(publication_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "control_factor_publication",
            id: publication_id.to_string(),
        })
}

async fn enrich_publication_q(
    db: &impl ConnectionTrait,
    model: PublicationModel,
) -> Result<ControlFactorPublicationInfo, StorageError> {
    let publication_id = model.publication_id.clone();
    let factor_ids = load_publication_factor_ids_q(db, &publication_id).await?;
    Ok(ControlFactorPublicationRowInfo::from(model).with_factor_ids(factor_ids))
}

async fn load_publication_factor_ids_q(
    db: &impl ConnectionTrait,
    publication_id: &FactorPublicationId,
) -> Result<Vec<ControlFactorId>, StorageError> {
    PublicationFactorEntity::find()
        .filter(PublicationFactorColumn::PublicationId.eq(publication_id.clone()))
        .all(db)
        .await
        .map(|rows| rows.into_iter().map(|row| row.factor_id).collect())
        .map_err(StorageError::from)
}

async fn validate_factor_for_publication_q(
    db: &impl ConnectionTrait,
    factor_id: &ControlFactorId,
    target_status: FactorStatus,
) -> Result<(), StorageError> {
    let info = load_factor_q(db, factor_id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "control_factor_value",
            id: factor_id.to_string(),
        })?;
    let typed = ControlFactorValue::from_info(&info)
        .map_err(|error| StorageError::Codec(error.to_string()))?;
    typed
        .validate_for_transition(target_status, None)
        .map_err(|error| StorageError::Codec(error.to_string()))
}

const fn factor_status_for_mode(mode: PublicationMode) -> FactorStatus {
    match mode {
        PublicationMode::Shadow => FactorStatus::Shadow,
        PublicationMode::Published => FactorStatus::Published,
    }
}

#[async_trait::async_trait]
impl ControlFactorRepository for PgControlFactorRepository {
    async fn create_materialization_run(
        &self,
        run: NewControlFactorMaterializationRun,
    ) -> Result<ControlFactorMaterializationRunInfo, StorageError> {
        create_materialization_run_q(&self.db, run).await
    }

    async fn create_stage_report(
        &self,
        report: NewControlFactorStageReport,
    ) -> Result<ControlFactorStageReportInfo, StorageError> {
        create_stage_report_q(&self.db, report).await
    }

    async fn create_factor(
        &self,
        factor: NewControlFactorValue,
    ) -> Result<ControlFactorValueInfo, StorageError> {
        create_factor_q(&self.db, factor).await
    }

    async fn transition_factor(
        &self,
        factor_id: &ControlFactorId,
        status: FactorStatus,
    ) -> Result<Option<ControlFactorValueInfo>, StorageError> {
        transition_factor_q(&self.db, factor_id, status).await
    }

    async fn create_publication(
        &self,
        publication: NewControlFactorPublication,
    ) -> Result<ControlFactorPublicationInfo, StorageError> {
        create_publication_q(&self.db, publication).await
    }

    async fn activate_publication(
        &self,
        publication_id: &FactorPublicationId,
        actor: &str,
        reason: &str,
    ) -> Result<ControlFactorPublicationInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let publication = activate_publication_q(&txn, publication_id, actor, reason).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(publication)
    }

    async fn rollback_publication(
        &self,
        active_publication_id: &FactorPublicationId,
        target_publication_id: &FactorPublicationId,
        actor: &str,
        reason: &str,
    ) -> Result<ControlFactorPublicationInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let publication = rollback_publication_q(
            &txn,
            active_publication_id,
            target_publication_id,
            actor,
            reason,
        )
        .await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(publication)
    }

    async fn expire_factors(&self, now: DateTime<Utc>) -> Result<u64, StorageError> {
        expire_factors_q(&self.db, now).await
    }

    async fn append_audit_event(
        &self,
        event: NewControlFactorAuditEvent,
    ) -> Result<ControlFactorAuditEventInfo, StorageError> {
        append_audit_event_q(&self.db, event).await
    }

    async fn load_active_publication(
        &self,
        mode: PublicationMode,
    ) -> Result<Option<ControlFactorPublicationInfo>, StorageError> {
        load_active_publication_q(&self.db, mode).await
    }
}
